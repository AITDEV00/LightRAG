"""
This module contains all Entity Resolution (Deduplication) related routes.
"""

from typing import Optional, Dict, Any
import traceback
from fastapi import APIRouter, Depends, Query, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
import asyncio
import json
import os
from datetime import datetime
import shutil
import zipfile
from fastapi.responses import FileResponse

from lightrag_auto_er.pipeline import run_pipeline
from lightrag_auto_er.config import settings as er_settings
from lightrag.utils import logger
from ..utils_api import get_combined_auth_dependency
from lightrag.kg.shared_storage import get_namespace_data, get_pipeline_status_lock
from lightrag.base import BaseKVStorage
from lightrag.kg.redis_impl import RedisKVStorage


# --- Job Management ---

class ERJobManager:
    """
    Manages Entity Resolution (ER) jobs using LightRAG's storage backend (Redis/JSON).
    Handles job state persistence, checkpointing for crash recovery, and status updates.
    """
    def __init__(self, rag):
        # Dynamically create storage using the same factory as doc_status.
        # This automatically uses RedisKVStorage if configured, or JsonKVStorage otherwise.
        # Namespace "er_jobs" isolates these records from documents.
        
        # Try to get working_dir from rag object directly
        working_dir = getattr(rag, "working_dir", None)
        
        if not working_dir:
             # Fallback to global config or config settings
             working_dir = rag.doc_status.global_config.get("working_dir")
        
        if not working_dir:
             from lightrag_auto_er.config import settings
             working_dir = settings.run_dir if hasattr(settings, "run_dir") else "./"

        # Initialize storage (KV Store)
        self.storage: BaseKVStorage = rag.key_string_value_json_storage_cls(
            namespace="er_jobs",
            workspace=rag.doc_status.global_config.get("workspace", ""),
            global_config=rag.doc_status.global_config, 
            embedding_func=None
        )
        
        self.run_dir = os.path.join(working_dir, "er_jobs")
        os.makedirs(self.run_dir, exist_ok=True)
        self.active_job_key = "ACTIVE_JOB_ID"

    async def initialize(self):
        """Ensure storage is initialized (connected to Redis/loaded from disk)."""
        if hasattr(self.storage, "initialize"):
            await self.storage.initialize()

    async def create_job(self, job_id: str = None) -> str:
        """Creates a new job with a unique ID and 'queued' status."""
        # Cleanup old jobs before creating a new one
        await self._cleanup_old_jobs()

        if not job_id:
            job_id = datetime.now().strftime("er_%Y-%m-%d_%H-%M-%S")
            
        job_data = {
            "job_id": job_id,
            "status": "queued",
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "progress": {"total": 0, "processed": 0},
            "history": [], # List of log messages
            "checkpoints": [], # For resuming execution
            "result_summary": {}
        }
        await self.storage.upsert({job_id: job_data})
        return job_id

    async def _cleanup_old_jobs(self):
        """Removes old job directories based on retention policy."""
        try:
            from ..config import global_args
            history_len = getattr(global_args, "er_result_history_len", 3)
            
            if not os.path.exists(self.run_dir):
                return

            subdirs = []
            for d in os.listdir(self.run_dir):
                path = os.path.join(self.run_dir, d)
                if os.path.isdir(path) and d.startswith("er_"):
                    subdirs.append(path)
            
            # Sort by name (timestamp is in name)
            subdirs.sort()
            
            if len(subdirs) >= history_len:
                to_delete = subdirs[:len(subdirs) - history_len + 1] 
                to_delete_count = len(subdirs) - history_len + 1
                if to_delete_count > 0:
                     for d in subdirs[:to_delete_count]:
                        logger.info(f"Cleaning up old ER job: {d}")
                        shutil.rmtree(d, ignore_errors=True)

        except Exception as e:
            logger.error(f"Failed to cleanup old ER jobs: {e}")

    async def get_job(self, job_id: str) -> Dict[str, Any]:
        """Retrieves job data."""
        return await self.storage.get_by_id(job_id)

    async def update_status(self, job_id: str, status: str, message: str = None, progress: Dict = None):
        """Updates job status, appending logs and updating timestamps."""
        job = await self.get_job(job_id)
        if not job:
            return 
        
        job["status"] = status
        job["updated_at"] = datetime.now().isoformat()
        
        if message:
            job["history"].append(f"{datetime.now().isoformat()}: {message}")
        
        if progress:
            job["progress"].update(progress)
            
        await self.storage.upsert({job_id: job})

    async def save_checkpoint(self, job_id: str, completed_item_id: str):
        """
        Saves a completed item ID to the job's checkpoint list.
        Used during execution to resume after crashes.
        """
        job = await self.get_job(job_id)
        if not job:
            return
            
        if "checkpoints" not in job:
            job["checkpoints"] = []
            
        job["checkpoints"].append(completed_item_id)
        await self.storage.upsert({job_id: job})

    async def get_checkpoints(self, job_id: str) -> set:
        """Returns a set of already processed item IDs."""
        job = await self.get_job(job_id)
        if not job or "checkpoints" not in job:
            return set()
        return set(job["checkpoints"])

    async def set_result_path(self, job_id: str, result_path: str, zip_path: str):
        """Links the Analysis result files to the job."""
        job = await self.get_job(job_id)
        if job:
            job["result_path"] = result_path
            job["zip_path"] = zip_path
            await self.storage.upsert({job_id: job})
            
    async def get_active_job_id(self) -> Optional[str]:
        """Returns the ID of the currently running job, if any."""
        data = await self.storage.get_by_id(self.active_job_key)
        # Check if stored value is a dict (standard storage) or string (raw redis legacy?)
        # BaseKVStorage usually stores JSON.
        if data and isinstance(data, dict):
             return data.get("job_id")
        return None

    async def try_acquire_lock(self, job_id: str) -> bool:
        """
        Attempts to acquire the global lock for a new job.
        Returns True if successful, False if another job is active.
        Uses Redis SETNX if available for atomic locking.
        """
        # If using RedisKVStorage, try to use atomic operations
        if isinstance(self.storage, RedisKVStorage) and self.storage._redis:
            key = f"{self.storage.final_namespace}:{self.active_job_key}"
            value = json.dumps({"job_id": job_id})
            # redis.set(key, value, nx=True) returns True if set, None/False if not
            try:
                success = await self.storage._redis.set(key, value, nx=True)
                return bool(success)
            except Exception as e:
                logger.error(f"Redis atomic lock failed, falling back: {e}")
        
        # Fallback for non-Redis or if access fails: Check then Set (Race condition possible)
        current = await self.get_active_job_id()
        if current:
            return False
            
        await self.storage.upsert({self.active_job_key: {"job_id": job_id}})
        return True

    async def force_release_lock(self):
        """Forces release of the lock by deleting the key."""
        await self.storage.delete([self.active_job_key])

    async def clear_active_job_id(self):
        """Clears the active job marker."""
        await self.force_release_lock()


# ----------------------

router = APIRouter(tags=["Deduplication"])

async def _run_analysis_task(job_id: str, rag, job_manager: ERJobManager):
    pipeline_status_lock = get_pipeline_status_lock()
    pipeline_status = await get_namespace_data("pipeline_status")
    
    try:
        # 1. Update UI (without locking system)
        async with pipeline_status_lock:
            pipeline_status["job_name"] = "ER: Analyzing Entities"
            pipeline_status["job_start"] = datetime.now().isoformat()
            pipeline_status["latest_message"] = "Initializing ER Analysis..."

        await job_manager.update_status(job_id, "processing", "Started analysis")

        # 2. Run Pipeline (Fetch & Analyze)
        # Inline fetching for simplicity and robustness
        all_entities = []
        limit = 1000
        offset = 0
        while True:
            entities = await rag.get_entities_jyao(limit=limit, offset=offset)
            if not entities:
                break
            all_entities.extend(entities)
            if len(entities) < limit:
                break
            offset += limit
            # Update UI periodically during fetch
            async with pipeline_status_lock:
                 pipeline_status["latest_message"] = f"Fetched {len(all_entities)} entities..."

        async with pipeline_status_lock:
            pipeline_status["docs"] = len(all_entities)
            pipeline_status["latest_message"] = "Running Entity Resolution Pipeline..."

        # Run ER Pipeline
        # Ensure return_merge_structure is set
        er_settings.RETURN_MERGE_STRUCTURE = True
        
        # Create a job-specific directory within the workspace-aware run_dir
        job_dir = os.path.join(job_manager.run_dir, job_id)
        os.makedirs(job_dir, exist_ok=True)
        
        from pathlib import Path
        job_dir_path = Path(job_dir)
        
        # NOTE: pipeline steps are now async and non-blocking due to internal fixes
        resolution_results = await run_pipeline(all_entities, output_dir=job_dir_path)
        
        # save results to job dir
        import pandas as pd
        if resolution_results is not None:
            # Assuming resolution_results is the merge_plan (list of dicts)
            # We save it as JSON for the execute step (if needed later)
            with open(os.path.join(job_dir, "merge_plan.json"), "w") as f:
                json.dump(resolution_results, f, indent=2)
            
            # Create CSV for user review
            df = pd.DataFrame(resolution_results)
            df.to_csv(os.path.join(job_dir, "merge_plan.csv"), index=False)
        
        # Create ZIP
        zip_filename = f"report_{job_id}.zip"
        zip_path = os.path.join(job_manager.run_dir, zip_filename)
        with zipfile.ZipFile(zip_path, 'w') as zf:
            # Add the specific files we generated
            if os.path.exists(os.path.join(job_dir, "merge_plan.csv")):
                zf.write(os.path.join(job_dir, "merge_plan.csv"), "merge_plan.csv")
            if os.path.exists(os.path.join(job_dir, "merge_plan.json")):
                zf.write(os.path.join(job_dir, "merge_plan.json"), "merge_plan.json")
        
        await job_manager.set_result_path(job_id, job_dir, zip_path)
        await job_manager.update_status(job_id, "completed", "Analysis completed")
        
        async with pipeline_status_lock:
            pipeline_status["latest_message"] = "ER Analysis Completed."
            pipeline_status["history_messages"].append("ER Analysis Completed.")

    except Exception as e:
        logger.error(f"ER Analysis Failed: {e}")
        logger.error(traceback.format_exc())
        await job_manager.update_status(job_id, "failed", str(e))
        async with pipeline_status_lock:
            pipeline_status["latest_message"] = f"ER Analysis Failed: {str(e)}"
            pipeline_status["history_messages"].append(f"ER Analysis Failed: {str(e)}")

    finally:
         # Release lock
        await job_manager.clear_active_job_id()
        
        async with pipeline_status_lock:
            pipeline_status["job_name"] = "Default Job"
            pipeline_status["job_start"] = None
            pipeline_status["latest_message"] = "ER Analysis Finished"


def create_er_routes(rag, api_key: Optional[str] = None):
    combined_auth = get_combined_auth_dependency(api_key)

    @router.on_event("startup")
    async def cleanup_stale_er_jobs():
        """On server startup, clear any stale active job locks."""
        try:
            job_manager = ERJobManager(rag)
            await job_manager.initialize()
            
            active_job_id = await job_manager.get_active_job_id()
            if active_job_id:
                logger.warning(f"Found stale active ER job lock {active_job_id} on startup. Clearing.")
                job = await job_manager.get_job(active_job_id)
                if job and job["status"] == "processing":
                     await job_manager.update_status(active_job_id, "failed", "Server restarted during processing")
                
                await job_manager.clear_active_job_id()

        except Exception as e:
            logger.error(f"Failed to cleanup stale ER jobs on startup: {e}")

    @router.post("/analyze", dependencies=[Depends(combined_auth)])
    async def analyze_deduplicate(
        background_tasks: BackgroundTasks,
        force_restart: bool = Query(False, description="Force start a new job even if one is marked active"),
    ):
        """
        Start an asynchronous Entity Resolution analysis job.
        Returns a job_id to poll status.
        Only one analysis job can run at a time.
        """
        # Initialize Job Manager
        job_manager = ERJobManager(rag)
        await job_manager.initialize()

        if force_restart:
             await job_manager.force_release_lock()

        job_id = datetime.now().strftime("er_%Y-%m-%d_%H-%M-%S")
        
        # Try to acquire lock atomically
        if not await job_manager.try_acquire_lock(job_id):
            active_id = await job_manager.get_active_job_id()
            if active_id:
                 job = await job_manager.get_job(active_id)
                 return {
                     "status": "success", 
                     "job_id": active_id, 
                     "message": "System busy with another ER job", 
                     "job_details": job
                 }
            else:
                # Key exists (transactionally), but get_active_job_id returned None.
                # This implies the key exists but contains {"job_id": None} or invalid data.
                # We should clear it and retry.
                logger.warning(f"Found zombie active job lock (key exists but no ID). Clearing and retrying.")
                await job_manager.clear_active_job_id()
                
                # Retry lock acquisition
                if not await job_manager.try_acquire_lock(job_id):
                     # If it fails again immediately, legitimate race or issue
                     active_id = await job_manager.get_active_job_id()
                     if active_id:
                         # Someone else grabbed it in the split second
                         job = await job_manager.get_job(active_id)
                         return {
                             "status": "success", 
                             "job_id": active_id, 
                             "message": "System busy with another ER job", 
                             "job_details": job
                         }
                     raise HTTPException(status_code=503, detail="Could not acquire job lock after cleanup. System busy.")

        # Create the job record with the ID we secured
        await job_manager.create_job(job_id=job_id)

        # Start Background Task
        background_tasks.add_task(_run_analysis_task, job_id, rag, job_manager)

        return {"status": "success", "job_id": job_id, "message": "Analysis started"}

    @router.get("/status/{job_id}", dependencies=[Depends(combined_auth)])
    async def get_deduplicate_status(job_id: str):
        """Get the status of an ER job."""
        job_manager = ERJobManager(rag)
        await job_manager.initialize()
        
        job = await job_manager.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
            
        return job

    @router.get("/report/{job_id}", dependencies=[Depends(combined_auth)])
    async def get_deduplicate_report(job_id: str):
        """Download the ZIP report for a completed analysis job."""
        job_manager = ERJobManager(rag)
        await job_manager.initialize()
        
        job = await job_manager.get_job(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        if job["status"] != "completed":
             # User said: "should call the status also if complete then get the file"
             # If not complete, just return status-like response? 
             # But this is GET /report, implied for file download. 
             # If I return JSON here, browser might download json.
             # Better to 400 with detail being the status.
             return {
                 "status": "pending", 
                 "job_status": job["status"], 
                 "message": "Analysis not complete yet. check status endpoint."
             }
             
        zip_path = job.get("zip_path")
        if not zip_path or not os.path.exists(zip_path):
            raise HTTPException(status_code=404, detail="Report file not found")
            
        return FileResponse(zip_path, filename=f"er_report_{job_id}.zip")

    return router
