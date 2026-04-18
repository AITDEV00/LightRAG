"""
Per-workspace logging handler.

Routes log output to separate files per workspace, matching the existing
folder structure and 5-minute rotation interval of the subprocess approach.

Log structure:
    logs/{workspace}/{workspace}_{YYYY-MM-DD_HH-MM}.log
"""
import os
import logging
import threading
from datetime import datetime, timezone, timedelta
from typing import Dict, Tuple, Optional
from contextvars import ContextVar

from app.config.settings import LOG_ROOT

# ContextVar set by the ASGI dispatcher before each request
current_workspace: ContextVar[Optional[str]] = ContextVar(
    "current_workspace", default=None
)

# Rotation interval in minutes (must match the old LOG_ROTATION_INTERVAL)
LOG_ROTATION_INTERVAL = 5


def _get_log_timestamp() -> datetime:
    """Round current time to nearest LOG_ROTATION_INTERVAL-minute interval."""
    # UAE time is UTC+4
    uae_tz = timezone(timedelta(hours=4))
    now = datetime.now(uae_tz)
    minutes = (now.minute // LOG_ROTATION_INTERVAL) * LOG_ROTATION_INTERVAL
    return now.replace(minute=minutes, second=0, microsecond=0)


def _get_log_path(workspace: str) -> str:
    """Generate timestamped log path for a workspace."""
    ts = _get_log_timestamp()
    workspace_log_dir = os.path.join(LOG_ROOT, workspace)
    os.makedirs(workspace_log_dir, exist_ok=True)
    filename = f"{workspace}_{ts.strftime('%Y-%m-%d_%H-%M')}.log"
    return os.path.join(workspace_log_dir, filename)


class UAETimeFormatter(logging.Formatter):
    """Formatter that always uses UAE time (UTC+4) for timestamps."""

    def formatTime(self, record, datefmt=None):
        # UAE time is UTC+4
        uae_tz = timezone(timedelta(hours=4))
        dt = datetime.fromtimestamp(record.created, uae_tz)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime("%Y-%m-%d %H:%M:%S")


class WorkspaceLogHandler(logging.Handler):
    """
    A logging handler that dispatches log records to per-workspace files.

    It reads ``current_workspace`` (a ``ContextVar``) to decide which file
    to write to.  When the workspace is ``None`` (e.g. admin endpoints or
    startup) the record is written to a shared ``_manager.log`` file.

    File handles are kept open and reused.  A background rotation task
    (driven by ``rotate_logs``) swaps them out every 5 minutes, exactly
    like the old ``process_manager.log_rotation_loop``.
    """

    def __init__(self):
        super().__init__()
        # (workspace, log_path) → open file handle
        self._files: Dict[str, Tuple[str, "typing.TextIO"]] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Core logging.Handler interface
    # ------------------------------------------------------------------

    def emit(self, record: logging.LogRecord):
        try:
            workspace = current_workspace.get()
            if workspace is None:
                workspace = "_manager"

            msg = self.format(record)

            with self._lock:
                log_path = _get_log_path(workspace)

                # Open file if not yet tracked, or if the path rotated
                entry = self._files.get(workspace)
                if entry is None or entry[0] != log_path:
                    # Close stale handle
                    if entry is not None:
                        try:
                            entry[1].close()
                        except Exception:
                            pass
                    fh = open(log_path, "a", encoding="utf-8")
                    self._files[workspace] = (log_path, fh)
                else:
                    fh = entry[1]

                fh.write(msg + "\n")
                fh.flush()
        except Exception:
            self.handleError(record)

    # ------------------------------------------------------------------
    # Rotation (called periodically by the ASGI dispatcher)
    # ------------------------------------------------------------------

    def rotate_logs(self):
        """
        Check every tracked workspace and swap the file handle when the
        5-minute bucket changes.  Mirrors ``LightRAGManager.rotate_logs``.
        """
        with self._lock:
            for workspace in list(self._files.keys()):
                new_path = _get_log_path(workspace)
                current_path, current_fh = self._files[workspace]

                if new_path != current_path:
                    try:
                        current_fh.close()
                    except Exception:
                        pass
                    
                    # Instead of eagerly opening a new file (which creates empty files
                    # for idle workspaces), we simply remove the stale handle.
                    # The next time emit() is called for this workspace, it will
                    # lazy-create the correct new log file.
                    self._files.pop(workspace, None)
                    print(f"🔄 [Logs] Closed stale log file for {workspace} (next log will open {new_path})")

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def close_workspace(self, workspace: str):
        """Close and remove the file handle for a single workspace."""
        with self._lock:
            entry = self._files.pop(workspace, None)
            if entry is not None:
                try:
                    entry[1].close()
                except Exception:
                    pass

    def close(self):
        """Close all open file handles (called on shutdown)."""
        with self._lock:
            for _path, fh in self._files.values():
                try:
                    fh.close()
                except Exception:
                    pass
            self._files.clear()
        super().close()


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------
# Singleton handler — attached to the root logger by setup_workspace_logging()
_handler: Optional[WorkspaceLogHandler] = None


def setup_workspace_logging():
    """
    Install the ``WorkspaceLogHandler`` on the root logger so that all
    ``lightrag`` log output is captured into per-workspace files.
    """
    global _handler
    if _handler is not None:
        return  # Already installed

    _handler = WorkspaceLogHandler()
    _handler.setFormatter(
        UAETimeFormatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    )

    root = logging.getLogger()
    root.addHandler(_handler)
    print("📝 [Logging] Per-workspace file handler installed.")


def get_workspace_log_handler() -> Optional[WorkspaceLogHandler]:
    """Return the singleton handler (or ``None`` if not yet installed)."""
    return _handler
