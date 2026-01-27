"""
LightRAG Enterprise Manager - Backward Compatible Entry Point

This is a thin wrapper that imports the VSA-structured application.
For the actual implementation, see app/main.py
"""
from app.main import app
from app.config.settings import args

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=args.host, port=args.port)