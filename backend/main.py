from fastapi import FastAPI, HTTPException, Form, Body, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
import boto3
import redis
import json
from datetime import datetime, timedelta
from uuid import uuid4
import os
from dotenv import load_dotenv
import asyncio
from contextlib import asynccontextmanager

from models.upload_models import *
from services.upload_service import UploadService
from services.cleanup_service import CleanupService

load_dotenv()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    cleanup_service = CleanupService()
    cleanup_task = asyncio.create_task(cleanup_service.start_cleanup_scheduler())
    
    yield
    
    # Shutdown
    cleanup_task.cancel()
    try:
        await cleanup_task
    except asyncio.CancelledError:
        pass

app = FastAPI(title="Large File Upload Service", lifespan=lifespan)

# CORS Configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
class ResumeRequest(BaseModel):
    session_id: str

class PauseRequest(BaseModel):
    session_id: str

class CancelRequest(BaseModel):
    session_id: str
# Initialize services
upload_service = UploadService()

@app.post("/upload/initiate")
async def initiate_upload(
    filename: str = Form(...),
    file_size: int = Form(...),
    content_type: str = Form(...),
    chunk_size: int = Form(10 * 1024 * 1024)
):
    """Initialize a new multipart upload session"""
    try:
        file_size = int(file_size)
        chunk_size = int(chunk_size)
        session_data = UploadSessionCreate(
            filename=filename,
            file_size=file_size,
            content_type=content_type,
            chunk_size=chunk_size
        )
        
        session = await upload_service.create_session(session_data)
        return {
            "session_id": session.id,
            "uploadId": session.upload_id,
            "key": session.s3_key,
            "chunk_size": session.chunk_size,
            "expires_at": session.expires_at.isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/upload/presigned-url")
async def get_presigned_url(
    session_id: str = Form(...),
    part_number: int = Form(...)
):
    """Generate presigned URL for uploading a specific part"""
    try:
        print(f"\n=== Presigned URL Request ===")
        print(f"Session ID: {session_id}")
        print(f"Part Number: {part_number}")
        
        session_data = await upload_service.get_session(session_id)
        print(f"Session from Redis: {session_data}")
        
        if not session_data:
            raise HTTPException(status_code=404, detail="Session not found in Redis")
        
        # Pass the session object instead of session_id
        url = upload_service.generate_presigned_url(session_data, part_number)
        return {"url": url}
    except Exception as e:
        print(f"!!! Presigned URL Error: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/upload/part-complete")
async def mark_part_complete(
    session_id: str = Form(...),
    part_number: int = Form(...),
    etag: str = Form(...),
    size: int = Form(...),
    checksum: Optional[str] = Form(None)
):
    """Mark a part as successfully uploaded"""
    try:
        part = PartUpload(
            part_number=part_number,
            etag=etag,
            size=size,
            checksum=checksum
        )
        
        await upload_service.mark_part_complete(session_id, part)
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/upload/complete")
async def complete_upload(
    session_id: str = Body(...),
    parts: List[dict] = Body(...)
):
    """Complete the multipart upload"""
    try:
        result = await upload_service.complete_upload(session_id, parts)
        return {
            "status": "completed",
            "location": result.get("location"),
            "etag": result.get("etag")
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# @app.post("/upload/abort")
# async def abort_upload(session_id: str = Body(...,embed=True)):
#     """Abort an ongoing upload"""
#     try:
#         await upload_service.abort_upload(session_id)
#         return {"status": "aborted"}
#     except Exception as e:
#         raise HTTPException(status_code=400, detail=str(e))
@app.post("/upload/abort")
async def abort_upload(request: CancelRequest):
    """Cancel/abort an upload"""
    try:
        session_id = request.session_id
        print(f"Attempting to abort session: {session_id}")
        
        session = await upload_service.get_session(session_id)
        if not session:
            # If session doesn't exist, consider it already cancelled
            return {"status": "cancelled", "message": "Session not found"}
        
        # Cancel the multipart upload on S3 if it exists
        if hasattr(session, 'upload_id') and session.upload_id:
            try:
                # Add S3 abort logic here if needed
                print(f"Aborting S3 multipart upload: {session.upload_id}")
            except Exception as s3_error:
                print(f"Error aborting S3 upload: {s3_error}")
                # Continue even if S3 abort fails
        
        # Update session status
        session.status = UploadStatus.CANCELLED
        await upload_service._store_session(session)
        
        return {
            "status": "cancelled",
            "session": session.dict()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error aborting upload: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/upload/session/{session_id}")
async def get_session(session_id: str):
    """Get upload session details"""
    try:
        session = await upload_service.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        return session
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/upload/sessions/active")
async def get_active_sessions():
    """Get all active upload sessions"""
    try:
        sessions = await upload_service.get_active_sessions()
        return {"sessions": sessions}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/upload/resume")
async def resume_upload(request: ResumeRequest):
    """Resume a paused upload with enhanced error handling"""
    try:
        session_id = request.session_id
        print(f"Attempting to resume session: {session_id}")
        
        # Get session with detailed error info
        session = await upload_service.get_session(session_id)
        if not session:
            print(f"Session not found in Redis: {session_id}")
            raise HTTPException(
                status_code=404, 
                detail=f"Session {session_id} not found or expired"
            )
        
        print(f"Session found with status: {session.status}")
        
        # More flexible resume conditions
        if session.status not in [UploadStatus.PAUSED, UploadStatus.FAILED]:
            if session.status == UploadStatus.COMPLETED:
                return {
                    "status": "already_completed",
                    "session": session.dict()
                }
            elif session.status == UploadStatus.UPLOADING:
                return {
                    "status": "already_uploading", 
                    "session": session.dict()
                }
            else:
                raise HTTPException(
                    status_code=400,
                    detail=f"Cannot resume session with status: {session.status}"
                )
        
        # Resume the upload
        session = await upload_service.resume_upload(session_id)
        return {
            "status": "resumed",
            "session": session.dict()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Unexpected error resuming upload: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")



# @app.post("/upload/pause")
# async def pause_upload(session_id: str = Body(...,embed=True)):
#     """Pause an ongoing upload"""
#     try:
#         await upload_service.pause_upload(session_id)
#         return {"status": "paused"}
#     except Exception as e:
#         raise HTTPException(status_code=400, detail=str(e))

@app.post("/upload/pause")
async def pause_upload(request: PauseRequest):
    """Pause an active upload"""
    try:
        session_id = request.session_id
        print(f"Attempting to pause session: {session_id}")
        
        session = await upload_service.get_session(session_id)
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        
        if session.status != UploadStatus.UPLOADING:
            raise HTTPException(
                status_code=400, 
                detail=f"Cannot pause session with status: {session.status}"
            )
        
        # Update session status
        session.status = UploadStatus.PAUSED
        await upload_service._store_session(session)
        
        return {
            "status": "paused",
            "session": session.dict()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error pausing upload: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))