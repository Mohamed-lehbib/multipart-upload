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
        url = await upload_service.generate_presigned_url(session_id, part_number)
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

@app.post("/upload/abort")
async def abort_upload(session_id: str = Body(...)):
    """Abort an ongoing upload"""
    try:
        await upload_service.abort_upload(session_id)
        return {"status": "aborted"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

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
async def resume_upload(session_id: str = Body(...)):
    """Resume a paused upload"""
    try:
        session = await upload_service.resume_upload(session_id)
        return {
            "status": "resumed",
            "session": session
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/upload/pause")
async def pause_upload(session_id: str = Body(...)):
    """Pause an ongoing upload"""
    try:
        await upload_service.pause_upload(session_id)
        return {"status": "paused"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
# from http.client import HTTPException
# from typing import List
# from fastapi import FastAPI, UploadFile, File, Form, Body
# import boto3
# from uuid import uuid4
# from fastapi.middleware.cors import CORSMiddleware
# import os
# from dotenv import load_dotenv

# app = FastAPI()

# # CORS
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# load_dotenv()

# # AWS S3 Configuration
# AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
# AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
# BUCKET_NAME = os.getenv("BUCKET_NAME")
# REGION = os.getenv("AWS_REGION")


# s3_client = boto3.client("s3", region_name="eu-west-3",
#                          aws_access_key_id=AWS_ACCESS_KEY,
#                          aws_secret_access_key=AWS_SECRET_KEY,
#     config=boto3.session.Config(signature_version='s3v4'))

# @app.post("/upload/initiate")
# async def initiate_upload(filename: str = Form(...), content_type: str = Form(...)):
#     key = f"uploads/{uuid4()}_{filename}"
#     response = s3_client.create_multipart_upload(Bucket=BUCKET_NAME, Key=key,
#         ContentType=content_type )
#     return {"uploadId": response["UploadId"], "key": key}

# @app.post("/upload/presigned-url")
# async def get_presigned_url(key: str = Form(...), uploadId: str = Form(...), partNumber: int = Form(...)):
#     url = s3_client.generate_presigned_url(
#         "upload_part",
#         Params={
#             "Bucket": BUCKET_NAME,
#             "Key": key,
#             "UploadId": uploadId,
#             "PartNumber": partNumber
#         },
#         ExpiresIn=3600,
#         HttpMethod="PUT"
#     )
#     return {"url": url}

# @app.post("/upload/complete")
# async def complete_upload(
#     key: str = Body(...),
#     uploadId: str = Body(...),
#     parts: List[dict] = Body(...)
# ):
#     try:
#         # Ensure parts are sorted by PartNumber
#         sorted_parts = sorted(parts, key=lambda x: x['PartNumber'])
        
#         response = s3_client.complete_multipart_upload(
#             Bucket=BUCKET_NAME,
#             Key=key,
#             UploadId=uploadId,
#             MultipartUpload={"Parts": sorted_parts}
#         )
#         return {"location": response["Location"]}
#     except Exception as e:
#         raise HTTPException(status_code=400, detail=str(e))