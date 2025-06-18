# models/upload_models.py
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
from enum import Enum

class UploadStatus(str, Enum):
    PENDING = "pending"
    UPLOADING = "uploading"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class UploadSessionCreate(BaseModel):
    filename: str
    file_size: int
    content_type: str
    chunk_size: int = 10 * 1024 * 1024  # 10MB default

class UploadSession(BaseModel):
    id: str
    filename: str
    s3_key: str
    upload_id: str
    file_size: int
    chunk_size: int
    content_type: str
    status: UploadStatus
    uploaded_parts: List[dict] = []
    created_at: datetime
    completed_at: Optional[datetime] = None
    expires_at: datetime
    error_message: Optional[str] = None
    retry_count: int = 0

class PartUpload(BaseModel):
    part_number: int
    etag: str
    size: int
    checksum: Optional[str] = None

class CompleteUploadRequest(BaseModel):
    session_id: str
    parts: List[dict]