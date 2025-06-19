# services/upload_service.py
import boto3
import redis
import json
from datetime import datetime, timedelta
from uuid import uuid4
from typing import Optional, List, Dict
import os
from models.upload_models import *

class UploadService:
    def __init__(self):
        # AWS S3 Configuration
        self.s3_client = boto3.client(
            "s3",
            region_name="us-east-1",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
            config=boto3.session.Config(signature_version='s3v4')
        )
        
        self.bucket_name = os.getenv("BUCKET_NAME")
        
        # Redis Configuration
        self.redis_client = redis.Redis(
            host="redis",
            port=6379,
            password=os.getenv("REDIS_PASSWORD", ""), 
            decode_responses=False,  # Keep as False for binary data safety
            socket_connect_timeout=5,  # Add timeout
            health_check_interval=30,  # Enable health checks
                    db=0
        )
        
        # Session expiration (7 days)
        self.session_ttl = timedelta(days=7)

    async def create_session(self, session_data: UploadSessionCreate) -> UploadSession:
        """Create a new upload session"""
        session_id = str(uuid4())
        s3_key = f"uploads/{session_id}_{session_data.filename}"
        
        # Initialize multipart upload on S3
        response = self.s3_client.create_multipart_upload(
            Bucket=self.bucket_name,
            Key=s3_key,
            ContentType=session_data.content_type,
            Metadata={
                'session-id': session_id,
                'original-filename': session_data.filename,
                'file-size': str(session_data.file_size)
            }
        )
        
        # Create session object
        session = UploadSession(
            id=session_id,
            filename=session_data.filename,
            s3_key=s3_key,
            upload_id=response["UploadId"],
            file_size=session_data.file_size,
            chunk_size=session_data.chunk_size,
            content_type=session_data.content_type,
            status=UploadStatus.PENDING,
            created_at=datetime.now(),
            expires_at=datetime.now() + self.session_ttl
        )
        
        # Store session in Redis
        await self._store_session(session)
        
        return session

    def generate_presigned_url(self, session_id: str, part_number: int) -> str:
        print(f"\n=== Generating S3 Presigned URL ===")
        print(f"Bucket: {self.bucket_name}")
        print(f"Key: {session.s3_key}")
        print(f"UploadId: {session.upload_id}")
        print(f"PartNumber: {part_number}")
        
        try:
            url = self.s3_client.generate_presigned_url(
                "upload_part",
                Params={
                    "Bucket": self.bucket_name,
                    "Key": session.s3_key,
                    "UploadId": session.upload_id,
                    "PartNumber": part_number
                },
                ExpiresIn=3600,
                HttpMethod="PUT"
            )
            print(f"Generated URL: {url}")
            return url
        except Exception as e:
            print(f"S3 Presigned URL Error: {str(e)}")
            raise

    async def mark_part_complete(self, session_id: str, part: PartUpload):
        """Mark a part as successfully uploaded"""
        session = await self.get_session(session_id)
        if not session:
            raise ValueError("Session not found")
        
        # Add part to uploaded parts
        part_data = {
            "PartNumber": part.part_number,
            "ETag": part.etag,
            "Size": part.size,
            "UploadedAt": datetime.now().isoformat()
        }
        
        if part.checksum:
            part_data["Checksum"] = part.checksum
        
        session.uploaded_parts.append(part_data)
        await self._store_session(session)

    async def complete_upload(self, session_id: str, parts: List[dict]) -> dict:
        """Complete the multipart upload"""
        session = await self.get_session(session_id)
        if not session:
            raise ValueError("Session not found")
        
        # Sort parts by part number
        sorted_parts = sorted(parts, key=lambda x: x['PartNumber'])
        
        # Complete multipart upload on S3
        response = self.s3_client.complete_multipart_upload(
            Bucket=self.bucket_name,
            Key=session.s3_key,
            UploadId=session.upload_id,
            MultipartUpload={"Parts": sorted_parts}
        )
        
        # Update session
        session.status = UploadStatus.COMPLETED
        session.completed_at = datetime.now()
        await self._store_session(session)
        
        return {
            "location": response.get("Location"),
            "etag": response.get("ETag")
        }

    async def abort_upload(self, session_id: str):
        """Abort an upload session"""
        session = await self.get_session(session_id)
        if not session:
            raise ValueError("Session not found")
        
        # Abort multipart upload on S3
        self.s3_client.abort_multipart_upload(
            Bucket=self.bucket_name,
            Key=session.s3_key,
            UploadId=session.upload_id
        )
        
        # Update session
        session.status = UploadStatus.CANCELLED
        await self._store_session(session)

    async def pause_upload(self, session_id: str):
        """Pause an upload session"""
        session = await self.get_session(session_id)
        if not session:
            raise ValueError("Session not found")
        
        session.status = UploadStatus.PAUSED
        await self._store_session(session)

    async def resume_upload(self, session_id: str) -> UploadSession:
        """Resume a paused upload session"""
        session = await self.get_session(session_id)
        if not session:
            raise ValueError("Session not found")
        
        if session.status != UploadStatus.PAUSED:
            raise ValueError("Session is not paused")
        
        session.status = UploadStatus.UPLOADING
        await self._store_session(session)
        
        return session

    async def get_session(self, session_id: str) -> Optional[UploadSession]:
        """Get session by ID"""
        session_data = self.redis_client.get(f"upload_session:{session_id}")
        if not session_data:
            return None
        
        data = json.loads(session_data)
        return UploadSession(**data)

    async def get_active_sessions(self) -> List[UploadSession]:
        """Get all active upload sessions"""
        pattern = "upload_session:*"
        keys = self.redis_client.keys(pattern)
        
        sessions = []
        for key in keys:
            session_data = self.redis_client.get(key)
            if session_data:
                data = json.loads(session_data)
                session = UploadSession(**data)
                if session.status not in [UploadStatus.COMPLETED, UploadStatus.CANCELLED]:
                    sessions.append(session)
        
        return sessions

    async def _store_session(self, session: UploadSession):
        """Store session in Redis"""
        try: 
            session_data = session.model_dump_json() 
            # Convert datetime objects to ISO format strings
            for key, value in session_data.items():
                if isinstance(value, datetime):
                    session_data[key] = value.isoformat()
            
            self.redis_client.setex(
                f"upload_session:{session.id}",
                int(self.session_ttl.total_seconds()),
                session_data
                 )
        except Exception as e:
            print(f"Failed to store session: {str(e)}")
            raise    