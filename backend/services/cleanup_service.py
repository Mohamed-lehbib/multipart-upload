# services/cleanup_service.py
import asyncio
import boto3
import redis
import json
import os
from datetime import datetime, timedelta
from typing import List
import logging

logger = logging.getLogger(__name__)

class CleanupService:
    def __init__(self):
        self.s3_client = boto3.client(
            "s3",
            region_name=os.getenv("AWS_REGION", "us-east-1"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("AWS_SECRET_KEY")
        )
        
        self.bucket_name = os.getenv("BUCKET_NAME")
        
        self.redis_client = redis.Redis(
            host="redis",
            port=6379,
            password="REDIS_PASSWORD",
            decode_responses=True,
            db=0
        )
        
        

    async def start_cleanup_scheduler(self):
        """Start the cleanup scheduler"""
        while True:
            try:
                await self.cleanup_expired_sessions()
                await self.cleanup_incomplete_uploads()
                
                # Run cleanup every 6 hours
                await asyncio.sleep(6 * 60 * 60)
                
            except asyncio.CancelledError:
                logger.info("Cleanup scheduler cancelled")
                break
            except Exception as e:
                logger.error(f"Error in cleanup scheduler: {e}")
                await asyncio.sleep(60)  # Wait 1 minute before retrying

    async def cleanup_expired_sessions(self):
        """Clean up expired upload sessions"""
        logger.info("Starting expired sessions cleanup")
        
        pattern = "upload_session:*"
        keys = self.redis_client.keys(pattern)
        
        expired_count = 0
        for key in keys:
            try:
                session_data = self.redis_client.get(key)
                if not session_data:
                    continue
                
                data = json.loads(session_data)
                expires_at = datetime.fromisoformat(data.get("expires_at", ""))
                
                if datetime.now() > expires_at:
                    # Abort the multipart upload
                    try:
                        self.s3_client.abort_multipart_upload(
                            Bucket=self.bucket_name,
                            Key=data["s3_key"],
                            UploadId=data["upload_id"]
                        )
                    except Exception as e:
                        logger.warning(f"Failed to abort S3 upload {data['upload_id']}: {e}")
                    
                    # Remove session from Redis
                    self.redis_client.delete(key)
                    expired_count += 1
                    
            except Exception as e:
                logger.error(f"Error processing session {key}: {e}")
        
        logger.info(f"Cleaned up {expired_count} expired sessions")

    async def cleanup_incomplete_uploads(self):
        """Clean up incomplete multipart uploads directly from S3"""
        logger.info("Starting S3 incomplete uploads cleanup")
        
        try:
            # List incomplete multipart uploads older than 7 days
            cutoff_date = datetime.now() - timedelta(days=7)
            
            response = self.s3_client.list_multipart_uploads(
                Bucket=self.bucket_name,
                Prefix="uploads/"
            )
            
            uploads = response.get("Uploads", [])
            cleanup_count = 0
            
            for upload in uploads:
                if upload["Initiated"] < cutoff_date:
                    try:
                        self.s3_client.abort_multipart_upload(
                            Bucket=self.bucket_name,
                            Key=upload["Key"],
                            UploadId=upload["UploadId"]
                        )
                        cleanup_count += 1
                        logger.info(f"Aborted stale upload: {upload['Key']}")
                        
                    except Exception as e:
                        logger.error(f"Failed to abort upload {upload['UploadId']}: {e}")
            
            logger.info(f"Cleaned up {cleanup_count} incomplete S3 uploads")
            
        except Exception as e:
            logger.error(f"Error during S3 cleanup: {e}")