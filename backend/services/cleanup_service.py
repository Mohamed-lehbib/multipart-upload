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
            region_name= "eu-west-3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("AWS_SECRET_KEY")
        )
        
        self.bucket_name = os.getenv("BUCKET_NAME")
        
        self.redis_client = redis.Redis(
            host="redis",
            port=6379,
            password=os.getenv("REDIS_PASSWORD", ""), 
            decode_responses=False,  # Keep as False for binary data safety
            socket_connect_timeout=5,  # Add timeout
            health_check_interval=30 , # Enable health checks
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
        """Clean up expired or orphaned sessions"""
        try:
            pattern = "upload_session:*"
            
            # Check Redis connection first
            try:
                self.redis_client.ping()  # Remove await - ping() is synchronous
            except Exception as redis_error:
                print(f"Redis unavailable for cleanup: {redis_error}")
                return
            
            keys = self.redis_client.keys(pattern)  # Remove await - keys() is synchronous
            print(f"Found {len(keys)} sessions to check for cleanup")
            
            cleaned_count = 0
            for key in keys:
                try:
                    session_data = self.redis_client.get(key)  # Remove await
                    if not session_data:
                        continue
                        
                    data = json.loads(session_data)
                    
                    # Parse created_at timestamp
                    created_at_str = data.get('created_at')
                    if created_at_str:
                        if isinstance(created_at_str, str):
                            created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                        else:
                            created_at = datetime.fromtimestamp(created_at_str)
                        
                        # Convert to UTC for comparison
                        if created_at.tzinfo is None:
                            created_at = created_at.replace(tzinfo=None)
                        else:
                            created_at = created_at.replace(tzinfo=None)
                        
                        # Only clean up sessions that are:
                        # 1. Older than 7 days (increased from 24 hours)
                        # 2. AND completed, cancelled, or failed
                        age_hours = (datetime.utcnow() - created_at).total_seconds() / 3600
                        session_status = data.get('status', '').lower()
                        
                        should_cleanup = False
                        
                        if age_hours > 7 * 24:  # 7 days
                            # Always cleanup very old sessions regardless of status
                            should_cleanup = True
                            print(f"Cleaning up very old session: {key} (age: {age_hours/24:.1f} days)")
                        elif age_hours > 48:  # 2 days
                            # Clean up completed, cancelled, or failed sessions after 2 days
                            if session_status in ['completed', 'cancelled', 'failed']:
                                should_cleanup = True
                                print(f"Cleaning up finished session: {key} (status: {session_status}, age: {age_hours:.1f}h)")
                        
                        if should_cleanup:
                            self.redis_client.delete(key)  # Remove await
                            cleaned_count += 1
                            
                except Exception as e:
                    print(f"Error processing session {key}: {str(e)}")
                    # Only delete corrupted session data if it's very old or unreadable
                    try:
                        # Try to get the TTL to see if it's a very old key
                        ttl = self.redis_client.ttl(key)  # Remove await
                        if ttl == -1:  # No expiration set, likely corrupted
                            self.redis_client.delete(key)  # Remove await
                            cleaned_count += 1
                            print(f"Deleted corrupted session (no TTL): {key}")
                        elif ttl < 3600:  # Less than 1 hour remaining
                            self.redis_client.delete(key)  # Remove await
                            cleaned_count += 1
                            print(f"Deleted corrupted session (expiring soon): {key}")
                    except Exception as delete_error:
                        print(f"Failed to delete corrupted session: {delete_error}")
            
            print(f"Session cleanup completed. Cleaned {cleaned_count} sessions")
                    
        except Exception as e:
            print(f"Error during session cleanup: {str(e)}")
            
    async def cleanup_incomplete_uploads(self):
        """Clean up incomplete multipart uploads directly from S3"""
        logger.info("Starting S3 incomplete uploads cleanup")
        
        try:
            # List incomplete multipart uploads older than 7 days
            cutoff_date = datetime.now().replace(tzinfo=None)  # Make timezone naive
            cutoff_date = cutoff_date - timedelta(days=7)
            
            response = self.s3_client.list_multipart_uploads(
                Bucket=self.bucket_name,
                Prefix="uploads/"
            )
            
            uploads = response.get("Uploads", [])
            cleanup_count = 0
            
            for upload in uploads:
                # Convert S3 datetime to naive datetime for comparison
                initiated_date = upload["Initiated"]
                if hasattr(initiated_date, 'tzinfo') and initiated_date.tzinfo is not None:
                    initiated_date = initiated_date.replace(tzinfo=None)
                
                if initiated_date < cutoff_date:
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