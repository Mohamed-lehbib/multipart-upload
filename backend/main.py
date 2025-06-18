from http.client import HTTPException
from typing import List
from fastapi import FastAPI, UploadFile, File, Form, Body
import boto3
from uuid import uuid4
from fastapi.middleware.cors import CORSMiddleware
import os
from dotenv import load_dotenv

app = FastAPI()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

load_dotenv()

# AWS S3 Configuration
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")
REGION = os.getenv("AWS_REGION")


s3_client = boto3.client("s3", region_name="eu-west-3",
                         aws_access_key_id=AWS_ACCESS_KEY,
                         aws_secret_access_key=AWS_SECRET_KEY,
    config=boto3.session.Config(signature_version='s3v4'))

@app.post("/upload/initiate")
async def initiate_upload(filename: str = Form(...), content_type: str = Form(...)):
    key = f"uploads/{uuid4()}_{filename}"
    response = s3_client.create_multipart_upload(Bucket=BUCKET_NAME, Key=key,
        ContentType=content_type )
    return {"uploadId": response["UploadId"], "key": key}

@app.post("/upload/presigned-url")
async def get_presigned_url(key: str = Form(...), uploadId: str = Form(...), partNumber: int = Form(...)):
    url = s3_client.generate_presigned_url(
        "upload_part",
        Params={
            "Bucket": BUCKET_NAME,
            "Key": key,
            "UploadId": uploadId,
            "PartNumber": partNumber
        },
        ExpiresIn=3600,
        HttpMethod="PUT"
    )
    return {"url": url}

@app.post("/upload/complete")
async def complete_upload(
    key: str = Body(...),
    uploadId: str = Body(...),
    parts: List[dict] = Body(...)
):
    try:
        # Ensure parts are sorted by PartNumber
        sorted_parts = sorted(parts, key=lambda x: x['PartNumber'])
        
        response = s3_client.complete_multipart_upload(
            Bucket=BUCKET_NAME,
            Key=key,
            UploadId=uploadId,
            MultipartUpload={"Parts": sorted_parts}
        )
        return {"location": response["Location"]}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))