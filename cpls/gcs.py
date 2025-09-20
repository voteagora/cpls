from google.cloud import storage
from datetime import datetime
import json
import gzip

from jobs import Job

from config import ENVIRONMENT

class GCSUploader:
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.client = None
        self.bucket = None

        # Initialize GCS client if credentials are available
        try:
            self.client = storage.Client()
            self.bucket = self.client.bucket(bucket_name)
        except Exception as e:
            print(f"Warning: GCS client not initialized: {e}")

    async def upload_job_result(self, job: Job):
        """Upload job result as gzipped JSON to GCS (and uncompressed in dev mode)"""
        if not self.client:
            print("GCS client not available, skipping upload")
            return

        # Prepare job data
        job_data = {
            "id": job.id,
            "type": job.type,
            "payload": job.payload,
            "status": job.status,
            "created_at": job.created_at.isoformat(),
            "started_at": job.started_at.isoformat() if job.started_at else None,
            "completed_at": job.completed_at.isoformat() if job.completed_at else None,
            "error": job.error
        }

        # Convert to JSON
        json_data = json.dumps(job_data, indent=2)

        # Create timestamp for file naming
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Upload compressed version (always)
        compressed_data = gzip.compress(json_data.encode())
        compressed_blob_name = f"jobs/{job.type}/{timestamp}_{job.id}.json.gz"

        compressed_blob = self.bucket.blob(compressed_blob_name)
        compressed_blob.upload_from_string(compressed_data, content_type="application/gzip")
        print(f"Uploaded compressed job {job.id} result to GCS: {compressed_blob_name}")

        # Upload uncompressed version in development mode
        if ENVIRONMENT == "development":
            uncompressed_blob_name = f"jobs/{job.type}/{timestamp}_{job.id}.json"
            uncompressed_blob = self.bucket.blob(uncompressed_blob_name)
            uncompressed_blob.upload_from_string(json_data, content_type="application/json")
            print(f"Uploaded uncompressed job {job.id} result to GCS (dev): {uncompressed_blob_name}")


    async def safe_upload_job_result(self, job: Job):
        """Helper function to upload job results to GCS"""
        try:
            await self.upload_job_result(job)
        except Exception as e:
            print(f"Failed to upload job {job.id} to GCS: {e}")
        