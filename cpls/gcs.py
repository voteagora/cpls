from google.cloud import storage
from datetime import datetime
import json
import gzip
from typing import Dict, Optional, TYPE_CHECKING

from config import ENVIRONMENT

if TYPE_CHECKING:
    from jobs import Job


class GCSClient:
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

    async def upload_dict(self, data: Dict, blob_name: str) -> bool:
        """
        Upload a Python dictionary as gzipped JSON to GCS

        Args:
            data: Dictionary to upload
            blob_name: Blob name ending with .json (will be converted to .json.gz)

        Returns:
            bool: True if successful, False otherwise
        """
        if not self.client:
            print("GCS client not available, skipping upload")
            return False

        try:
            # Ensure blob name ends with .json
            if not blob_name.endswith('.json'):
                raise ValueError("Blob name must end with '.json'")

            # Convert to JSON and compress
            json_data = json.dumps(data, indent=2)
            compressed_data = gzip.compress(json_data.encode())

            # Create compressed blob name
            compressed_blob_name = blob_name.replace('.json', '.json.gz')

            # Upload compressed version
            compressed_blob = self.bucket.blob(compressed_blob_name)
            compressed_blob.upload_from_string(compressed_data, content_type="application/gzip")
            print(f"Uploaded compressed data to GCS: {compressed_blob_name}")

            # Upload uncompressed version in development mode
            if ENVIRONMENT == "development":
                uncompressed_blob = self.bucket.blob(blob_name)
                uncompressed_blob.upload_from_string(json_data, content_type="application/json")
                print(f"Uploaded uncompressed data to GCS (dev): {blob_name}")

            return True

        except Exception as e:
            print(f"Failed to upload data to GCS blob {blob_name}: {e}")
            return False

    async def read_dict(self, blob_name: str) -> Optional[Dict]:
        """
        Read a gzipped JSON blob from GCS and return as dictionary

        Args:
            blob_name: Blob name ending with .json (will be converted to .json.gz)

        Returns:
            Dict if successful, None otherwise
        """
        if not self.client:
            print("GCS client not available, cannot read")
            return None

        try:
            # Ensure blob name ends with .json
            if not blob_name.endswith('.json'):
                raise ValueError("Blob name must end with '.json'")

            # Create compressed blob name
            compressed_blob_name = blob_name.replace('.json', '.json.gz')

            # Download compressed blob
            compressed_blob = self.bucket.blob(compressed_blob_name)

            if not compressed_blob.exists():
                print(f"Blob {compressed_blob_name} does not exist")
                return None

            compressed_data = compressed_blob.download_as_bytes()

            # Decompress and parse JSON
            json_data = gzip.decompress(compressed_data).decode()
            data = json.loads(json_data)

            print(f"Successfully read data from GCS: {compressed_blob_name}")
            return data

        except Exception as e:
            print(f"Failed to read data from GCS blob {blob_name}: {e}")
            return None

    async def upload_job_result(self, job: 'Job'):
        """Upload job result as gzipped JSON to GCS (legacy method for compatibility)"""
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

        # Create blob name with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        blob_name = f"jobs/{job.type}/{timestamp}_{job.id}.json"

        # Use the generic upload method
        return await self.upload_dict(job_data, blob_name)

    async def safe_upload_job_result(self, job: 'Job'):
        """Helper function to safely upload job results to GCS"""
        try:
            await self.upload_job_result(job)
        except Exception as e:
            print(f"Failed to upload job {job.id} to GCS: {e}")


# For backward compatibility
GCSUploader = GCSClient