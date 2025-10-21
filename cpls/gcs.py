from google.cloud import storage
from datetime import datetime
import json
import gzip
import os
from pathlib import Path
from typing import Dict, List, Optional, TYPE_CHECKING

from config import ENVIRONMENT

if TYPE_CHECKING:
    from jobs import Job


class GCSClient:
    def __init__(self, bucket_name: str, write_local_copies: bool = False, local_copy_dir: str = "/Users/jm/code/cpls/data"):
        self.bucket_name = bucket_name
        self.client = None
        self.bucket = None
        self.write_local_copies = write_local_copies
        self.local_copy_dir = local_copy_dir

        # Create local copy directory if needed
        if self.write_local_copies:
            Path(self.local_copy_dir).mkdir(parents=True, exist_ok=True)
            print(f"Local copies will be written to: {self.local_copy_dir}")

        # Initialize GCS client if credentials are available
        try:
            self.client = storage.Client()
            self.bucket = self.client.bucket(bucket_name)
        except Exception as e:
            print(f"Warning: GCS client not initialized: {e}")

    async def get_blob(self, blob_name):
        blob = self.bucket.blob(blob_name)
        return blob
    
    async def list_blobs(self, prefix):
        blobs = self.bucket.list_blobs(prefix=prefix)
        return blobs

    def _write_local_copy(self, blob_name: str, data: bytes, is_text: bool = False):
        """
        Write a local copy of the blob to disk

        Args:
            blob_name: The GCS blob name (used to create the local file path)
            data: The data to write (bytes)
            is_text: If True, write as text file; if False, write as binary
        """
        if not self.write_local_copies:
            return

        try:
            # Create full local path
            local_path = Path(self.local_copy_dir) / blob_name

            # Create parent directories if needed
            local_path.parent.mkdir(parents=True, exist_ok=True)

            # Write the file
            if is_text:
                local_path.write_text(data.decode() if isinstance(data, bytes) else data)
            else:
                local_path.write_bytes(data)

            print(f"Wrote local copy to: {local_path}")
        except Exception as e:
            print(f"Warning: Failed to write local copy of {blob_name}: {e}")

    async def upload_dict(self, data: Dict, blob_name: str, cache_control: Optional[str] = None, metadata: Optional[Dict[str, str]] = None) -> bool:
        """
        Upload a Python dictionary as gzipped JSON to GCS

        Args:
            data: Dictionary to upload
            blob_name: Blob name ending with .json (will be converted to .json.gz)
            cache_control: Cache-Control header value (e.g., "public, max-age=3600")
            metadata: Key-value pairs to store as blob metadata

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

            # Set cache control if provided
            if cache_control:
                compressed_blob.cache_control = cache_control

            # Set metadata if provided
            if metadata:
                compressed_blob.metadata = metadata

            compressed_blob.upload_from_string(compressed_data, content_type="application/gzip")
            # print(f"Uploaded compressed data to GCS: {compressed_blob_name}")

            # Write local copy of compressed version
            self._write_local_copy(compressed_blob_name, compressed_data, is_text=False)

            # Upload uncompressed version in development mode
            if ENVIRONMENT == "development":
                uncompressed_blob = self.bucket.blob(blob_name)

                # Set cache control if provided
                if cache_control:
                    uncompressed_blob.cache_control = cache_control

                # Set metadata if provided
                if metadata:
                    uncompressed_blob.metadata = metadata

                uncompressed_blob.upload_from_string(json_data, content_type="application/json")
                # print(f"Uploaded uncompressed data to GCS (dev): {blob_name}")

                # Write local copy of uncompressed version
                self._write_local_copy(blob_name, json_data.encode(), is_text=True)

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

    async def upload_ndjson(self, data: List[Dict], blob_name: str, cache_control: Optional[str] = None, metadata: Optional[Dict[str, str]] = None) -> bool:
        """
        Upload a list of dictionaries as gzipped NDJSON to GCS

        Args:
            data: List of dictionaries to upload
            blob_name: Blob name ending with .ndjson (will be converted to .ndjson.gz)
            cache_control: Cache-Control header value (e.g., "public, max-age=3600")
            metadata: Key-value pairs to store as blob metadata

        Returns:
            bool: True if successful, False otherwise
        """
        if not self.client:
            print("GCS client not available, skipping upload")
            return False

        try:
            # Ensure blob name ends with .ndjson
            if not blob_name.endswith('.ndjson'):
                raise ValueError("Blob name must end with '.ndjson'")

            # Convert to NDJSON (newline-delimited JSON)
            ndjson_data = '\n'.join(json.dumps(item) for item in data)
            compressed_data = gzip.compress(ndjson_data.encode())

            # Create compressed blob name
            compressed_blob_name = blob_name.replace('.ndjson', '.ndjson.gz')

            # Upload compressed version
            compressed_blob = self.bucket.blob(compressed_blob_name)

            # Set cache control if provided
            if cache_control:
                compressed_blob.cache_control = cache_control

            # Set metadata if provided
            if metadata:
                compressed_blob.metadata = metadata

            compressed_blob.upload_from_string(compressed_data, content_type="application/gzip")
            print(f"Uploaded compressed NDJSON to GCS: {compressed_blob_name}")

            # Write local copy of compressed version
            self._write_local_copy(compressed_blob_name, compressed_data, is_text=False)

            # Upload uncompressed version in development mode
            if ENVIRONMENT == "development":
                uncompressed_blob = self.bucket.blob(blob_name)

                # Set cache control if provided
                if cache_control:
                    uncompressed_blob.cache_control = cache_control

                # Set metadata if provided
                if metadata:
                    uncompressed_blob.metadata = metadata

                uncompressed_blob.upload_from_string(ndjson_data, content_type="application/x-ndjson")
                print(f"Uploaded uncompressed NDJSON to GCS (dev): {blob_name}")

                # Write local copy of uncompressed version
                self._write_local_copy(blob_name, ndjson_data.encode(), is_text=True)

            return True

        except Exception as e:
            print(f"Failed to upload NDJSON to GCS blob {blob_name}: {e}")
            return False

    async def read_ndjson(self, blob_name: str) -> Optional[List[Dict]]:
        """
        Read a gzipped NDJSON blob from GCS and return as list of dictionaries

        Args:
            blob_name: Blob name ending with .ndjson (will be converted to .ndjson.gz)

        Returns:
            List[Dict] if successful, None otherwise
        """
        if not self.client:
            print("GCS client not available, cannot read")
            return None

        try:
            # Ensure blob name ends with .ndjson
            if not blob_name.endswith('.ndjson'):
                raise ValueError("Blob name must end with '.ndjson'")

            # Create compressed blob name
            compressed_blob_name = blob_name.replace('.ndjson', '.ndjson.gz')

            # Download compressed blob
            compressed_blob = self.bucket.blob(compressed_blob_name)

            if not compressed_blob.exists():
                print(f"Blob {compressed_blob_name} does not exist")
                return None

            compressed_data = compressed_blob.download_as_bytes()

            # Decompress and parse NDJSON
            ndjson_data = gzip.decompress(compressed_data).decode()

            # Parse each line as JSON
            data = []
            for line in ndjson_data.strip().split('\n'):
                if line.strip():  # Skip empty lines
                    data.append(json.loads(line))

            print(f"Successfully read {len(data)} records from GCS: {compressed_blob_name}")
            return data

        except Exception as e:
            print(f"Failed to read NDJSON from GCS blob {blob_name}: {e}")
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

        # Use the generic upload method with job-specific metadata
        job_metadata = {
            "job_id": job.id,
            "job_type": job.type,
            "job_status": job.status,
            "upload_timestamp": datetime.now().isoformat()
        }

        return await self.upload_dict(
            job_data,
            blob_name,
            cache_control="private, max-age=86400",  # Cache for 24 hours
            metadata=job_metadata
        )

    async def safe_upload_job_result(self, job: 'Job'):
        """Helper function to safely upload job results to GCS"""
        try:
            await self.upload_job_result(job)
        except Exception as e:
            print(f"Failed to upload job {job.id} to GCS: {e}")


# For backward compatibility
GCSUploader = GCSClient