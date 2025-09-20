"""
Job Processing Server with Queue, Scheduler, and GCS Integration
"""

import asyncio
import gzip
import json
import os
from datetime import datetime
from typing import Dict, List, Optional
from enum import Enum
import uuid

from contextlib import asynccontextmanager
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from google.cloud import storage
import uvicorn

from ui import generate_dashboard_html

# Load environment variables from .env file
load_dotenv()


class JobStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class Job(BaseModel):
    id: str
    type: str
    payload: Dict
    status: JobStatus = JobStatus.PENDING
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None


class JobRequest(BaseModel):
    type: str
    payload: Dict


class JobQueue:
    def __init__(self):
        self.queue: asyncio.Queue = asyncio.Queue()
        self.jobs: Dict[str, Job] = {}
        self.current_job: Optional[Job] = None
        self.processing = False

    async def add_job(self, job_type: str, payload: Dict) -> str:
        job_id = str(uuid.uuid4())
        job = Job(
            id=job_id,
            type=job_type,
            payload=payload,
            created_at=datetime.now()
        )
        self.jobs[job_id] = job
        await self.queue.put(job)
        return job_id

    async def process_jobs(self):
        """Process jobs sequentially from the queue"""
        self.processing = True
        while self.processing:
            try:
                job = await self.queue.get()
                self.current_job = job
                job.status = JobStatus.PROCESSING
                job.started_at = datetime.now()

                try:
                    # Simulate job processing
                    await self._execute_job(job)
                    job.status = JobStatus.COMPLETED
                except Exception as e:
                    job.status = JobStatus.FAILED
                    job.error = str(e)
                finally:
                    job.completed_at = datetime.now()
                    self.current_job = None

                    # Upload result to GCS
                    await upload_job_result_to_gcs(job)

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Error processing job: {e}")

    async def _execute_job(self, job: Job):
        """Execute the actual job logic"""
        # Add your job processing logic here
        print(f"Processing job {job.id} of type {job.type}")

        # Simulate work based on job type
        if job.type == "scheduled":
            await asyncio.sleep(2)  # Simulate scheduled job work
        elif job.type == "external":
            await asyncio.sleep(3)  # Simulate external job work
        else:
            await asyncio.sleep(1)  # Default job work

        print(f"Completed job {job.id}")

    def get_all_jobs(self) -> List[Job]:
        """Get all jobs sorted by creation time"""
        return sorted(self.jobs.values(), key=lambda x: x.created_at, reverse=True)

    def stop(self):
        """Stop processing jobs"""
        self.processing = False


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


# Load configuration from environment
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "cpls-usmr-dev-25q3")
SERVER_HOST = os.getenv("SERVER_HOST", "0.0.0.0")
SERVER_PORT = int(os.getenv("SERVER_PORT", "8000"))
SCHEDULER_INTERVAL_MINUTES = int(os.getenv("SCHEDULER_INTERVAL_MINUTES", "10"))
MAX_JOB_PROCESSING_TIME = int(os.getenv("MAX_JOB_PROCESSING_TIME_SECONDS", "300"))
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

# Initialize components
job_queue = JobQueue()
scheduler = AsyncIOScheduler()
gcs_uploader = GCSUploader(GCS_BUCKET_NAME)


async def upload_job_result_to_gcs(job: Job):
    """Helper function to upload job results to GCS"""
    try:
        await gcs_uploader.upload_job_result(job)
    except Exception as e:
        print(f"Failed to upload job {job.id} to GCS: {e}")


async def scheduled_job():
    """Function to be called by the scheduler periodically"""
    job_id = await job_queue.add_job(
        job_type="scheduled",
        payload={
            "message": "Scheduled job",
            "timestamp": datetime.now().isoformat(),
            "interval_minutes": SCHEDULER_INTERVAL_MINUTES
        }
    )
    print(f"Added scheduled job: {job_id} (interval: {SCHEDULER_INTERVAL_MINUTES} minutes)")


@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    """Manage application lifecycle"""
    # Startup
    print("Starting server...")

    # Start job processor in background
    asyncio.create_task(job_queue.process_jobs())

    # Configure scheduler to run at specified interval
    scheduler.add_job(
        scheduled_job,
        'interval',
        minutes=SCHEDULER_INTERVAL_MINUTES,
        id='scheduled_job',
        max_instances=1
    )
    scheduler.start()
    print(f"Scheduler configured to run every {SCHEDULER_INTERVAL_MINUTES} minutes")

    # Run first scheduled job immediately
    await scheduled_job()

    print("Server started: Job processor and scheduler are running")

    yield

    # Shutdown
    print("Shutting down server...")
    job_queue.stop()
    scheduler.shutdown()
    print("Server shutdown: Stopped job processor and scheduler")


# Create FastAPI app with lifespan manager
app = FastAPI(title="Job Processing Server", lifespan=lifespan)


@app.post("/jobs", response_model=Dict[str, str])
async def create_job(job_request: JobRequest):
    """
    POST endpoint to receive new job notifications from external producers
    """
    job_id = await job_queue.add_job(
        job_type=job_request.type,
        payload=job_request.payload
    )
    return {"job_id": job_id, "status": "queued"}


@app.get("/jobs/{job_id}")
async def get_job(job_id: str):
    """Get details of a specific job"""
    if job_id not in job_queue.jobs:
        raise HTTPException(status_code=404, detail="Job not found")

    job = job_queue.jobs[job_id]
    return {
        "id": job.id,
        "type": job.type,
        "status": job.status,
        "created_at": job.created_at.isoformat(),
        "started_at": job.started_at.isoformat() if job.started_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
        "error": job.error
    }


@app.get("/", response_class=HTMLResponse)
async def job_dashboard():
    """
    GET endpoint that serves a UI to show the list of jobs being processed
    """
    jobs = job_queue.get_all_jobs()
    return generate_dashboard_html(jobs, JobStatus)


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "queue_size": job_queue.queue.qsize(),
        "total_jobs": len(job_queue.jobs),
        "current_job": job_queue.current_job.id if job_queue.current_job else None
    }


if __name__ == "__main__":
    print(f"Starting server in {ENVIRONMENT} mode")
    print(f"GCS Bucket: {GCS_BUCKET_NAME}")
    print(f"Scheduler Interval: {SCHEDULER_INTERVAL_MINUTES} minutes")
    uvicorn.run(app, host=SERVER_HOST, port=SERVER_PORT)