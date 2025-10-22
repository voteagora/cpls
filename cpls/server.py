"""
Job Processing Server with Queue, Scheduler, and GCS Integration
"""

import asyncio
from datetime import datetime
from typing import Dict

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

from apscheduler.schedulers.asyncio import AsyncIOScheduler
import uvicorn

from .ui import generate_dashboard_html
from .gcs import GCSClient
from .jobs import JobQueue, JobRequest, JobStatus
import time

from .config import ENVIRONMENT, GCS_BUCKET_NAME, SERVER_HOST, SERVER_PORT, SCHEDULER_INTERVAL_MINUTES


# Initialize components
job_queue = JobQueue()
scheduler = AsyncIOScheduler()
gcs_client = GCSClient(GCS_BUCKET_NAME)


async def scheduled_job(infra_dao_slug):
    """Function to be called by the scheduler periodically"""

    if infra_dao_slug == 'optimism':
        sources = ['dao_node', 'eas-atlas']
    elif infra_dao_slug == 'jeffdao':
        sources = ['eas-oodao']
    elif infra_dao_slug in ('scroll', 'cyber', 'pguild'):
        sources = ['dao_node']
    else:
        raise Exception(f"Unknown infra_dao_slug: {infra_dao_slug}")

    job_id = await job_queue.add_job(
        job_type="scheduled",
        payload={
            "message": f"Scheduled job - {infra_dao_slug}",
            "timestamp": datetime.now().isoformat(),
            # "interval_minutes": SCHEDULER_INTERVAL_MINUTES,
            "infra_dao_slug": infra_dao_slug,
            "sources": sources
            
        }
    )
    print(f"Added scheduled job: {job_id} for infra_dao_slug: {infra_dao_slug} w/ sources: {sources} @ interval: {SCHEDULER_INTERVAL_MINUTES} minutes)")


@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    """Manage application lifecycle"""
    # Startup
    print("Starting server...")

    # Start job processor in background
    asyncio.create_task(job_queue.process_jobs(gcs_client))

    INFRA_DAO_SLUGS = ["scroll", "cyber", "pguild", "jeffdao"]

    for infra_dao_slug in INFRA_DAO_SLUGS:
        # Configure scheduler to run at specified interval
        scheduler.add_job(
            scheduled_job,
            'interval',
            minutes=SCHEDULER_INTERVAL_MINUTES,
            id='scheduled_job-' + infra_dao_slug,
            max_instances=1,
            kwargs = {'infra_dao_slug': infra_dao_slug}
        )

    scheduler.start()

    print(f"Scheduler configured to run every {SCHEDULER_INTERVAL_MINUTES} minutes")

    if ENVIRONMENT == 'development':
        for infra_dao_slug in INFRA_DAO_SLUGS:
            # Run first scheduled job immediately
            await scheduled_job(infra_dao_slug)

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