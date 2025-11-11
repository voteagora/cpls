"""
Job Processing Server with Queue, Scheduler, and GCS Integration
"""

import asyncio
import copy
from datetime import datetime
from typing import Dict
from collections import defaultdict

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

from apscheduler.schedulers.asyncio import AsyncIOScheduler
import uvicorn

from .ui import generate_dashboard_html
from .gcs import GCSClient
from .jobs import JobQueue, JobRequest, JobStatus
import time

from .config import INFRA_DAO_SLUGS, ENVIRONMENT, GCS_BUCKET_NAME, SERVER_HOST, SERVER_PORT, SCHEDULER_INTERVAL_MINUTES, load_tenant_configs, RESET_PROPOSALS_ON_RESTART


reset_tracker = defaultdict(lambda: RESET_PROPOSALS_ON_RESTART)

# Initialize components
job_queue = JobQueue(15)
scheduler = AsyncIOScheduler()
gcs_client = GCSClient(GCS_BUCKET_NAME)


async def scheduled_proposal_job(config: Dict, infra_dao_slug: str):
    """Function to be called by the scheduler periodically"""

    sources = []

    print("Config:")
    print(config['features'])

    assert isinstance(config['features'].get('oodao', False), bool)
    assert isinstance(config['features'].get('snapshot_proposals', False), bool)
    assert isinstance(config['features'].get('dao_node_proposals', False), bool)

    print(config['features'].get('dao_node_proposals', False))

    if config['features'].get('oodao', False):
        sources.append('eas-oodao')
    
    if config['features'].get('snapshot_proposals', False):
        sources.append('snapshot')

    if infra_dao_slug == 'optimism':
        sources.append('eas-atlas')

    if config['features'].get('dao_node_proposals', False):
        sources.append('dao_node')
    
    print(sources)

    job_id = await job_queue.add_job(
        job_type="scheduled",
        payload={
            "message": f"Scheduled job - {infra_dao_slug}",
            "timestamp": datetime.now().isoformat(),
            # "interval_minutes": SCHEDULER_INTERVAL_MINUTES,
            "infra_dao_slug": infra_dao_slug,
            "logic": "refresh_list",
            "sources": sources,
            "config": config,
            "reset": reset_tracker[infra_dao_slug]
        }
    )

    reset_tracker[infra_dao_slug] = False

    print(f"Added scheduled job: {job_id} for infra_dao_slug: {infra_dao_slug} w/ sources: {sources} @ interval: {SCHEDULER_INTERVAL_MINUTES} minutes)")

async def scheduled_ens_job(config: Dict, infra_dao_slug: str):
    """Function to be called by the scheduler periodically"""

    sources = []

    assert isinstance(config['features'].get('oodao', False), bool)
    assert isinstance(config['features'].get('snapshot_proposals', False), bool)
    assert isinstance(config['features'].get('dao_node_proposals', False), bool)

    if config['features'].get('oodao', False):
        sources.append('eas-oodao')
    
    if config['features'].get('snapshot_proposals', False):
        sources.append('snapshot')

    if infra_dao_slug == 'optimism':
        sources.append('eas-atlas')

    if config['features'].get('dao_node_proposals', False):
        sources.append('dao_node')

    job_id = await job_queue.add_job(
        job_type="scheduled",
        payload={
            "message": f"Scheduled job - {infra_dao_slug}",
            "timestamp": datetime.now().isoformat(),
            # "interval_minutes": SCHEDULER_INTERVAL_MINUTES,
            "infra_dao_slug": infra_dao_slug,
            "logic": "refresh_ens",
            "sources": sources,
            "config": config,
        }
    )

    print(f"Added scheduled ens job: {job_id} for infra_dao_slug: {infra_dao_slug} w/ sources: {sources} @ interval: {SCHEDULER_INTERVAL_MINUTES} minutes)")


@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    """Manage application lifecycle"""
    # Startup
    print("Starting server...")

    # Load tenant configs once at startup and attach to app state
    tenants_config = load_tenant_configs()
    print(f"Loaded {len(tenants_config)} tenant configurations")

    # Start job processor in background
    asyncio.create_task(job_queue.process_jobs(gcs_client))

    if "all" in INFRA_DAO_SLUGS:
        infra_dao_slugs = list(tenants_config.keys())
    else:    
        infra_dao_slugs = INFRA_DAO_SLUGS


    for infra_dao_slug in infra_dao_slugs:
        # Configure scheduler to run at specified interval

        config = copy.copy(tenants_config[infra_dao_slug])

        assert infra_dao_slug == config['schema']

        scheduler.add_job(
            scheduled_proposal_job,
            'interval',
            minutes=SCHEDULER_INTERVAL_MINUTES,
            id='scheduled-proposal-job-' + infra_dao_slug,
            max_instances=1,
            kwargs = {'config' : config, 'infra_dao_slug' : infra_dao_slug}
        )


    scheduler.start()

    print(f"Scheduler configured to run every {SCHEDULER_INTERVAL_MINUTES} minutes")

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