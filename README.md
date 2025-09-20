# Job Processing Server

A Python server for sequential job processing with queue management, scheduling, and Google Cloud Storage integration.

## Features

✅ **In-memory job queue** - Processes jobs sequentially (max 1 at a time) without blocking HTTP endpoints
✅ **POST endpoint** (`/jobs`) - Receives new job notifications from external producers
✅ **Scheduled jobs** - Automatically enqueues new jobs at configurable intervals
✅ **Web UI** (`/`) - Dashboard showing all jobs and their processing status
✅ **GCS integration** - Writes gzipped JSON results to Google Cloud Storage after each job

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

### Environment Variables

The server uses environment variables for configuration. Create a `.env` file in the project root (copy from `.env.example`):

```bash
cp .env.example .env
```

Edit the `.env` file with your configuration:

```env
# Google Cloud Storage Configuration
GCS_BUCKET_NAME=your-bucket-name

# Server Configuration
SERVER_HOST=0.0.0.0
SERVER_PORT=8000

# Scheduler Configuration
SCHEDULER_INTERVAL_MINUTES=10

# Environment
ENVIRONMENT=development
```

### GCS Authentication

For Google Cloud Storage, ensure you have either:
- `GOOGLE_APPLICATION_CREDENTIALS` environment variable pointing to a service account key
- Application Default Credentials configured via `gcloud auth`

## Running the Server

```bash
python server.py
```

Or with uvicorn directly:

```bash
uvicorn server:app --host 0.0.0.0 --port 8000 --reload
```

## API Endpoints

### POST /jobs
Submit a new job to the queue:

```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{"type": "external", "payload": {"data": "example"}}'
```

Response:
```json
{
  "job_id": "uuid-here",
  "status": "queued"
}
```

### GET /
View the job dashboard UI in your browser:
```
http://localhost:8000
```

### GET /jobs/{job_id}
Get details of a specific job:

```bash
curl http://localhost:8000/jobs/{job_id}
```

### GET /health
Health check endpoint:

```bash
curl http://localhost:8000/health
```

## Job Processing

- Jobs are processed sequentially (one at a time)
- Each job goes through states: `pending` → `processing` → `completed`/`failed`
- Scheduled jobs are automatically added based on SCHEDULER_INTERVAL_MINUTES
- All completed jobs are uploaded to GCS as gzipped JSON files

## GCS Output Format

Jobs are saved to GCS with the following structure:
- Path: `jobs/{job_type}/{timestamp}_{job_id}.json.gz`
- Content: Gzipped JSON containing job details, status, and timing information

## Example Usage

1. Start the server:
```bash
python server.py
```

2. Submit a job:
```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{"type": "data-processing", "payload": {"source": "api", "action": "transform"}}'
```

3. View the dashboard:
Open http://localhost:8000 in your browser to see all jobs and their status

## Development

To extend job processing logic, modify the `_execute_job` method in the `JobQueue` class:

```python
async def _execute_job(self, job: Job):
    if job.type == "your-custom-type":
        # Add your custom processing logic here
        pass
```