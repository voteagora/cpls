# Common Proposal Listing Service 

A Python server for sequential job processing with queue management, scheduling, and Google Cloud Storage integration, that builds a common list of proposals.

## Features

- ✅ **In-memory job queue** - Processes jobs sequentially (max 1 at a time) without blocking HTTP endpoints
- ✅ **POST endpoint** (`/jobs`) - Receives new job notifications from external producers
- ✅ **Scheduled jobs** - Automatically enqueues new jobs at configurable intervals
- ✅ **Web UI** (`/`) - Dashboard showing all jobs and their processing status
- ✅ **GCS integration** - Writes gzipped JSON results to Google Cloud Storage after each job

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


# Appendix - Source Schema Analysis

```
--------------------------------------------------------------------------------
UNIQUE FIELDS BY SOURCE
--------------------------------------------------------------------------------

Fields unique to 'dao_node':
  - block_number: str
  - calldatas: list[str]
  - cancel_event: dict
  - cancel_event.block_number: str
  - cancel_event.id: str
  - cancel_event.log_index: int
  - cancel_event.timestamp: int
  - cancel_event.transaction_index: int
  - decoded_proposal_data: list[list[bool, int]], list[list[int, str], list[list[int, list[empty], str]]], list[list[int, str], list[list[int, list[int], list[str], str]]], list[list[int, str], list[list[list[empty], str]]], list[list[int, str], list[list[list[int], list[str], str]]]
  - execute_event: dict
  - execute_event.block_number: str
  - execute_event.id: str
  - execute_event.log_index: int
  - execute_event.timestamp: int
  - execute_event.transaction_index: int
  - log_index: int
  - proposal_data: str
  - queue_event: dict
  - queue_event.block_number: str
  - queue_event.eta: int
  - queue_event.id: str
  - queue_event.log_index: int
  - queue_event.timestamp: int
  - queue_event.transaction_index: int
  - signatures: list[str]
  - targets: list[str]
  - totals: dict
  - totals.0: dict
  - totals.0.0: str
  - totals.0.1: str
  - totals.1: dict
  - totals.1.0: str
  - totals.1.1: str
  - totals.10: dict
  - totals.10.0: str
  - totals.10.1: str
  - totals.11: dict
  - totals.11.0: str
  - totals.11.1: str
  - totals.12: dict
  - totals.12.0: str
  - totals.12.1: str
  - totals.13: dict
  - totals.13.0: str
  - totals.13.1: str
  - totals.14: dict
  - totals.14.1: str
  - totals.15: dict
  - totals.15.1: str
  - totals.16: dict
  - totals.16.1: str
  - totals.17: dict
  - totals.17.1: str
  - totals.18: dict
  - totals.18.1: str
  - totals.19: dict
  - totals.19.1: str
  - totals.2: dict
  - totals.2.0: str
  - totals.2.1: str
  - totals.20: dict
  - totals.20.1: str
  - totals.21: dict
  - totals.21.1: str
  - totals.3: dict
  - totals.3.0: str
  - totals.3.1: str
  - totals.4: dict
  - totals.4.0: str
  - totals.4.1: str
  - totals.5: dict
  - totals.5.0: str
  - totals.5.1: str
  - totals.6: dict
  - totals.6.0: str
  - totals.6.1: str
  - totals.7: dict
  - totals.7.0: str
  - totals.7.1: str
  - totals.8: dict
  - totals.8.0: str
  - totals.8.1: str
  - totals.9: dict
  - totals.9.0: str
  - totals.9.1: str
  - totals.no-param: dict
  - totals.no-param.0: str
  - totals.no-param.1: str
  - totals.no-param.2: str
  - transaction_index: int
  - values: list[int]
  - voting_module: str
  - voting_module_name: str

Fields unique to 'eas-atlas':
  - attester: str
  - calculationOptions: int
  - choices: list[empty], list[str]
  - contract: str
  - criteria: int
  - criteria_value: int
  - expirationTime: int
  - max_approvals: int
  - onchain_proposalid: int
  - outcome.APP: dict
  - outcome.APP.0: dict, int
  - outcome.APP.0.1: int
  - outcome.APP.1: dict, int
  - outcome.APP.1.1: int
  - outcome.APP.3: dict
  - outcome.APP.3.1: int
  - outcome.APP.4: dict
  - outcome.APP.4.1: int
  - outcome.APP.5: dict
  - outcome.APP.5.1: int
  - outcome.APP.8: dict
  - outcome.APP.8.1: int
  - outcome.CHAIN: dict
  - outcome.CHAIN.1: dict, int
  - outcome.CHAIN.1.1: int
  - outcome.USER: dict
  - outcome.USER.0: dict, int
  - outcome.USER.0.1: int
  - outcome.USER.1: dict, int
  - outcome.USER.1.1: int
  - outcome.USER.2: dict, int
  - outcome.USER.2.1: int
  - outcome.USER.3: dict
  - outcome.USER.3.1: int
  - outcome.USER.4: dict
  - outcome.USER.4.1: int
  - outcome.USER.5: dict
  - outcome.USER.5.1: int
  - outcome.USER.6: dict
  - outcome.USER.6.1: int
  - outcome.USER.7: dict
  - outcome.USER.7.1: int
  - outcome.USER.8: dict
  - outcome.USER.8.1: int
  - outcome.USER.9: dict
  - outcome.USER.9.1: int
  - proposal_type_id: int
  - recipient: str
  - refUID: str
  - resolver: str
  - revocable: bool
  - revocationTime: int
  - schema: str
  - tiers: list[empty], list[int]
  - time: int

Fields unique to 'eas-oodao':
  - dao_id: str
  - outcome.token-holders: dict
  - outcome.token-holders.0: str
  - outcome.token-holders.1: str
  - tags: list[str]
  - transaction_hash: str

--------------------------------------------------------------------------------
FIELDS COMMON TO ALL SOURCES
--------------------------------------------------------------------------------

11 field(s) present in all sources:

  data_eng_properties:
    [dao_node]: dict
    [eas-atlas]: dict
    [eas-oodao]: dict

  data_eng_properties.liveness:
    [dao_node]: str
    [eas-atlas]: str
    [eas-oodao]: str

  data_eng_properties.source:
    [dao_node]: str
    [eas-atlas]: str
    [eas-oodao]: str

  end_block:
    [dao_node]: int
    [eas-atlas]: int
    [eas-oodao]: int

  end_blocktime:
    [dao_node]: int
    [eas-atlas]: int
    [eas-oodao]: int

  id:
    [dao_node]: str
    [eas-atlas]: str
    [eas-oodao]: str

  proposer:
    [dao_node]: str
    [eas-atlas]: str
    [eas-oodao]: str

  proposer_ens:
    [dao_node]: null, str
    [eas-atlas]: null
    [eas-oodao]: null

  start_block:
    [dao_node]: int
    [eas-atlas]: int
    [eas-oodao]: int

  start_blocktime:
    [dao_node]: int
    [eas-atlas]: int
    [eas-oodao]: int

  title:
    [dao_node]: str
    [eas-atlas]: str
    [eas-oodao]: str
```
