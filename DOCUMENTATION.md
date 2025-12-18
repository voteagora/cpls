# Common Proposal Listing Service (CPLS) - Technical Documentation

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Core Components](#core-components)
4. [Data Sources](#data-sources)
5. [Job Processing System](#job-processing-system)
6. [Data Storage & GCS Integration](#data-storage--gcs-integration)
7. [Multi-Tenant Configuration](#multi-tenant-configuration)
8. [Database Integration](#database-integration)
9. [External Services](#external-services)
10. [API Endpoints](#api-endpoints)
11. [Data Schema](#data-schema)
12. [Deployment](#deployment)
13. [Configuration Reference](#configuration-reference)

---

## Overview

CPLS (Common Proposal Listing Service) is a Python-based data aggregation and synchronization service designed to collect, normalize, and store governance proposals from multiple decentralized autonomous organizations (DAOs). The service aggregates proposal data from various sources (on-chain and off-chain) and produces a unified, queryable dataset stored in Google Cloud Storage (GCS).

### Key Capabilities

- **Multi-source proposal aggregation** - Collects proposals from 4 distinct data sources
- **Multi-tenant support** - Handles multiple DAOs (Optimism, ENS, Uniswap, Scroll, Cyber, etc.)
- **Scheduled synchronization** - Periodic job execution to keep data fresh
- **Intelligent caching** - Hash-based change detection to avoid redundant processing
- **Vote tracking** - Captures voter data, voting power snapshots, and delegate metadata
- **Lifecycle management** - Tracks proposal states (PENDING, ACTIVE, SUCCEEDED, DEFEATED, EXECUTED, etc.)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              CPLS Server                                     │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────────────┐  │
│  │   FastAPI App   │    │   APScheduler   │    │      Job Queue          │  │
│  │   (HTTP API)    │───▶│   (Scheduler)   │───▶│   (15 Workers)          │  │
│  └─────────────────┘    └─────────────────┘    └───────────┬─────────────┘  │
│                                                             │                │
│  ┌──────────────────────────────────────────────────────────▼──────────────┐│
│  │                         Sync Modules                                     ││
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐    ││
│  │  │ DaoNodeSync  │ │ SnapshotSync │ │ EASAtlasSync │ │ EASOoDaoSync │    ││
│  │  │ (On-chain)   │ │ (Off-chain)  │ │ (Optimism)   │ │ (OoDAO)      │    ││
│  │  └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘    ││
│  └──────────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
          ┌───────────────────────────┼───────────────────────────┐
          ▼                           ▼                           ▼
┌─────────────────┐         ┌─────────────────┐         ┌─────────────────┐
│  Google Cloud   │         │   PostgreSQL    │         │   BlockCache    │
│    Storage      │         │   Database      │         │    Service      │
│  (Data Output)  │         │  (Data Source)  │         │ (Blockchain RPC)│
└─────────────────┘         └─────────────────┘         └─────────────────┘
```

### Request Flow

1. **Scheduler triggers** a job at configured intervals (default: 10 minutes)
2. **Job Queue** receives the job and assigns it to an available worker
3. **Sync Module** fetches data from the appropriate source (API, database, blockchain)
4. **Data is processed**, enriched with metadata (ENS names, delegate info, voting power)
5. **Results are uploaded** to GCS as gzipped JSON/NDJSON files
6. **Proposal lists** are regenerated and uploaded

---


## Core Components

### 1. Server (`cpls/server.py`)

The main FastAPI application that orchestrates the entire service.

**Key Responsibilities:**
- Initializes the job queue with 15 concurrent workers
- Configures APScheduler for periodic job execution
- Loads tenant configurations at startup
- Provides HTTP endpoints for job management and monitoring
- Manages application lifecycle (startup/shutdown)

**Lifespan Management:**
```python
@asynccontextmanager
async def lifespan(app_instance: FastAPI):
    # Startup: Load configs, start workers, configure scheduler
    tenants_config = load_tenant_configs()
    asyncio.create_task(job_queue.process_jobs(gcs_client))
    scheduler.start()
    
    yield  # Application runs
    
    # Shutdown: Stop workers and scheduler
    job_queue.stop()
    scheduler.shutdown()
```

### 2. Job Queue (`cpls/jobs.py`)

A concurrent job processing system with DAO-level locking.

**Features:**
- **15 concurrent workers** - Process jobs in parallel
- **DAO-level locks** - Prevents concurrent processing of the same DAO
- **Job skipping** - If a DAO's lock is held, new jobs for that DAO are skipped
- **Status tracking** - Jobs go through: PENDING → PROCESSING → COMPLETED/FAILED/SKIPPED

**Job Structure:**
```python
class Job(BaseModel):
    id: str                          # UUID
    type: str                        # "scheduled" or custom type
    payload: Dict                    # Job-specific data
    status: JobStatus                # PENDING, PROCESSING, COMPLETED, FAILED, SKIPPED
    created_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    error: Optional[str]
    stats: Optional[Dict]            # Processing statistics
```

**Job Payload Structure:**
```python
{
    "message": "Scheduled job - optimism",
    "timestamp": "2024-01-15T10:30:00",
    "infra_dao_slug": "optimism",
    "logic": "refresh_list",
    "sources": ["dao_node", "eas-atlas", "snapshot"],
    "config": {...},                 # Tenant configuration
    "reset": False                   # Force reprocess archived proposals
}
```

### 3. Base Sync Class (`cpls/sync.py`)

Abstract base class providing common functionality for all sync modules.

**Core Methods:**

| Method | Description |
|--------|-------------|
| `refresh_list()` | Main entry point - fetches and processes proposals |
| `refresh_source_list()` | Regenerates per-source proposal list |
| `refresh_full_list()` | Regenerates combined proposal list |
| `overwrite_proposal()` | Uploads proposal to GCS with metadata |
| `overwrite_votes()` | Uploads vote data to GCS |
| `overwrite_hasnt_voted()` | Uploads non-voter delegate data |
| `get_vp_snapshot_all_delegates()` | Gets voting power snapshot at block |
| `get_delegate_metadata()` | Fetches delegate social links |
| `check_existing_proposal_hash()` | Hash-based change detection |

**Caching Strategy:**
- Proposals are hashed using SHA-256
- If hash matches existing blob, processing is skipped
- Archived proposals (ended + 5 minutes) are not reprocessed unless `reset=True`
- Cache-Control headers are set based on liveness:
  - **Live proposals**: 5 minutes (prod) / 1.6 minutes (dev)
  - **Archived proposals**: 1 year (prod) / 2 minutes (dev)

### 4. GCS Client (`cpls/gcs.py`)

Handles all Google Cloud Storage operations.

**Supported Operations:**
- `upload_dict()` - Upload JSON (optionally gzipped)
- `read_dict()` - Read JSON from GCS
- `upload_ndjson()` - Upload newline-delimited JSON (for lists)
- `read_ndjson()` - Read NDJSON from GCS
- `upload_job_result()` - Upload job execution results

**File Naming Convention:**
```
data/{dao_slug}/proposal/{source}/raw/{proposal_id}.json.gz     # Individual proposals
data/{dao_slug}/proposal_list/{source}/raw.ndjson.gz            # Per-source list
data/{dao_slug}/proposal_list.full.ndjson.gz                    # Combined list
data/{dao_slug}/votes/{proposal_id}.ndjson.gz                   # Vote records
data/{dao_slug}/hasnt_voted/{proposal_id}.ndjson.gz             # Non-voters
data/{dao_slug}/vpsnapshot/{source}/raw/{block}.ndjson.gz       # VP snapshots
jobs/{job_type}/{timestamp}_{job_id}.json                       # Job results
```

**Local Development:**
When `ENVIRONMENT=dev`, files are also written to a local directory for debugging.

---

## Data Sources

CPLS aggregates proposals from four distinct sources:

### 1. DaoNode (`cpls/sync_daonode.py`)

**Source Type:** On-chain governance proposals via DaoNode API

**Supported DAOs:** Optimism, Uniswap, ENS, Scroll, Cyber, and others

**Data Flow:**
1. Fetch progress from `https://{dao}.prod.agoradata.xyz/v1/progress`
2. Fetch proposals from `https://{dao}.prod.agoradata.xyz/v1/proposals`
3. For each proposal:
   - Fetch detailed data from `/v1/proposal/{id}`
   - Read votes from PostgreSQL database
   - Calculate quorum from smart contract
   - Determine lifecycle stage from on-chain state
   - Enrich with delegate metadata and ENS names

**Lifecycle States Handled:**
- `PENDING` - Before voting starts
- `ACTIVE` - Voting in progress
- `SUCCEEDED` - Passed but not queued
- `QUEUED` - Waiting for execution
- `EXECUTED` - Successfully executed
- `DEFEATED` - Failed to pass
- `CANCELLED` - Cancelled by proposer
- `PASSED` - Special case for unqueueable proposals

**Special Handling:**
- Censored/test proposals are skipped (hardcoded lists)
- Hybrid proposals (linked to EAS-Atlas) include off-chain data
- Quorum calculation varies by DAO (contract calls, votable supply, fixed values)

### 2. Snapshot (`cpls/sync_snapshot.py`)

**Source Type:** Off-chain governance via Snapshot GraphQL API

**Supported DAOs:** ENS, Uniswap, Derive, EtherFi

**Data Flow:**
1. Query Snapshot GraphQL API for proposals
2. For each proposal:
   - Fetch all votes (paginated, max 5 pages)
   - Calculate voting power snapshot for Copeland voting
   - Track non-voters with voting power

**Snapshot Spaces:**
```python
{
    'ens': 'ens.eth',
    'uniswap': 'uniswapgovernance.eth',
    'derive': 'derivexyz.eth',
    'etherfi': 'etherfi-dao.eth'
}
```

**Proposal States:**
- `active` → liveness = 'live'
- `closed` → liveness = 'archived'

### 3. EAS-Atlas (`cpls/sync_eas_atlas.py`)

**Source Type:** Ethereum Attestation Service (EAS) for Optimism governance

**Purpose:** Handles off-chain proposals that may be linked to on-chain execution

**Data Flow:**
1. Read proposal creation attestations from database
2. Fetch decoded attestation data from BlockCache service
3. Read votes from `atlas.VotesWithMeta` view
4. Read citizen data for non-voter tracking
5. Calculate outcomes based on proposal type

**Proposal Types:**
- `OPTIMISTIC` - Default approval unless vetoed
- `STANDARD` - Requires explicit approval
- `OPTIMISTIC_TIERED` - Tiered voting thresholds
- `APPROVAL` - Multiple choice approval voting

**Hybrid Proposals:**
Proposals can be "hybrid" - created off-chain via EAS but linked to on-chain execution. These are flagged and handled specially in the full list generation.

### 4. EAS-OoDAO (`cpls/sync_eas_oodao.py`)

**Source Type:** On-chain DAO (OoDAO) governance via EAS attestations

**Purpose:** Handles governance for DAOs using the OoDAO framework

**EAS Schema UIDs (Mainnet):**
```python
{
    'CREATE_PROPOSAL': '0x442d586d8424b5485de1ff46cb235dcb96b41d19834926bbad1cd157fbeeb8fc',
    'SIMPLE_VOTE': '0xa6abd1e115de8e83b71f6db6db22d44d730a217f21e6008034f94e682efe9c70',
    'ADVANCED_VOTE': '0x4baeed1a31235fddb0757c6b0e006fee241019cd0ed3e0706ec2b2d1329acfe4',
    'SET_PROPOSAL_TYPE': '0x5e302e7f8743e8369750afaf8a822d6ca4ecbee78b737c4276220732c4488d7a',
    'DELETE': '0x3dd1714d33cc8dc91f24307758d4309a54a3b17080a3059bc67585e97a769a1e',
    # ... more schemas
}
```

**Data Flow:**
1. Read proposals from `eas_attestations_v2` table
2. Read proposal deletions (for cancelled proposals)
3. Read proposal type (author-set or admin-approved)
4. Read votes from database
5. Calculate quorum and approval thresholds
6. Determine lifecycle stage

**Lifecycle Determination:**
```python
if 'delete_event' in proposal:
    lifecycle_stage = 'CANCELLED'
elif current_time < start_time:
    lifecycle_stage = 'PENDING'
elif start_time <= current_time < end_time:
    lifecycle_stage = 'ACTIVE'
elif current_time >= end_time:
    if quorum_check and approval_check:
        lifecycle_stage = 'PASSED'
    else:
        lifecycle_stage = 'DEFEATED'
```

---

## Job Processing System

### Scheduler Configuration

Jobs are scheduled using APScheduler with the following settings:

```python
scheduler.add_job(
    scheduled_proposal_job,
    'interval',
    minutes=SCHEDULER_INTERVAL_MINUTES,  # Default: 10
    id='scheduled-proposal-job-' + infra_dao_slug,
    max_instances=1,  # Prevent overlapping executions
    kwargs={'config': config, 'infra_dao_slug': infra_dao_slug}
)
```

### Worker Pool

The job queue uses 15 concurrent workers with DAO-level locking:

```python
class JobQueue:
    def __init__(self, num_workers: int = 15):
        self.queue: asyncio.Queue = asyncio.Queue()
        self.dao_locks: Dict[str, asyncio.Lock] = {}
        self.num_workers = num_workers
```

**Locking Behavior:**
- Each DAO has its own lock
- Only one job per DAO can run at a time
- If a DAO's lock is held, new jobs are marked as SKIPPED
- This prevents data corruption from concurrent writes

### Job Execution Flow

```
1. Scheduler triggers job
   ↓
2. Job added to queue
   ↓
3. Worker picks up job
   ↓
4. Acquire DAO lock
   ↓
5. For each source in job.sources:
   ├── Initialize sync module
   ├── Call refresh_list()
   ├── Collect stats (skipped, refreshed)
   └── Continue to next source
   ↓
6. Upload job result to GCS
   ↓
7. Release DAO lock
```

### Reset Behavior

The `RESET_PROPOSALS_ON_RESTART` configuration controls whether archived proposals are reprocessed on server restart:

```python
reset_tracker = defaultdict(lambda: RESET_PROPOSALS_ON_RESTART)

# First job after restart uses reset=True
# Subsequent jobs use reset=False
reset_tracker[infra_dao_slug] = False
```

---

## Data Storage & GCS Integration

### Bucket Structure

```
{bucket_name}/
├── data/
│   └── {dao_slug}/
│       ├── proposal/
│       │   ├── dao_node/raw/{proposal_id}.json.gz
│       │   ├── snapshot/raw/{proposal_id}.json.gz
│       │   ├── eas-atlas/raw/{proposal_id}.json.gz
│       │   └── eas-oodao/raw/{proposal_id}.json.gz
│       ├── proposal_list/
│       │   ├── dao_node/raw.ndjson.gz
│       │   ├── snapshot/raw.ndjson.gz
│       │   ├── eas-atlas/raw.ndjson.gz
│       │   └── eas-oodao/raw.ndjson.gz
│       ├── proposal_list.full.ndjson.gz
│       ├── votes/{proposal_id}.ndjson.gz
│       ├── hasnt_voted/{proposal_id}.ndjson.gz
│       └── vpsnapshot/{source}/raw/{block_number}.ndjson.gz
└── jobs/
    └── {job_type}/{timestamp}_{job_id}.json
```

### Blob Metadata

Each proposal blob includes metadata for efficient querying:

```python
metadata = {
    'proposal_id': '12345',
    'liveness': 'live',           # or 'archived'
    'source': 'dao_node',
    'hash': 'sha256_hash',
    'num_of_votes': 150
}
```

### Cache-Control Headers

```python
# Live proposals
cache_control = 'public, max-age=300'   # 5 minutes (prod)
cache_control = 'public, max-age=100'   # ~1.6 minutes (dev)

# Archived proposals
cache_control = 'public, max-age=31536000'  # 1 year (prod)
cache_control = 'public, max-age=120'       # 2 minutes (dev)
```

---

## Multi-Tenant Configuration

### Tenant Config Loading

Tenant configurations are loaded from YAML files at startup:

```python
TENANTS_CONFIG_PATH = Path('/config/envs/prod')

def load_tenant_configs():
    tenant_configs = {}
    for yaml_file in TENANTS_CONFIG_PATH.glob("*.yaml"):
        config_data = yaml.safe_load(yaml_file)
        
        # Extract deployment-specific config
        deployment = config_data['deployments'][DEPLOYMENT]
        del config_data['deployments']
        config_data['deployment'] = deployment
        
        tenant_slug = yaml_file.stem  # e.g., 'optimism', 'ens'
        tenant_configs[tenant_slug] = config_data
    
    return tenant_configs
```

### Expected Tenant Config Structure

```yaml
schema: optimism
dao_slug: Optimism
index_tenant_prefix: op

features:
  oodao: false
  snapshot_proposals: false
  dao_node_proposals: true

deployments:
  main:
    chain_id: 10
    gov:
      address: "0xcDF27F107725988f2261Ce2256bDfCdE8B382B10"
    token:
      address: "0x4200000000000000000000000000000000000042"
    oodao:
      address: "0x..."
      chain_id: 1
```

### Feature Flags

Each tenant can enable/disable data sources:

| Feature | Description |
|---------|-------------|
| `oodao` | Enable EAS-OoDAO sync |
| `snapshot_proposals` | Enable Snapshot sync |
| `dao_node_proposals` | Enable DaoNode sync |

Special case: Optimism always includes `eas-atlas` source.

---

## Database Integration

### PostgreSQL Client (`cpls/postgres.py`)

Simple async connection pool wrapper:

```python
class PostgreSQLClient:
    def __init__(self, database_url):
        self.database_url = database_url
        self.pool = None

    async def connect(self):
        if not self.pool:
            self.pool = await asyncpg.create_pool(
                self.database_url, 
                min_size=1, 
                max_size=10
            )
        return self.pool
```

### Database Schemas Used

| Schema | Purpose |
|--------|---------|
| `auazure` | EAS attestations, token delegate events |
| `atlas` | Optimism-specific votes and citizens |
| `agora` | Delegate statements and metadata |
| `alltenant` | Cross-tenant offchain proposals |
| `{dao_slug}` | DAO-specific views (votes, proposal_types) |

### Key Queries

**Votes Query:**
```sql
SELECT transaction_hash, block_number, chain_id, voter, support, weight, reason, params
FROM {dao_slug}.votes 
WHERE proposal_id = '{proposal_id}';
```

**Voting Power Snapshot:**
```sql
WITH qry AS (
    SELECT DISTINCT ON (delegate) 
        delegate as addr, 
        new_balance as vp 
    FROM auazure.{prefix}_token_delegate_votes_changed 
    WHERE address = '{token_addr}' 
      AND block_number <= {block_number}
    ORDER BY delegate, block_number DESC
)
SELECT * FROM qry WHERE vp::numeric > 0;
```

**Delegate Metadata:**
```sql
SELECT DISTINCT ON(address) 
    address, discord, twitter as x, warpcast
FROM agora.delegate_statements 
WHERE dao_slug = '{dao_slug}' 
  AND (LENGTH(discord) > 2 OR LENGTH(twitter) > 2 OR LENGTH(warpcast) > 2) 
ORDER BY address, updated_at_ts DESC;
```

---

## External Services

### BlockCache Service (`cpls/blockcache.py`)

A caching layer for blockchain RPC calls.

**Base URL:** `https://blockcache-production.up.railway.app/`

**Endpoints Used:**

| Endpoint | Purpose |
|----------|---------|
| `/exact_blocktime/{chain}/{block}` | Get exact block timestamp |
| `/estimated_blocktime/{chain}/{block}` | Estimate future block timestamp |
| `/last_block_before_timestamp/{chain}/{ts}` | Find block by timestamp |
| `/ens/{chain}/{address}` | Resolve ENS name |
| `/decoded_eas/{chain}/attestation/{uid}` | Decode EAS attestation |
| `/contract_call/{chain}/{address}` | Execute view function |

**Contract Calls:**
```python
# Get quorum for a proposal
await bc.contract_call_encoded(
    chain_id=10,
    contract_address=gov_addr,
    block_number=start_block,
    method_signature='quorum(uint256)',
    values=[int(proposal_id)]
)

# Get votable supply
await bc.votable_supply_at_block(chain_id, gov_addr, block_number)
```

**Retry Configuration:**
- Retries on: `ReadError`, `ConnectError`, `TimeoutException`, `RemoteProtocolError`
- Strategy: Exponential backoff (1s → 10s)
- Max attempts: 3

### Snapshot GraphQL API

**Endpoint:** `https://hub.snapshot.org/graphql`

**Proposals Query:**
```graphql
query {
    proposals(
        where: {space: "ens.eth", flagged: false}
        orderBy: "created"
        orderDirection: asc
        first: 1000
    ) {
        id, author, body, choices, created, end, link,
        network, scores, scores_state, scores_total,
        scores_updated, snapshot, start, state, title, type, votes
    }
}
```

**Votes Query:**
```graphql
{
    votes(
        where: {space: "ens.eth", proposal: "{proposal_id}"}
        orderBy: "created"
        orderDirection: asc
        skip: {offset}
        first: 1000
    ) {
        id, voter, created, choice, reason, app, vp, vp_by_strategy, vp_state
    }
}
```

### DaoNode API

**Base URL:** `https://{dao}.prod.agoradata.xyz/v1/`

**Endpoints:**
- `/progress` - Current indexing progress
- `/proposals` - List all proposals
- `/proposal/{id}` - Proposal details
- `/proposal_types` - Available proposal types

---

## API Endpoints

### POST `/jobs`

Submit a new job to the queue.

**Request:**
```json
{
    "type": "external",
    "payload": {
        "infra_dao_slug": "optimism",
        "logic": "refresh_list",
        "sources": ["dao_node"],
        "config": {...}
    }
}
```

**Response:**
```json
{
    "job_id": "uuid-here",
    "status": "queued"
}
```

### GET `/jobs/{job_id}`

Get job details.

**Response:**
```json
{
    "id": "uuid",
    "type": "scheduled",
    "status": "completed",
    "created_at": "2024-01-15T10:30:00",
    "started_at": "2024-01-15T10:30:01",
    "completed_at": "2024-01-15T10:35:00",
    "error": null
}
```

### GET `/`

HTML dashboard showing all jobs with filtering by DAO.

**Features:**
- Job status color coding
- DAO filter dropdown
- Copy job ID to clipboard
- Local timezone conversion
- Stats display (refreshed/skipped counts)

### GET `/health`

Health check endpoint.

**Response:**
```json
{
    "status": "healthy",
    "queue_size": 0,
    "total_jobs": 150,
    "current_job": null
}
```

---

## Data Schema

### Common Fields (All Sources)

| Field | Type | Description |
|-------|------|-------------|
| `id` | string | Unique proposal identifier |
| `title` | string | Proposal title (extracted from description) |
| `proposer` | string | Ethereum address of proposer |
| `proposer_ens` | string/null | ENS name of proposer |
| `start_block` | int | Voting start block |
| `start_blocktime` | int | Voting start timestamp |
| `end_block` | int | Voting end block |
| `end_blocktime` | int | Voting end timestamp |
| `data_eng_properties.liveness` | string | "live" or "archived" |
| `data_eng_properties.source` | string | Source identifier |
| `data_eng_properties.hash` | string | Content hash |

### DaoNode-Specific Fields

| Field | Type | Description |
|-------|------|-------------|
| `block_number` | string | Creation block |
| `targets` | list[str] | Contract addresses to call |
| `calldatas` | list[str] | Encoded function calls |
| `signatures` | list[str] | Function signatures |
| `values` | list[int] | ETH values to send |
| `voting_module` | string | Voting module address |
| `voting_module_name` | string | "approval", "optimistic", etc. |
| `quorum` | string | Required quorum (wei) |
| `lifecycle_stage` | string | Current stage |
| `queue_event` | object | Queue transaction details |
| `execute_event` | object | Execution transaction details |
| `cancel_event` | object | Cancellation details |
| `totals` | object | Vote totals by option |

### EAS-Atlas-Specific Fields

| Field | Type | Description |
|-------|------|-------------|
| `attester` | string | Attestation creator |
| `schema` | string | EAS schema UID |
| `proposal_type_id` | int | Proposal type identifier |
| `choices` | list[str] | Voting options |
| `criteria` | int | Approval criteria |
| `outcome` | object | Vote tallies by citizen type |
| `hybrid` | bool | Linked to on-chain proposal |
| `onchain_proposalid` | int | Linked governor proposal ID |

### EAS-OoDAO-Specific Fields

| Field | Type | Description |
|-------|------|-------------|
| `dao_id` | string | OoDAO identifier |
| `tags` | list[str] | Proposal tags |
| `transaction_hash` | string | Creation transaction |
| `proposal_type` | object | Type configuration |
| `proposal_type_approval` | string | "APPROVED", "PENDING", "ERROR" |
| `outcome.token-holders` | object | Vote tallies |
| `quorum_check` | bool | Passed quorum threshold |
| `approval_check` | bool | Passed approval threshold |

### Snapshot-Specific Fields

| Field | Type | Description |
|-------|------|-------------|
| `type` | string | Voting type (single-choice, copeland, etc.) |
| `choices` | list[str] | Voting options |
| `scores` | list[float] | Votes per choice |
| `scores_total` | float | Total votes cast |
| `scores_state` | string | Scoring status |
| `state` | string | "active" or "closed" |
| `snapshot` | string | Snapshot block number |

---

## Deployment

### Docker

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Clone tenant configs from private repo
ARG GITHUB_TOKEN
RUN git clone https://${GITHUB_TOKEN}@github.com/voteagora/tenants.git /config

COPY cpls/ ./cpls/

EXPOSE 8001

ENV PYTHONUNBUFFERED=1 \
    SERVER_HOST=0.0.0.0 \
    SERVER_PORT=8001

CMD ["python", "-m", "uvicorn", "cpls.server:app", "--host", "0.0.0.0", "--port", "8001"]
```

### Build & Run

```bash
# Build
docker build --build-arg GITHUB_TOKEN=your_token -t cpls .

# Run
docker run -p 8001:8001 \
    -e GCS_BUCKET_NAME=your-bucket \
    -e DATABASE_URL=postgresql://... \
    -e ALCHEMY_API_KEY=your_key \
    -e GOOGLE_CREDENTIALS='{"type":"service_account",...}' \
    cpls
```

---

## Configuration Reference

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ENVIRONMENT` | `dev` | Environment mode (dev/prod) |
| `GCS_BUCKET_NAME` | `cpls-usmr-dev-25q3` | GCS bucket name |
| `SERVER_HOST` | `0.0.0.0` | Server bind address |
| `SERVER_PORT` | `8001` | Server port |
| `SCHEDULER_INTERVAL_MINUTES` | `10` | Job scheduling interval |
| `ALCHEMY_API_KEY` | - | Alchemy API key for RPC |
| `BLOCKCACHE_URL` | `https://blockcache-production.up.railway.app/` | BlockCache service URL |
| `DATABASE_URL` | - | PostgreSQL connection string |
| `TENANT_CONFIG_PATH` | `/config/envs/prod` | Path to tenant YAML files |
| `DEPLOYMENT` | `main` | Deployment environment name |
| `INFRA_DAO_SLUGS` | `ens,optimism,cyber,pguild,syndicate` | DAOs to process (comma-separated, or "all") |
| `RESET_PROPOSALS_ON_RESTART` | `true` | Reprocess archived proposals on restart |
| `GOOGLE_CREDENTIALS` | - | JSON service account credentials |

### Feature Behavior by Environment

| Feature | dev | prod |
|---------|-----|------|
| Local file copies | ✓ | ✗ |
| Uncompressed GCS uploads | ✓ | ✗ |
| Live proposal cache | 100s | 300s |
| Archived proposal cache | 120s | 1 year |

---

## Error Handling

### SkipProposal Exception

Used to skip processing without failing the job:

```python
class SkipProposal(Exception):
    def __init__(self, reason, proposal_id):
        self.reason = reason
        self.proposal_id = proposal_id

# Usage
raise SkipProposal("content state is unchanged", proposal_id)
raise SkipProposal("Proposal is in archival state", proposal_id)
```

### Retry Decorators

All external API calls use tenacity for retries:

```python
@retry(
    retry=retry_if_exception_type((
        httpx.ReadError,
        httpx.ConnectError,
        httpx.TimeoutException,
        httpx.RemoteProtocolError
    )),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    stop=stop_after_attempt(3)
)
async def api_call():
    ...
```

### Job Failure Handling

- Full tracebacks are captured and stored in `job.error`
- Failed jobs are still uploaded to GCS
- Worker continues processing other jobs
- DAO lock is released even on failure

---

## Title Processing

The `title_processor.py` module extracts titles from proposal descriptions:

**Extraction Priority:**
1. Markdown headers (`# Title`, `## Title`)
2. Underlined headers (`Title\n=====`)
3. First line of text

**Cleanup:**
- Removes bold markers (`**`)
- Removes italic markers (`__`)
- Normalizes escaped newlines
- Strips whitespace

```python
def get_title_from_proposal_description(description: str) -> str:
    # Returns "Untitled" if no title found
```

---

## Monitoring & Debugging

### Dashboard

Access the web dashboard at `http://localhost:8001/` to:
- View all jobs and their statuses
- Filter by DAO
- See processing statistics
- View error messages

### Logging

The service uses Python's logging module with structured output:
- Worker status updates
- Lock acquisition/release
- Job completion stats
- Error tracebacks

### Health Check

```bash
curl http://localhost:8001/health
```

Returns queue size, total jobs, and current processing job.

---

## Summary

CPLS is a robust, multi-tenant data aggregation service that:

1. **Collects** governance proposals from 4 sources (DaoNode, Snapshot, EAS-Atlas, EAS-OoDAO)
2. **Normalizes** data into a common schema with source-specific extensions
3. **Enriches** proposals with voting power snapshots, delegate metadata, and ENS names
4. **Stores** results in GCS with intelligent caching and change detection
5. **Scales** with 15 concurrent workers and DAO-level locking
6. **Monitors** via web dashboard and health endpoints

The service is designed for high reliability with retry logic, graceful error handling, and comprehensive logging.
