# Snapshot Synchronization Overview

This document captures the new Snapshot integration that now runs alongside the existing DAO-Node sync inside the CPLS job processor. It explains what was added, how it works, and why specific trade‑offs were chosen.

## Goals

- Mirror the DAO-Node object-storage approach for Snapshot proposals so downstream consumers can read blobs/NDJSON from GCS without touching Postgres.
- Preserve historical proposals even when Snapshot authors delete or edit them (the legacy indexer assumed immutability and silently lost data).
- Keep the implementation simple enough to test locally while leaving room for future webhook/message replay enhancements.

## Environment & Scheduling

- `SNAPSHOT_SPACES` is now parsed as a mapping (`alias=snapshot-space`). A default is provided for the spaces that existed in the legacy pipeline:
  - `ens=ens.eth`
  - `uniswap=uniswapgovernance.eth`
  - `derive=derivexyz.eth`
  - `etherfi=etherfi-dao.eth`
- Each alias becomes the folder name in GCS (matching the prior pattern where `infra_dao_slug` drove paths) and the scheduler job id. The actual Snapshot space id (e.g. `ens.eth`) is still passed to the API client.
- Jobs are queued through the existing FastAPI scheduler; no changes were needed outside of adding a new `snapshot` payload type.

## API Access Layer (`SnapshotAPIClient`)

- A minimal GraphQL client issues paginated `proposals` queries (`first=1000`) with a `flagged: false` guard, reproducing the filtering from the old indexer. The selection includes `votes` and `link` so the raw objects carry vote counts and the proposal URL.
- Individual lookups (`proposal(id: …)`) are available to confirm whether a missing ID was deleted or only temporarily absent.
- Using a dedicated class makes the sync code testable by swapping in fakes.

## SnapshotSync Pipeline

- Code now lives under `cpls/syncs/`:
  - `dao_node.py` retains the legacy DAO-Node sync logic (tidied but behaviourally identical).
  - `snapshot.py` contains `SnapshotAPIClient` plus `SnapshotSync`.
  - `utils.py` holds the shared `json_hash` helper.
  - `__init__.py` re-exports both syncs (`from cpls.syncs import ...`).
- `SnapshotSync` mirrors `DaoNodeSync` but works entirely off Snapshot’s GraphQL API.
- Raw blobs are written gzipped at `data/<alias>/proposal/snapshot/raw/<proposal_id>.json.gz` (the production object name) so this sync updates the same blobs existing consumers read, with metadata (`source`, `liveness`, `hash`, `synced_at`, optional `deleted_at`).
- Each proposal is normalized onto the downstream contract before hashing/writing: `votes → num_of_votes`, `start/end/created → *_blocktime`, `body → description`, and `link → url`. This mirrors the legacy `sync_snapshot.py` enrichment so notification consumers keep working.
- Cache lifetimes are selected via `ENVIRONMENT == 'prod'` (the repo's production value).
- The payload hash is computed from the proposal data + liveness to avoid accidental cache busting when only metadata changes. If nothing changed, the existing blob is left untouched but `synced_at` is refreshed in-memory for bookkeeping.
- Liveness buckets:
  - `live` for `pending`/`active`
  - `archived` for everything else returned by Snapshot (mirrors DAO-Node `execute/cancel` handling)
  - `deleted` when a proposal disappears from the API altogether. Retirement is guarded twice so a transient blip never deletes a live proposal:
    1. **Direct re-fetch gate** — a proposal missing from the list query is only a candidate if `proposal(id: …)` *also* returns null. This is essential because the list query uses `flagged: false`: flagged (spam) proposals vanish from the list but still resolve by id, so they must not be treated as deleted.
    2. **Consecutive-miss threshold** — a confirmed miss records a `missing_count` strike (stored in `data_eng_properties`, no content-hash change); a proposal is only retired after `DELETION_MISS_THRESHOLD` (2) consecutive confirmed misses, and the counter resets the moment it reappears. This absorbs indexer lag / replica inconsistency. API errors during verification are never read as deletions.

    Retirement writes a tombstone blob (`liveness=deleted`, `deleted_at`) instead of removing the file, preserving historical references.
- Summary NDJSON files live alongside proposals in `data/<alias>/proposal_list/snapshot/(live|archived).ndjson`; they strip heavy fields (`body`, `hash`) and remain sorted by `end` descending to stay consistent with existing consumers.

## Testing

- A lightweight test suite (`tests/test_snapshot_sync.py`) uses fake GCS clients and API clients to assert:
  1. Live vs archived segregation and summary generation.
  2. Deletion only after consecutive confirmed misses (tombstone metadata).
  3. Flagged-but-alive proposals are never retired; strikes reset when a proposal reappears.
- Tests are synchronous-friendly (using `asyncio.run`) so they can be executed without additional fixtures.
- `pytest` was added to `requirements.txt` to document the test dependency.

## Why GCS Blobs Instead of Postgres

- The job processor already writes DAO-Node data to GCS; keeping Snapshot in the same format avoids duplicating storage logic and simplifies consumer code (one fetch path for both sources).
- Tombstones (`liveness=deleted`) solve the previous disappearing-proposal bug without needing schema changes or backfills.

## Future Enhancements

- Snapshot webhooks (`proposal/*`) and/or the `messages` replay endpoint can be integrated later for near real-time updates. The current polling architecture keeps that door open by centralising all writes through `SnapshotSync`.
- Vote ingestion is still handled by the legacy pipeline. Once votes move over, they should read the new blobs (or reuse the API client) to stay consistent with mutable proposals.
