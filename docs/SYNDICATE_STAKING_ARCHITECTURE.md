# Syndicate L3 Collective Staking Integration Architecture

## Overview

This document describes the architecture for integrating L3 Collective staking into the Syndicate governance system. The integration allows users who have staked tokens on the L3 Collective chain to participate in governance with their staked balances, in addition to their delegated tokens on L1.

### Key Features

- **Dual Voting Power**: Combines both L1 delegation and L3 staking balances
- **Cross-Chain Integration**: Synchronizes vote power across L1 and L3 (L3 Collective)
- **Point-in-Time Snapshots**: Captures voting power at proposal creation time
- **Votable Supply Enhancement**: Includes staking balances in total votable supply calculations
- **Transparent Aggregation**: Delegates can have delegation-only, staking-only, or both types of voting power

## Data Flow

### Proposal Creation and VP Snapshot Flow

```mermaid
sequenceDiagram
    participant UI as agora-next UI
    participant L1 as L1 Chain (EAS)
    participant CPLS as CPLS Sync Job
    participant PG as PostgreSQL
    participant BC as BlockCache
    participant DaoNode as dao-node API
    participant L3 as L3 Collective
    participant GCS as GCS Bucket

    Note over UI,L1: 1. Proposal Creation
    UI->>L1: Create proposal attestation
    L1-->>UI: Proposal created at block N

    Note over CPLS,GCS: 2. CPLS Sync Cycle
    CPLS->>PG: Query new proposals
    PG-->>CPLS: Proposal metadata (block N, timestamps)

    Note over CPLS,L3: 3. L1 → L3 Block Conversion
    CPLS->>BC: Get timestamp for L1 block N
    BC-->>CPLS: Timestamp T
    CPLS->>BC: Get L3 block at timestamp T
    BC->>L3: Query block by timestamp
    L3-->>BC: L3 block M
    BC-->>CPLS: L3 block M

    Note over CPLS,GCS: 4. Delegation VP from DB
    CPLS->>PG: Query delegation balances at block N
    PG-->>CPLS: Delegation VP dict {addr: vp}

    Note over CPLS,L3: 5. Staking VP from dao-node
    CPLS->>DaoNode: GET /v1/staking/all-stakes/at-block/M
    DaoNode-->>CPLS: Staking VP dict {addr: amount}

    Note over CPLS,GCS: 6. Merge VP Sources
    CPLS->>CPLS: Merge delegation + staking VP
    CPLS->>GCS: Cache VP snapshot
    GCS-->>CPLS: Snapshot saved

    Note over CPLS,GCS: 7. Calculate Total Votable Supply
    CPLS->>PG: Get DB votable supply
    PG-->>CPLS: Delegation votable supply
    CPLS->>DaoNode: GET /v1/staking/total/at-block/M
    DaoNode-->>CPLS: Total staking amount
    CPLS->>CPLS: Sum both sources
    CPLS->>GCS: Save proposal with total VP
```

### Vote Casting and Outcome Calculation Flow

```mermaid
sequenceDiagram
    participant Voter as Voter Address
    participant L1 as L1 Chain (EAS)
    participant CPLS as CPLS Sync Job
    participant PG as PostgreSQL
    participant GCS as GCS Bucket

    Note over Voter,L1: 1. Vote Cast on L1
    Voter->>L1: Cast vote attestation
    L1-->>Voter: Vote recorded

    Note over CPLS,PG: 2. Vote Indexing
    CPLS->>PG: Query votes for proposal
    PG-->>CPLS: Raw votes with L1 weights

    Note over CPLS,GCS: 3. Initial Vote Processing
    loop For each raw vote
        CPLS->>CPLS: Add voter metadata (ENS, socials)
        CPLS->>CPLS: Calculate initial outcome with L1 weights
    end
    CPLS->>GCS: Overwrite votes (initial)

    Note over CPLS,GCS: 4. VP Lookup
    CPLS->>GCS: Read cached merged VP (from proposal creation)
    GCS-->>CPLS: VP dict {addr: delegation+staking}

    Note over CPLS,CPLS: 5. Vote Weight Correction (SAME JOB)
    loop For each vote
        CPLS->>CPLS: Lookup voter's merged VP
        CPLS->>CPLS: Replace vote weight with merged VP
        CPLS->>CPLS: Recalculate outcome with merged VP
    end

    Note over CPLS,GCS: 6. Save Data
    CPLS->>GCS: Overwrite votes (merged weights)
    CPLS->>CPLS: Update proposal outcome
    CPLS->>GCS: Save updated proposal
```

**Key Timing Details**:

- **No delay between correction and vote**: Both happen in the same atomic job execution
- **VP Snapshot is cached**: Created once when proposal starts, reused for all vote corrections

### Complete Lifecycle Timeline

```mermaid
sequenceDiagram
    participant User as User
    participant L1 as L1 Chain (EAS)
    participant PG as PostgreSQL
    participant CPLS as CPLS Sync Job
    participant BC as BlockCache
    participant DaoNode as dao-node API
    participant L3 as L3 Collective
    participant GCS as GCS Bucket

    Note over User,L1: Proposal Creation Phase
    User->>L1: Create proposal attestation
    L1-->>User: Proposal created at block N

    Note over CPLS: Wait for sync cycle
    CPLS->>PG: Query new proposals
    PG-->>CPLS: Proposal metadata (block N)

    Note over CPLS,L3: VP Snapshot (First Sync)
    CPLS->>BC: Get timestamp for L1 block N
    BC-->>CPLS: Timestamp T
    CPLS->>BC: Get L3 block at timestamp T
    BC->>L3: Query block by timestamp
    L3-->>BC: L3 block M
    BC-->>CPLS: L3 block M

    CPLS->>PG: Fetch delegation VP from DB at block N
    PG-->>CPLS: Delegation VP dict

    CPLS->>DaoNode: Fetch staking VP at block M
    DaoNode-->>CPLS: Staking VP dict

    CPLS->>CPLS: Merge VP sources (delegation + staking)
    CPLS->>GCS: Cache VP snapshot

    CPLS->>PG: Get DB votable supply
    PG-->>CPLS: Delegation votable supply
    CPLS->>DaoNode: Get total staking amount at block M
    DaoNode-->>CPLS: Total staking amount
    CPLS->>CPLS: Calculate total votable supply
    CPLS->>GCS: Save proposal with total VP

    Note over User,L1: Vote Cast Phase
    User->>L1: Cast vote attestation
    L1-->>User: Vote recorded
    L1->>PG: Vote indexed to PostgreSQL

    Note over CPLS: Wait for next sync cycle
    CPLS->>PG: Query votes for proposal
    PG-->>CPLS: Raw votes with L1 weights

    Note over CPLS,GCS: Vote Processing (Same Job)
    loop For each vote
        CPLS->>CPLS: Enrich vote with metadata
    end

    CPLS->>GCS: Read cached VP snapshot
    GCS-->>CPLS: Merged VP dict

    loop For each vote
        CPLS->>CPLS: Lookup voter's merged VP
        CPLS->>CPLS: Correct vote weight with merged VP
        CPLS->>CPLS: Recalculate outcome
    end

    CPLS->>GCS: Save votes
    CPLS->>CPLS: Generate hasn't voted list
    CPLS->>GCS: Save hasn't voted list
    CPLS->>GCS: Save updated proposal
```

**Key Observations**:

1. **VP Snapshot Created Once**: When proposal starts, the VP snapshot (delegation + staking) is calculated and cached. This same snapshot is reused for all votes.

2. **Correction is Atomic**: The vote weight correction happens in the same job as vote ingestion. There's never a window where users see incorrect vote weights (if they do, it's only during job processing).
