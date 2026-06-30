import copy
import httpx, time
import asyncio
import logging
from .gcs import GCSClient
from .sync import Sync, SkipProposal, FIVE_MINUTES_IN_SECONDS

from .title_processor import get_title_from_proposal_description

from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
    after_log
)

from typing import List, Optional

# A live proposal must be confirmed absent (missing from the space list AND null on
# a direct id lookup) on this many consecutive syncs before it is retired, so a
# transient API blip or a flagged proposal can't delete a live one.
SNAPSHOT_DELETION_MISS_THRESHOLD = 2

# Configure retry decorator for Snapshot GraphQL operations
snapshot_retry = retry(
    retry=retry_if_exception_type((
        httpx.ReadError,
        httpx.ConnectError,
        httpx.TimeoutException,
        httpx.RemoteProtocolError
    )),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    stop=stop_after_attempt(3),
    before_sleep=before_sleep_log(logging.getLogger("cpls.snapshot"), logging.WARNING),
    after=after_log(logging.getLogger("cpls.snapshot"), logging.DEBUG)
)

class SnapshotGraphQLClient:

    def __init__(self, tenant, http_client: httpx.AsyncClient = None):
        self.url = "https://hub.snapshot.org/graphql"
        self.tenant = tenant
        self.space = {'ens' : 'ens.eth',
                      'uniswap' : 'uniswapgovernance.eth',
                      'derive' : 'derivexyz.eth',
                      'etherfi' : 'etherfi-dao.eth'}[tenant]

        self.page_size = 1000
        self.client = http_client if http_client is not None else httpx.AsyncClient()

    @snapshot_retry
    async def get_votes(self, proposal_id, page) -> List:

        skip = page * self.page_size
        QUERY = """
                {
                items: votes(where: {space : "%s", proposal : "%s"}, orderBy: "created", orderDirection: asc, skip: %s, first: %s) {
                    id
                    voter
                    created
                    choice
                    reason
                    app
                    vp
                    vp_by_strategy
                    vp_state
                }
                }
                """ % (self.space, proposal_id, skip, self.page_size)

        resp = await self.client.post(self.url, json={'query': QUERY})
        payload = resp.json()['data']['items']

        return payload
    
    async def get_all_votes(self, proposal_id) -> List:

        votes = []
        page = 0
        while page <= 5: # For some reason it errors after 5.
            page_votes = await self.get_votes(proposal_id, page)

            if page_votes is None:
                break
        
            votes.extend(page_votes)
    
            if len(page_votes) < self.page_size:
                break
            page += 1
        return votes

    @snapshot_retry
    async def get_proposals(self) -> List:

        QUERY = """
                    query {
                        items: proposals(
                        where: {space: "%s", flagged: false}
                        orderBy: "created"
                        orderDirection: asc,
                        first: 1000
                        ) {
                        id
                        author
                        body
                        choices
                        created
                        end
                        link
                        network
                        scores
                        scores_state
                        scores_total
                        scores_updated
                        snapshot
                        start
                        state
                        title
                        type
                        votes
                        }
                    }
                    """ % self.space

        resp = await self.client.post(self.url, json={'query': QUERY})
        payload = resp.json()['data']['items']

        return payload

    @snapshot_retry
    async def get_proposal(self, proposal_id) -> Optional[dict]:
        # Direct lookup used to confirm a disappearance. Snapshot returns null once
        # a proposal is deleted, but still returns flagged ones, so this distinguishes
        # "deleted" from "merely dropped from the flagged:false list".
        QUERY = """
                query {
                    item: proposal(id: "%s") {
                        id
                        state
                    }
                }
                """ % proposal_id

        resp = await self.client.post(self.url, json={'query': QUERY})
        resp.raise_for_status()
        body = resp.json()
        # A GraphQL error payload (HTTP 200, errors[], possibly null data) is NOT a
        # deletion. Raise so the caller skips verification instead of counting a miss;
        # only an explicit null `item` with no errors means the proposal is gone.
        if body.get('errors'):
            raise RuntimeError(f"Snapshot API error: {body['errors']}")
        data = body.get('data')
        if data is None:
            raise RuntimeError("Snapshot API returned no data")
        return data.get('item')



class SnapshotSync(Sync):

    SOURCE = 'snapshot'

    def __init__(self, infra_dao_slug, config=None, reset=False, http_client=None):

        super().__init__(infra_dao_slug, config, reset, http_client)

        self.index_tenant_prefix = self.config['index_tenant_prefix']
        self.token_addr = self.config['deployment']['token']['address']
        self.dao_slug = self.config['dao_slug']

        self.sc = SnapshotGraphQLClient(self.infra_dao_slug, http_client)

    def govless_proposal_blob_name(self, proposal_id):
        return f"data/{self.infra_dao_slug}/proposal/{self.SOURCE}/raw/{proposal_id}.json.gz"
    

    async def refresh_list(self, gcs_client: 'GCSClient'):

        self.bc.clear_lru()
        self.delegate_metadata = None

        proposals = await self.sc.get_proposals()

        anything_changed = False
        skipped_count = 0
        refreshed_count = 0
        seen_ids = set()

        for i, proposal in enumerate(proposals):

            proposal_id = proposal['id']
            seen_ids.add(proposal_id)

            try:
                blob, existing_liveness, existing_proposal_hash, existing_num_of_votes  = await self.read_existing_raw_proposal_hash_if_exists(proposal_id, gcs_client)
            except SkipProposal as e:
                print(e)
                skipped_count += 1
                continue

            if existing_liveness == 'archived' and not self.reset:
                continue
        
            proposal['description'] = proposal['body']
            del proposal['body']

            curtime = int(time.time())
            # These are needed for cache busting.
            proposal['after_start_time'] = proposal['start'] > curtime
            proposal['after_end_time'] = proposal['end'] > curtime

            try:
                proposal_hash = self.check_existing_proposal_hash(proposal, existing_proposal_hash)
            except SkipProposal as e:
                print(e)
                skipped_count += 1
                continue

            num_of_votes = proposal['votes']
            proposal['num_of_votes'] = num_of_votes
            del proposal['votes']

            # No new votes have come in, we can re-use the last tally
            reuse_tally = existing_num_of_votes > 0 and (existing_num_of_votes == num_of_votes) and (not self.reset)

            liveness = 'live'

            anything_changed = True
            refreshed_count += 1

            if reuse_tally:
                pass
            elif proposal['type'] == 'copeland':

                vp_snapshot = await self.get_vp_snapshot_all_delegates(block_number=proposal['snapshot'], 
                                                                 gcs_client=gcs_client, 
                                                                 reset=self.reset)
                
                vp_snapshot = {s['addr'].lower(): s['vp'] for s in vp_snapshot}

                votes = await self.sc.get_all_votes(proposal_id)

                if self.delegate_metadata is None:
                    self.delegate_metadata = await self.get_delegate_metadata()
                
                votes_out = []
                for vote in votes:
                    addr = vote['voter'].lower()
                    delegate_meta = self.delegate_metadata.get(addr, {})
                    vote.update(delegate_meta)

                    del vp_snapshot[addr]

                    votes_out.append(vote)

                await self.overwrite_votes(votes, proposal_id, gcs_client)

                hasnt_voted = []
                for non_voter, vp in vp_snapshot.items():
                    row = {'addr' : non_voter,
                           'vp': vp}
                    delegate_meta = self.delegate_metadata.get(non_voter, {})
                    row.update(delegate_meta)
                    hasnt_voted.append(row)

                await self.overwrite_hasnt_voted(hasnt_voted, proposal_id, gcs_client)


            # This section here, enriches the proposal object, in a way that will only update,
            # if the hash for the proposal's state changes. Downstream consumers can either use it, accepting
            # the caveate, or re-calculate it.

            proposal['end_blocktime'] = proposal['end']
            proposal['start_blocktime'] = proposal['start']
            proposal['created_blocktime'] = proposal['created']
            if proposal['state'] == 'closed':
                liveness = 'archived'
            elif proposal['state'] == 'active':
                assert liveness == 'live', "Proposal state is active, but liveness is not live, this is a bug."
            else:
                raise Exception("Proposal state is not closed, this is a bug.")

            await self.overwrite_proposal(proposal, proposal_hash, liveness, gcs_client)

        if await self._reconcile_deleted_proposals(seen_ids, gcs_client):
            anything_changed = True

        if anything_changed or self.reset:
            await self.refresh_source_list(gcs_client)
            await self.refresh_full_list(gcs_client)

        print (f"Refreshed {refreshed_count} proposals, skipped {skipped_count}")
        return {
            'skipped': skipped_count,
            'refreshed': refreshed_count
        }

    async def _reconcile_deleted_proposals(self, seen_ids, gcs_client: 'GCSClient') -> bool:
        """Retire live proposals that vanished from Snapshot (edited/deleted).

        A proposal is retired only after it is confirmed absent from both the space
        list AND a direct id lookup, on SNAPSHOT_DELETION_MISS_THRESHOLD consecutive
        syncs. Flagged proposals (dropped from the flagged:false list but still
        resolvable by id) and transient drops never get retired; API errors during
        verification are never read as deletions.
        """
        prefix = f"data/{self.infra_dao_slug}/proposal/{self.SOURCE}/raw/"
        blobs = await gcs_client.list_blobs(prefix=prefix)

        changed = False
        for blob in blobs:
            if not blob.name.endswith('.json.gz'):
                continue

            data = await gcs_client.read_dict(blob.name)
            if not data:
                continue

            props = data.get('data_eng_properties', {})
            # Only live proposals can transition to deleted; archived/deleted are terminal.
            if props.get('liveness') != 'live':
                continue

            proposal_id = data.get('id')
            if proposal_id is None:
                continue

            prior_strikes = props.get('missing_count', 0)

            present = proposal_id in seen_ids
            if not present:
                try:
                    present = await self.sc.get_proposal(proposal_id) is not None
                except Exception as e:
                    print(f"snapshot: could not verify {proposal_id}, skipping: {e}")
                    continue

            if present:
                if prior_strikes:
                    await self._write_snapshot_record(data, 'live', gcs_client, missing_count=0)
                    changed = True
                continue

            strikes = prior_strikes + 1
            if strikes < SNAPSHOT_DELETION_MISS_THRESHOLD:
                await self._write_snapshot_record(data, 'live', gcs_client, missing_count=strikes)
            else:
                await self._write_snapshot_record(
                    data, 'deleted', gcs_client, missing_count=strikes, deleted_at=int(time.time())
                )
            changed = True

        return changed

    async def _write_snapshot_record(
        self, proposal, liveness, gcs_client: 'GCSClient', *, missing_count=0, deleted_at=None
    ):
        """Rewrite a proposal blob with updated liveness / strike metadata, preserving
        the existing content hash so a strike or tombstone is not seen as a content
        change by hash-based consumers (only liveness flips)."""
        props = proposal.setdefault('data_eng_properties', {})
        proposal_hash = props.get('hash')

        props['liveness'] = liveness
        props['source'] = self.SOURCE
        props['hash'] = proposal_hash
        if missing_count:
            props['missing_count'] = missing_count
        else:
            props.pop('missing_count', None)
        if deleted_at is not None:
            props['deleted_at'] = deleted_at

        metadata = {
            'proposal_id': proposal['id'],
            'liveness': liveness,
            'source': self.SOURCE,
            'hash': proposal_hash,
            'num_of_votes': proposal.get('num_of_votes', 0),
        }

        cache_contr = self.calc_cache_control(liveness)
        await gcs_client.upload_dict(
            proposal, self.proposal_blob_name(proposal['id']), metadata=metadata, cache_control=cache_contr
        )




