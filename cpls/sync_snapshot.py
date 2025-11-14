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

from typing import List

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

        for i, proposal in enumerate(proposals):
        
            proposal_id = proposal['id']

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

            if proposal['state'] == 'closed':
                liveness = 'archived'
            else:
                raise Exception("Proposal state is not closed, this is a bug.")

            await self.overwrite_proposal(proposal, proposal_hash, liveness, gcs_client)

        if anything_changed or self.reset:
            await self.refresh_source_list(gcs_client)
            await self.refresh_full_list(gcs_client)

        print (f"Refreshed {refreshed_count} proposals, skipped {skipped_count}")
        return {
            'skipped': skipped_count,
            'refreshed': refreshed_count
        }




