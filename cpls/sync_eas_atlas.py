import time, json

from collections import defaultdict

import httpx

from .gcs import GCSClient
from .sync import Sync, SkipProposal, FIVE_MINUTES_IN_SECONDS

from .title_processor import get_title_from_proposal_description

from .config import ALCHEMY_API_KEY

class EASAtlasSync(Sync):

    SOURCE = 'eas-atlas'

    async def read_votes(self, proposal_id):
        pool = await self.pg.connect()
        async with pool.acquire() as connection:
            rows = await connection.fetch(f"""select voter, support, weight::text, reason, params, citizen_type, voter_metadata->>'name' name, voter_metadata->>'image' image from atlas."VotesWithMeta" where  proposal_id = '{proposal_id}';""")
            rows = [dict(r) for r in rows]
            return rows
    

    async def read_citizens(self):
        qry = """SELECT 
            c."address" as addr, 
            1 as vp, 
            citizen_type,
            voter_metadata_text::json->>'name' name, 
            voter_metadata_text::json->>'image' image
        FROM atlas.citizens_mat c"""

        pool = await self.pg.connect()
        async with pool.acquire() as connection:
            rows = await connection.fetch(qry)
            rows = [dict(r) for r in rows]
            breakpoint()
            return rows

    async def read_proposal_create_attestations(self):
        pool = await self.pg.connect()
        async with pool.acquire() as connection:
            rows = await connection.fetch(f"""select created_attestation_hash from alltenant.offchain_proposals op ;""")
            return [r['created_attestation_hash'] for r in rows]


    async def refresh_list(self, gcs_client: 'GCSClient'):


        ######################################
        # Step 1 - Get a list of recent-ish proposals.  Think either the "full list of any proposal ever" OR "just stuff that may or may not be ready to archive"
        #

        citizens = await self.read_citizens()
        
        known_create_attestations = await self.read_proposal_create_attestations()

        anything_changed = False
        skipped_count = 0
        refreshed_count = 0
        for proposals_uid in known_create_attestations:

            for chain_id in [10, 1]:

                
                proposal_attestation = await self.bc.get_decoded_eas(chain_id, proposals_uid)

                if proposal_attestation is None:
                    print(f"Failed to fetch proposal {proposals_uid}")
                    continue

                if proposal_attestation['attestation']['uid'] == '0x0000000000000000000000000000000000000000000000000000000000000000':
                    print(f"Failed to fetch proposal {proposals_uid}")
                    continue

                proposal = proposal_attestation['attestation']
                proposal['chain_id'] = proposal_attestation['chain_id']
                proposal.update(proposal_attestation['decoded_data'])
                del proposal['data']
                proposal['resolver'] = proposal_attestation['schema']['resolver']

                proposal_type = proposal['proposal_type']

                proposal['id'] = str(proposal['id'])

                proposal_id = proposal['id']

                try:
                    blob, existing_liveness, existing_proposal_hash, existing_num_of_votes  = await self.read_existing_raw_proposal_hash_if_exists(proposal_id, gcs_client)
                except SkipProposal as e:
                    print(e)
                    skipped_count += 1
                    continue

                proposal['title'] = get_title_from_proposal_description(proposal['description'])
                proposal['proposer_ens'] = await self.bc.get_ens(proposal['proposer'])

                votes = await self.read_votes(proposal_id)
                num_of_votes = len(votes)
                                   
                # No new votes have come in, we can re-use the last tally
                reuse_tally = existing_num_of_votes > 0 and (existing_num_of_votes == num_of_votes) and (not self.reset)
                proposal['num_of_votes'] = num_of_votes

                if reuse_tally:
                    existing_proposal_data = await gcs_client.read_dict(blob.name)
                    outcome = existing_proposal_data['outcome']
                else:

                    if proposal_type in ('OPTIMISTIC', 'STANDARD'):

                        outcome = defaultdict(lambda: defaultdict(int))

                        for vote in votes:
                            support = int(vote['support'])
                            outcome[vote['citizen_type']][support] += int(vote['weight'])

                    elif proposal_type == 'APPROVAL': 

                        outcome = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
                    
                        for vote in votes:
                            support = vote['support']
                            options =json.loads(support)
                            for option in options:
                                outcome[vote['ctizen_type']][option][1] += int(vote['weight'])

                    await self.overwrite_votes(votes, proposal_id, gcs_client)

                proposal['outcome'] = outcome
            
                try:
                    proposal_hash = self.check_existing_proposal_hash(proposal, existing_proposal_hash)
                except SkipProposal as e:
                    print(e)
                    skipped_count += 1
                    continue

                set_of_voters = set(row['addr'].lower() for row in votes)
                has_not_voted = [row for row in citizens if row['addr'].lower() not in set_of_voters]
                await self.overwrite_hasnt_voted(has_not_voted, proposal_id, gcs_client)
                
                # This section here, enriches the proposal object, in a way that will only update,
                # if the hash for the proposal's state changes. Downstream consumers can either use it, accepting
                # the caveate, or re-calculate it.

                start_block = proposal['start_block']
                start_blocktime = await self.get_timestamp(chain_id, start_block)
                proposal['start_blocktime'] = start_blocktime

                end_block = start_block + 259200 # TODO - the EAS Attestation needs the end-block timestamp.

                proposal['end_block'] = end_block
                end_blocktime = await self.get_timestamp(chain_id, end_block)
                proposal['end_blocktime'] = end_blocktime

                liveness = 'live'

                cur_time = int(time.time())                

                if cur_time > proposal['end_blocktime'] + FIVE_MINUTES_IN_SECONDS:
                    liveness = 'archived'

                anything_changed = True
                refreshed_count += 1

                await self.overwrite_proposal(proposal, proposal_hash, liveness, gcs_client)

            if anything_changed:
                await self.refresh_source_list(gcs_client)
                await self.refresh_full_list(gcs_client)

        return {
            'skipped': skipped_count,
            'refreshed': refreshed_count
        }

