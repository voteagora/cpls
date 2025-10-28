import json, time, copy
from collections import defaultdict

from .sync import Sync, SkipProposal, FIVE_MINUTES_IN_SECONDS, to_eth_address
from .gcs import GCSClient

OODAO = {
   'INSTANTIATE' : '0x6bd280a85f895b15798d2b8e524a651e034f42b3ce614cb394c9d5e2ae2b10c7',
   'PERMA_INSTANTIATE' : '0x01a6e34a0b986043b892902536c69a24f37fcfdaea39fbd2216dedbc6d53d83f',
   'GRANT' : '0x7e4752a595f69560a5759e1acc1c70995758b45f00972374de9e1b801d19b758',
   'CREATE_PROPOSAL_TYPE' : '0x880613f78650605d0a0617ec006b8181de80f688a12da6e78f1a8bf3d6b4f922',
   'CREATE_PROPOSAL' : '0x442d586d8424b5485de1ff46cb235dcb96b41d19834926bbad1cd157fbeeb8fc',
   'CHECK_PROPOSAL' : '0xd0fa030b9d06e954b910a86eeffc02aa641eaeef4216f9402ab4503f44c8e6a8',
   'SET_PROPOSAL_TYPE' : '0x4468df37e17deb20b5096fb12107d4841b79ff6a62292e798eb7b79d0e764eb5',
   'SET_PARAM_VALUE' : '0x7ac5f4a1c2c47e910132a546af770c1a6ff0c02e931020de6f955cf42eae9a6b',
   'DELEGATED_SIMPLE_VOTE' : '0xde80f2c4e6168c2f68c1b466087ffba7994c2b7ff8f4113689c75ee82ef59c61',
   'DELEGATED_ADVANCED_VOTE' : '0x4aa210b34a3b488c54f7ec482763c5ec8a52be5669c24216d3814b009076fb50',
   'SIMPLE_VOTE' : '0x2b0e624e00310c7e88a1b7840238e285152b38ab00160b14c0d4e54e0a53a3aa',
   'ADVANCED_VOTE' : '0xa7497737b4bdc0eaf60e90a290602216fb2a0e8c886e50bad63324ca8b76a587',
   'DELETE' : '0x42793d748cbdd8d815556d7bbeacae5e82cbce605ea048474e7e60a02577cf49',
}

class EASOoDaoSync(Sync):

    SOURCE = 'eas-oodao'

    async def read_votes_from_db(self, proposal_id, dao_slug):
        pool = await self.pg.connect()
        async with pool.acquire() as connection:
            qry = f"""select support, weight from {dao_slug}.votes where proposal_id = '{proposal_id}';"""
            rows = await connection.fetch(qry)
            return rows

    async def read_votes_direct_from_eas(self, proposal_id):
        pool = await self.pg.connect()
        async with pool.acquire() as connection:
            qry = f"""select * from auazure."eas_attestations_v2" ocv WHERE topic3 in ('{OODAO['SIMPLE_VOTE']}', '{OODAO['ADVANCED_VOTE']}') and decoded_attestation->'proposal_id' = '{proposal_id}';"""
            print(qry)
            rows = await connection.fetch(qry)
            return rows

    async def read_proposal_type_range(self, dao_slug):

        pool = await self.pg.connect()
        async with pool.acquire() as connection:
            row = await connection.fetchrow(f"""select min(quorum::numeric)::text min_quorum_pct, max(quorum::numeric)::text max_quorum_pct, min(approval_threshold::numeric)::text min_approval_threshold_pct, max(approval_threshold::numeric)::text max_approval_threshold_pct from {dao_slug}.proposal_types;""")
            return row
        
    async def read_snapshot_votable_supply(self, block_number, dao_slug):

        token = '0x55f6e82a8bf5736d46837246dcbeaf7e61b3c27c'
         
        pool = await self.pg.connect()
        async with pool.acquire() as connection:
            row = await connection.fetchrow(f"""select {dao_slug}.get_votable_supply_at_block({block_number}, '{token}') as votable_supply;""")
            return int(row['votable_supply'])

    async def read_snapshot_voting_power(self, delegate, block_number, dao_slug):

        token = '0x55f6e82a8bf5736d46837246dcbeaf7e61b3c27c'
         
        pool = await self.pg.connect()
        async with pool.acquire() as connection:
            row = await connection.fetchrow(f"""select {dao_slug}.get_voting_power_at_block('{delegate}', {block_number}, '{token}') as voting_power;""")
            return int(row['voting_power'])
           
    async def read_proposal_type(self, proposal_id):

        # KEEPING THIS LOGIC SEPERATE AND SYNC for now, to make it easier to debug and change.

        qry = f"""SELECT 
                    decoded_attestation
                FROM 
                    auazure.eas_attestations_v2 ea2
                WHERE 
                    data = (
                        SELECT 
                            ref_uid
                        FROM 
                            auazure.eas_attestations_v2 eav
                        WHERE 
                            data = '{proposal_id}'
                        ORDER BY attestation_time desc limit 1
                        );"""

        pool = await self.pg.connect()
        async with pool.acquire() as connection:
            row = await connection.fetchrow(qry)

            if row is None:
                authors_prop_type = None
            else:
                authors_prop_type = json.loads(row['decoded_attestation'])   

        qry = f"""SELECT 
                    decoded_attestation
                FROM 
                    auazure.eas_attestations_v2 ea2
                WHERE 
                    data = (
                        SELECT 
                            ref_uid
                        FROM 
                            auazure.eas_attestations_v2 eav
                        WHERE 
                            topic3 = '{OODAO['SET_PROPOSAL_TYPE']}'
                            AND decoded_attestation->>'proposal_id' = '{proposal_id}'
                        ORDER BY attestation_time desc limit 1
                        );"""

        pool = await self.pg.connect()
        async with pool.acquire() as connection:
            row = await connection.fetchrow(qry)

            if row is None:
                approved_prop_type = None
            else:
                approved_prop_type = json.loads(row['decoded_attestation'])

        return authors_prop_type, approved_prop_type
    
    async def read_proposals(self):

        # TODO - Filter on DAO-ID

        pool = await self.pg.connect()
        async with pool.acquire() as connection:
            rows = await connection.fetch(f"""select 
                                                transaction_hash,
                                                topic1_cropped as dao_id,
                                                data as uid,
                                                topic2_cropped as author,
                                                chain_id,
                                                decoded_attestation->>'tags' as tags,
                                                decoded_attestation->'endts' as endts,
                                                decoded_attestation->>'title' as title,
                                                decoded_attestation->'startts' as startts,
                                                decoded_attestation->>'description' as description,
                                                data as proposal_id
                                                from auazure."eas_attestations_v2" ocp WHERE topic3 = '{OODAO['CREATE_PROPOSAL']}';""")
            return rows

    async def read_proposal_deletions(self):

        # TODO - Filter on DAO-ID

        pool = await self.pg.connect()
        async with pool.acquire() as connection:
            rows = await connection.fetch(f"""select 
                                                transaction_hash,
                                                topic1_cropped as dao_id,
                                                data as uid,
                                                topic2_cropped as deleter,
                                                chain_id,
                                                ref_uid,
                                                attestation_time
                                                from auazure."eas_attestations_v2" ocp WHERE decoded_attestation->>'verb' = 'CREATE_PROPOSAL';""")
            return rows

    async def refresh_list(self, gcs_client: 'GCSClient'):


        ######################################
        # Step 1 - Get a list of recent-ish proposals.  Think either the "full list of any proposal ever" OR "just stuff that may or may not be ready to archive"
        #

        proposals = await self.read_proposals()
        deletions = await self.read_proposal_deletions()

        deletions = {row['ref_uid'] : dict(row) for row in deletions}

        default_type_ranges = await self.read_proposal_type_range(self.infra_dao_slug)
        default_type_ranges = {k : int(v) for k, v in default_type_ranges.items() if v is not None}

        anything_changed = False
        skipped_count = 0
        refreshed_count = 0

        for proposal_meta in proposals:

            proposal = dict(proposal_meta)

            proposal_id = proposal_meta['proposal_id']

            if proposal['uid'] in deletions:
                proposal['delete_event'] = deletions[proposal['uid']]

            proposal['id'] = proposal_id

            authors_prop_type, approved_prop_type = await self.read_proposal_type(proposal_id)

            if approved_prop_type:
                proposal['proposal_type'] = approved_prop_type
                proposal['proposal_type_approval'] = 'APPROVED'
            elif authors_prop_type:
                proposal['proposal_type'] = authors_prop_type
                proposal['proposal_type_approval'] = 'PENDING'
                proposal['default_proposal_type_ranges'] = default_type_ranges
            else: # Backwards compatibility only, will delete later this week.
                proposal['proposal_type'] = {"name": "Unset Proposal Type", "class": "STANDARD", "quorum": 10000, "description": "This Proposal Type is Unset", "approval_threshold": 10000}
                proposal['proposal_type_approval'] = 'ERROR'
                proposal['default_proposal_type_ranges'] = default_type_ranges

            proposal_type_name = proposal['proposal_type'].get('class', 'STANDARD')
            
            proposal['proposer'] = to_eth_address(proposal_meta['author'])
            proposal['proposer_ens'] = await self.bc.get_ens(proposal['proposer'])
            del proposal['author']            

            try:
                blob, existing_liveness, existing_proposal_hash, existing_num_of_votes = await self.read_existing_raw_proposal_hash_if_exists(proposal_id, gcs_client)
            except SkipProposal as e:
                print(e)
                skipped_count += 1
                continue

            votes = await self.read_votes_from_db(proposal_id, self.infra_dao_slug)
            num_of_votes = len(votes)

            # No new votes have come in, we can re-use the last tally
            reuse_tally = existing_num_of_votes > 0 and (existing_num_of_votes == num_of_votes)

            proposal['num_of_votes'] = num_of_votes

            if reuse_tally:
                existing_proposal_data = await gcs_client.read_dict(blob.name)
                outcome = existing_proposal_data['outcome']
            
            else:
                if proposal_type_name in ('UNSET', 'OPTIMISTIC', 'STANDARD'):

                    outcome = defaultdict(lambda: defaultdict(int))

                    for vote in votes:

                        outcome['token-holders'][vote['support']] += int(vote['weight']) 

                        # vote_att = json.loads(vote['decoded_attestation']) #vote['decoded_attestation']
                        # print(vote)
                        # choice = vote_att['choice']
                        # outcome['token-holders'][choice] += await self.read_snapshot_voting_power(vote['voter'], vote['block_number'], self.infra_dao_slug)
                
                    for key in outcome['token-holders'].keys():
                        outcome['token-holders'][key] = str(outcome['token-holders'][key])

                elif proposal_type_name == 'APPROVAL': 
                    raise NotImplementedError("Approval Types are Not implemented yet.")
                else:
                    raise NotImplementedError(f"Proposal Type {proposal_type_name} is not implemented yet.")

            proposal['outcome'] = outcome
            proposal['tags'] = proposal['tags'].split(',')


        
            try:
                proposal_hash = self.check_existing_proposal_hash(proposal, existing_proposal_hash)
            except SkipProposal as e:
                print(e)
                skipped_count += 1
                continue

            startts = int(proposal_meta['startts'])
            endts = int(proposal_meta['endts'])

            curts = int(time.time())

            if curts >= startts:
                start_block = await self.bc.last_block_before_timestamp(proposal['chain_id'], startts)
            else:
                start_block = -1

            if curts >= endts:
                end_block = await self.bc.last_block_before_timestamp(proposal['chain_id'], endts)
            else:
                end_block = -1


            if start_block > 0:
                proposal['total_voting_power_at_start'] = str(await self.read_snapshot_votable_supply(start_block, self.infra_dao_slug))
            
            if 'delete_event' in proposal:
                proposal['lifecycle_stage'] = 'CANCELLED'
            elif curts < startts:
                proposal['lifecycle_stage'] = 'PENDING'
            elif startts <= curts < endts:
                proposal['lifecycle_stage'] = 'ACTIVE'
            elif curts >= endts:

                if proposal_type_name == 'UNSET':
                    proposal['lifecycle_stage'] = 'EXPIRED'
                    liveness = 'archived'
                else:

                    # TODO - Count Abstain?

                    passing_quorum = (proposal['proposal_type']['quorum'] / 10000) * int(proposal['total_voting_power_at_start'])
                    passing_approval_threshold = (proposal['proposal_type']['approval_threshold'] / 10000) * int(proposal['total_voting_power_at_start'])
                    
                    quorum_check = passing_quorum > sum([int(weight) for weight in proposal['outcome']['token-holders'].values()])
                    approval_check = passing_approval_threshold > proposal['outcome']['token-holders'].get(1, 0)

                    proposal['quorum_check'] = quorum_check
                    proposal['approval_check'] = approval_check

                    if quorum_check and approval_check:
                        proposal['lifecycle_stage'] = 'PASSED'
                        
                    else:
                        proposal['lifecycle_stage'] = 'DEFEATED'


            proposal['start_blocktime'] = startts
            proposal['end_blocktime'] = endts

            proposal['start_block'] = start_block
            proposal['end_block'] = end_block

            del proposal['startts']
            del proposal['endts']
            del proposal['proposal_id']

            liveness = 'live'

            if curts > (endts + FIVE_MINUTES_IN_SECONDS):
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


