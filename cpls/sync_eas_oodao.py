import json, time, copy
from collections import defaultdict

from .sync import Sync, SkipProposal, FIVE_MINUTES_IN_SECONDS, to_eth_address
from .gcs import GCSClient

class EASOoDaoSync(Sync):

    SOURCE = 'eas-oodao'

    async def read_votes_from_db(self, proposal_id):
        pool = await self.pg.connect()
        async with pool.acquire() as connection:
            qry = f"""select support, weight from syndicate.votes where proposal_id = '{proposal_id}';"""
            rows = await connection.fetch(qry)
            return rows

    async def read_votes_direct_from_eas(self, proposal_id):
        pool = await self.pg.connect()
        async with pool.acquire() as connection:
            qry = f"""select * from auazure."eas_attestations_v2" ocv WHERE topic3 in ('0xffcc8fe77f55448bee5f0e24844ee76f83c3c2718dcf8a75de750cf4797ad0bc', '0x04cb5678af613212e584cf8d117ee3fcd038a9ab657ecf0c596cabe1e6ebd9f0') and decoded_attestation->'proposal_id' = '{proposal_id}';"""
            print(qry)
            rows = await connection.fetch(qry)
            return rows

    async def read_proposal_type_range(self, dao_slug):

        if dao_slug == 'jeffdao':
            dao_slug = 'syndicate'

        pool = await self.pg.connect()
        async with pool.acquire() as connection:
            row = await connection.fetchrow(f"""select min(quorum::numeric)::text min_quorum_pct, max(quorum::numeric)::text max_quorum_pct, min(approval_threshold::numeric)::text min_approval_threshold_pct, max(approval_threshold::numeric)::text max_approval_threshold_pct from {dao_slug}.proposal_types;""")
            return row
        
    async def read_snapshot_votable_supply(self, block_number, dao_slug):

        if dao_slug == 'jeffdao':
            dao_slug = 'syndicate'

        token = '0x55f6e82a8bf5736d46837246dcbeaf7e61b3c27c'
         
        pool = await self.pg.connect()
        async with pool.acquire() as connection:
            row = await connection.fetchrow(f"""select {dao_slug}.get_votable_supply_at_block({block_number}, '{token}') as votable_supply;""")
            return int(row['votable_supply'])

    async def read_snapshot_voting_power(self, delegate, block_number, dao_slug):

        if dao_slug == 'jeffdao':
            dao_slug = 'syndicate'

        token = '0x55f6e82a8bf5736d46837246dcbeaf7e61b3c27c'
         
        pool = await self.pg.connect()
        async with pool.acquire() as connection:
            row = await connection.fetchrow(f"""select {dao_slug}.get_voting_power_at_block('{delegate}', {block_number}, '{token}') as voting_power;""")
            return int(row['voting_power'])
           
    async def read_proposal_type(self, proposal_id):

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
                            topic3 = '0xc218b18af140c97644087c59e8ab35b981e73e026ffaf318f371c4ddc56efcb9'
                            AND decoded_attestation->>'proposal_id' = '{proposal_id}'
                        );"""

        pool = await self.pg.connect()
        async with pool.acquire() as connection:
            row = await connection.fetchrow(qry)

            if row is None:
                return None
            
            row = json.loads(row['decoded_attestation'])
            return row


        qry = """select decoded_attestation from  auazure.eas_attestations_v2 where id = 'log_0x244535597e1f8670a380d7eab2cf4d94a1bb57b93d89fe4cfff50fafe998b104_141'""";
        pool = await self.pg.connect()
        async with pool.acquire() as connection:
            row = await connection.fetchrow(qry)
            row = json.loads(row['decoded_attestation'])
            return row
        
        # pool = await self.pg.connect()
        # async with pool.acquire() as connection:
        #    rows = await connection.fetch(f"""select decoded_attestation->>'proposal_type' from auazure."eas_attestations_v2" ocp WHERE 
        #                                                  topic3 = '0xc218b18af140c97644087c59e8ab35b981e73e026ffaf318f371c4ddc56efcb9' and
        #                                                  decoded_attestation->'proposal_id' = '{proposal_id}' order by block_number desc;""")
        #    return rows
    
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
                                                decoded_attestation->'proposal_id' as proposal_id
                                                from auazure."eas_attestations_v2" ocp WHERE topic3 = '0x12e8600c9bb57b5b436fa09735cfc63e95098552122001c465b610261eea8a93';""")
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

            proposal_type = await self.read_proposal_type(proposal_id)

            if proposal_type is None:
                proposal_type_name = 'UNSET'
                proposal_type = copy.deepcopy(default_type_ranges)
            else:
                proposal_type_name = proposal_type.get('class')

            proposal['proposal_type'] = proposal_type
            
            proposal['proposer'] = to_eth_address(proposal_meta['author'])
            proposal['proposer_ens'] = await self.bc.get_ens(proposal['proposer'])
            del proposal['author']            

            try:
                blob, existing_liveness, existing_proposal_hash, existing_num_of_votes = await self.read_existing_raw_proposal_hash_if_exists(proposal_id, gcs_client)
            except SkipProposal as e:
                print(e)
                skipped_count += 1
                continue

            votes = await self.read_votes_from_db(proposal_id)
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

                elif proposal_type == 'APPROVAL': 
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
            
            # TODO - detect if cancelled.

            if False:
                proproal['lifecycle_stage'] = 'CANCELLED'
            elif curts >= startts:
                proposal['lifecycle_stage'] = 'PENDING'
            elif startts <= curts < endts:
                proposal['lifecycle_stage'] = 'ACTIVE'
            elif curts >= endts:

                passing_quorum = proposal['total_voting_power_at_start'] * proposal['quorum']
                
                if proposal['outcome']['token-holders']['YES'] >= passing_quorum:
                    proposal['lifecycle_stage'] = 'PASSED'
                else:
                    proposal['lifecycle_stage'] = 'FAILED'


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


