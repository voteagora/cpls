import json, time, copy
from collections import defaultdict

from .sync import Sync, SkipProposal, FIVE_MINUTES_IN_SECONDS, to_eth_address
from .gcs import GCSClient

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
            qry = f"""select * from auazure."eas_attestations_v2" ocv WHERE topic3 in ('0xa68afde70897d2955e726c1a1da9e77ab466994b5da6666ceb518a5c538edc1e', '0x22e4a4e20f724e4162a553d076493d05d3edaff561c2708f67f4a23067074413') and decoded_attestation->'proposal_id' = '{proposal_id}';"""
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
                            topic3 = '0x4468df37e17deb20b5096fb12107d4841b79ff6a62292e798eb7b79d0e764eb5'
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
                                                data as proposal_id
                                                from auazure."eas_attestations_v2" ocp WHERE topic3 = '0x442d586d8424b5485de1ff46cb235dcb96b41d19834926bbad1cd157fbeeb8fc';""")
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
                    
                    quorum_check = passing_quorum > (proposal['outcome']['token-holders'][1] + proposal['outcome']['token-holders'][2]) 
                    approval_check = passing_approval_threshold > proposal['outcome']['token-holders'][1]

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


