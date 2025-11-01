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

    def __init__(self, infra_dao_slug, config=None, reset=False):

        super().__init__(infra_dao_slug, config, reset)

        try:

            self.oodao_dao_id = self.config['oodao']['address']
            self.oodao_chain_id = self.config['oodao']['chain_id']

            self.token_addr = self.config['token']['address']
            self.token_chain_id = self.config['token']['chain_id']

            self.dao_slug = self.config['dao_slug'] # This is the capitals one, in the DB.  infra_dao_slug is the lowercase one that matches the DB schema and tenants config file names.

        except:
            raise Exception(f'problem with config: {self.config}')
    
    async def read_votes_from_db(self, proposal_id):
        pool = await self.pg.connect()
        async with pool.acquire() as connection:
            # No need to contract scope this, because the proposal_id is unique
            qry = f"""select transaction_hash, block_number, chain_id, voter, support, weight, ts from {self.infra_dao_slug}.votes where proposal_id = '{proposal_id}';"""
            rows = await connection.fetch(qry)
            return rows

    async def read_votes_direct_from_eas(self, proposal_id):
        pool = await self.pg.connect()
        async with pool.acquire() as connection:
            # No need to contract scope this, because the proposal_id is unique
            qry = f"""select * from auazure."eas_attestations_v2" ocv WHERE topic3 in ('{OODAO['SIMPLE_VOTE']}', '{OODAO['ADVANCED_VOTE']}') and decoded_attestation->'proposal_id' = '{proposal_id}';"""
            rows = await connection.fetch(qry)
            return rows

    async def read_proposal_type_range(self, dao_slug):

        pool = await self.pg.connect()
        async with pool.acquire() as connection:
            row = await connection.fetchrow(f"""select min(quorum::numeric)::text min_quorum_pct, max(quorum::numeric)::text max_quorum_pct, min(approval_threshold::numeric)::text min_approval_threshold_pct, max(approval_threshold::numeric)::text max_approval_threshold_pct from {dao_slug}.proposal_types where contract = '{self.oodao_dao_id}';""")
            return row
        
    async def read_snapshot_votable_supply(self, block_number):

        pool = await self.pg.connect()
        async with pool.acquire() as connection:

            # This only works if the token has a different address on different chains.
            row = await connection.fetchrow(f"""select {self.infra_dao_slug}.get_votable_supply_at_block({block_number}, '{self.token_addr}') as votable_supply;""")
            return int(row['votable_supply'])

    async def read_snapshot_voting_power(self, delegate, block_number):

        pool = await self.pg.connect()
        async with pool.acquire() as connection:
            row = await connection.fetchrow(f"""select {self.infra_dao_slug}.get_voting_power_at_block('{delegate}', {block_number}, '{self.token_addr}') as voting_power;""")
            return int(row['voting_power'])
    
    async def get_vp_snapshot_all_delegates(self, block_number):

        qry = f"""select distinct on (delegate) delegate, block_number, new_votes 
                    from auazure.multi_synd_token_delegate_votes_changed 
                    where
                    and address = '{self.token_addr}' 
                    and block_number <= {block_number}
                ORDER BY delegate, block_number desc"""
         
        pool = await self.pg.connect()
        async with pool.acquire() as connection:
            rows = await connection.fetch(qry)
            return rows


    async def get_delegate_metadata(self):

        qry = f"""select distinct on(address) address,
                                              discord, 
                                              twitter as x, 
                                              warpcast 
                    from agora.delegate_statements mstdc 
                    where "dao_slug" = '{self.dao_slug}' 
                    and (LENGTH(discord) > 2 or LENGTH(twitter) > 2 or LENGTH(warpcast) > 2) 
                    order by address, updated_at_ts desc"""
         
        pool = await self.pg.connect()

        out = {}
        async with pool.acquire() as connection:
            rows = await connection.fetch(qry)

            for row in rows:
                addr = row['address'].lower()
                out[addr] = {}

                for platform in ['warpcast', 'x', 'discord']:
                    if (row[platform] is not None):
                        out[addr][platform] = row[platform]

            return out

    async def read_proposal_type(self, proposal_id):

        # KEEPING THIS LOGIC SEPERATE AND SYNC for now, to make it easier to debug and change.

        qry = f"""SELECT 
                    data,
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
                authors_prop_type['eas_uid'] = row['data']

        qry = f"""SELECT 
                    data,
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
                approved_prop_type['eas_uid'] = row['data']

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
                                                from auazure."eas_attestations_v2" ocp WHERE topic3 = '{OODAO['CREATE_PROPOSAL']}' and topic1_cropped == '{self.oodao_dao_id}';""")
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
                                                from auazure."eas_attestations_v2" ocp WHERE decoded_attestation->>'verb' = 'CREATE_PROPOSAL' and topic1_cropped == '{self.oodao_dao_id}';""")
            return rows

    async def refresh_list(self, gcs_client: 'GCSClient'):

        self.bc.clear_lru()
        self.delegate_metadata = None

        ######################################
        # Step 1 - Get a list of recent-ish proposals.  Think either the "full list of any proposal ever" OR "just stuff that may or may not be ready to archive"
        #

        proposals = await self.read_proposals()
        deletions = await self.read_proposal_deletions()

        deletions = {row['ref_uid'] : dict(row) for row in deletions}

        default_type_ranges = await self.read_proposal_type_range()
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

            proposal['tags'] = proposal['tags'].split(',')
            assert isinstance(proposal['tags'], list), "Expected tags to be a list, but got %s" % type(proposal['tags'])

            if approved_prop_type:
                proposal['proposal_type'] = approved_prop_type
                proposal['proposal_type_approval'] = 'APPROVED'
            elif authors_prop_type and ('gov-proposal' in proposal['tags']):
                proposal['proposal_type'] = authors_prop_type
                proposal['proposal_type_approval'] = 'APPROVED'
            elif authors_prop_type:
                proposal['proposal_type'] = authors_prop_type
                proposal['proposal_type_approval'] = 'PENDING'
                proposal['default_proposal_type_ranges'] = default_type_ranges
            else: # Backwards compatibility only, will delete later this week.
                proposal['proposal_type'] = {"eas_uid": '0x0',"name": "Unset Proposal Type", "class": "STANDARD", "quorum": 10000, "description": "This Proposal Type is Unset", "approval_threshold": 10000}
                proposal['proposal_type_approval'] = 'ERROR'
                proposal['default_proposal_type_ranges'] = default_type_ranges

            proposal_type_name = proposal['proposal_type'].get('class', 'STANDARD')
            
            proposal['proposer'] = to_eth_address(proposal_meta['author'])
            try:
                proposal['proposer_ens'] = await self.bc.get_ens_lru(proposal['proposer'])
            except:
                pass
            proposal['proposer_ens'] = None
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
            reuse_tally = existing_num_of_votes > 0 and (existing_num_of_votes == num_of_votes) and (not self.reset)
            proposal['num_of_votes'] = num_of_votes

            if reuse_tally:
                existing_proposal_data = await gcs_client.read_dict(blob.name)
                outcome = existing_proposal_data['outcome']
            
            else:

                if self.delegate_metadata is None:
                    self.delegate_metadata = await self.get_delegate_metadata()

                if proposal_type_name in ('UNSET', 'OPTIMISTIC', 'STANDARD'):

                    outcome = defaultdict(lambda: defaultdict(int))

                    votes_out = []
                    vote_set = []

                    for vote in votes:

                        copy_of_vote = copy.deepcopy(dict(vote))
                        copy_of_vote['weight'] = str(int(vote['weight']))

                        vote_set.append(copy_of_vote['voter'])

                        addr = vote['voter'].lower()

                        try: # This isn't great, but 1 in 1000 calls seems to fail, and break the pipeline. 
                             # TODO - fix and remove.
                            copy_of_vote['ens'] = await self.bc.get_ens_lru(addr)
                        except:
                            pass

                        delegate_meta = self.delegate_metadata.get(addr, {})

                        copy_of_vote.update(delegate_meta)

                        outcome['token-holders'][vote['support']] += int(vote['weight']) 

                        votes_out.append(copy_of_vote)

                        # vote_att = json.loads(vote['decoded_attestation']) #vote['decoded_attestation']
                        # print(vote)
                        # choice = vote_att['choice']
                        # outcome['token-holders'][choice] += await self.read_snapshot_voting_power(vote['voter'], vote['block_number'], self.infra_dao_slug)
                
                    vote_set = set(vote_set)

                    for key in outcome['token-holders'].keys():
                        outcome['token-holders'][key] = str(outcome['token-holders'][key])

                    await self.overwrite_votes(votes_out, proposal_id, gcs_client)

                elif proposal_type_name == 'APPROVAL': 
                    raise NotImplementedError("Approval Types are Not implemented yet.")
                else:
                    raise NotImplementedError(f"Proposal Type {proposal_type_name} is not implemented yet.")

            proposal['outcome'] = outcome

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
                proposal['total_voting_power_at_start'] = str(await self.read_snapshot_votable_supply(start_block))

                if self.delegate_metadata is None:
                    self.delegate_metadata = await self.get_delegate_metadata()

                if not reuse_tally:
                    snapshot_vp = await self.get_vp_snapshot_all_delegates(start_block)

                    snapshot_vp_out = []
                    for row in snapshot_vp:

                        if (row['delegate'] not in vote_set) and int(row['new_votes']) > 0:

                            addr = row['delegate'].lower()
                            record = {
                                'addr': addr,
                                'bn': row['block_number'],
                                'vp': row['new_votes']
                                }
                            
                            try:
                                ens = await self.bc.get_ens_lru(addr)
                                if ens is not None:
                                    record['ens'] = ens
                            except:
                                pass

                            delegate_metadata = self.delegate_metadata.get(addr, {})

                            record.update(delegate_metadata)
                            
                            snapshot_vp_out.append(record)                        


                    await self.overwrite_hasnt_voted(snapshot_vp_out, proposal_id, gcs_client)
            
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
                    
                    quorum_check = sum([int(weight) for weight in proposal['outcome']['token-holders'].values()]) >= passing_quorum
                    approval_check = int(proposal['outcome']['token-holders'].get('1', 0)) >= passing_approval_threshold

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

