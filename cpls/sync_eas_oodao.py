import json, time, copy
from collections import defaultdict

from .sync import Sync, SkipProposal, FIVE_MINUTES_IN_SECONDS, to_eth_address
from .gcs import GCSClient
from .config import PROPOSAL_CHECK_SECRET, get_proposal_check_api_url

OODAO = {
    11155111 : {
       'INSTANTIATE' : '0xa45718ef6b8758277682e9914ed85b960e19fd8331ed75e24641d228b7efcd2d',
       'PERMA_INSTANTIATE' : '0x3921c650e5c0a565fe4e2b5dad38546999588bd18904a3354641ca6c998f6bc4',
       'GRANT' : '0xd430f8dc7a9503e92e621503eed2c716a524604be530842601d7fb0e1bb8ff15',
       'CREATE_PROPOSAL_TYPE' : '0x4147434e77680f972dcaa494427b876fa0f5ecdfde56131dd24a988ad90a6950',
       'CREATE_PROPOSAL' : '0x38bfba767c2f41790962f09bcf52923713cfff3ad6d7604de7cc77c15fcf169a',
       'CHECK_PROPOSAL' : '0x08df8e6e629077cabef4ed15cd4ff4f2359c2a60ad65b8355ac1f905b8f23a6f',
       'SET_PROPOSAL_TYPE' : '0xa6ca209ead271e33d86bf969fb5b9d5f559bf3fb22765ede70652b1faa4973b5',
       'SET_PARAM_VALUE' : '0xa1e21d322b14d3d79bd697b106b7374e19a61eb766907ef27d392dd635d9642f',
       'DELEGATED_SIMPLE_VOTE' : '0x291f9b12f6624076505cb07cc62acf79bd7403cceb435e91d279dcbe6336c94b',
       'DELEGATED_ADVANCED_VOTE' : '0xd9a51aa77ea609950350db55093af36e4c0dce621a131164cb7b410b9c2435bc',
       'SIMPLE_VOTE' : '0x19c36b80a224c4800fd6ed68901ec21f591563c8a5cb2dd95382d430603f91ff',
       'ADVANCED_VOTE' : '0x991b014c62b19364882fc89dbf3baa6104b4598ee2c4f29152be2cbcfcb4cb81',
       'DELETE' : '0x2c451b53c595d44441eb1e8242912a0446e7bfc5f745535537908bef47e6e334',
    }, 
    1 : {
       'INSTANTIATE' : '0x4564d3a746bafcf78838969daaff3ba173e9f5ab73ac3023ea98dd9220953e75',
       'PERMA_INSTANTIATE' : '0xc825cf97f111a55edfb1bec7b6b624dae6ccab74a3a3a42f89891dfb0d06137b',
       'GRANT' : '0x8c6e4ae96424697731fd6aaa20b759a25684af059a23ba1b6aa48a12c21fd5d7',
       'CREATE_PROPOSAL_TYPE' : '0xab4e473a3f8a0a0a490619bcd2ecf23ad1be4720d4033fa16f2d1cbd1519caa1',
       'CREATE_PROPOSAL' : '0x38bfba767c2f41790962f09bcf52923713cfff3ad6d7604de7cc77c15fcf169a',
       'CHECK_PROPOSAL' : '0x80155c3a8c4ea17ce96e8899f7ab1ceca9e85382d7f893619a1d03947a70f844',
       'SET_PROPOSAL_TYPE' : '0x0519039455b478a51b33c82934bf28814f5755f6bb21c20fc47fd98c2a3fafa3',
       'SET_PARAM_VALUE' : '0x5c27757206150b56764617513558234fc12ab9bb64eee71afb525fa9689c4842',
       'DELEGATED_SIMPLE_VOTE' : '0x59bad5b0800beda0617b1d883711be28f8a0e619ef549310bfa6432ba64ab399',
       'DELEGATED_ADVANCED_VOTE' : '0xfcff350666dcf68a382ea5e22ac4529915312b29f3bfe5a070424dcc781e354e',
       'SIMPLE_VOTE' : '0x12cd8679de42e111a5ece9f2aee44dc8b8351024dea881cda97c2ff5b58349f6',
       'ADVANCED_VOTE' : '0xc4465af5d96b474b1c7a6418500461d3de1fc35552679bf695eb2b3124817dce',
       'DELETE' : '0x8c97e2700d9a3e52e76a4bf3c42b5a5d178f193cb14f6e2ddbc6db4ed1eae767',
    }
}
class EASOoDaoSync(Sync):

    SOURCE = 'eas-oodao'

    def __init__(self, infra_dao_slug, config=None, reset=False, http_client=None):

        super().__init__(infra_dao_slug, config, reset, http_client)

        try:

            self.index_tenant_prefix = self.config['index_tenant_prefix']
            
            self.oodao_dao_id = self.config['deployment']['oodao']['address']
            self.oodao_chain_id = self.config['deployment']['oodao']['chain_id']
            self.oodao_shemas = OODAO[int(config['deployment']['oodao']['chain_id'])]

            self.token_addr = self.config['deployment']['token']['address']
            self.token_chain_id = self.config['deployment']['chain_id']

            self.dao_slug = self.config['dao_slug'] # This is the capitals one, in the DB.  infra_dao_slug is the lowercase one that matches the DB schema and tenants config file names.

        except:
            raise Exception(f'problem with config: {self.config}')

    # async def read_votes_direct_from_eas(self, proposal_id):
    #     pool = await self.pg.connect()
    #     async with pool.acquire() as connection:
    #         # No need to contract scope this, because the proposal_id is unique
    #         qry = f"""select * from auazure."eas_attestations_v2" ocv WHERE topic3 in ('{self.oodao_shemas['SIMPLE_VOTE']}', '{self.oodao_shemas['ADVANCED_VOTE']}') and decoded_attestation->'proposal_id' = '{proposal_id}';"""
    #         rows = await connection.fetch(qry)
    #         return rows

    async def read_proposal_type_range(self):

        pool = await self.pg.connect()
        async with pool.acquire() as connection:
            row = await connection.fetchrow(f"""select min(quorum::numeric)::text min_quorum_pct, max(quorum::numeric)::text max_quorum_pct, min(approval_threshold::numeric)::text min_approval_threshold_pct, max(approval_threshold::numeric)::text max_approval_threshold_pct from {self.infra_dao_slug}.proposal_types where contract = '{self.oodao_dao_id}';""")
            return row
        
    async def read_snapshot_votable_supply(self, block_number, chain_id):

        pool = await self.pg.connect()
        async with pool.acquire() as connection:
            qry = f"""select {self.infra_dao_slug}.get_votable_supply_at_block({block_number}, '{self.token_addr.lower()}') as votable_supply;"""
            # This only works if the token has a different address on different chains.
            row = await connection.fetchrow(qry)
            vp = int(row['votable_supply'])
            print(vp)

        total_nonivotes_vp = await self.get_total_nonivotes_vp_at_block(block_number)
        total_votable_supply = vp + total_nonivotes_vp
        
        if total_nonivotes_vp > 0:
            print(f"Total votable supply: {vp} (DB) + {total_nonivotes_vp} (nonivotes) = {total_votable_supply}")
        
        return total_votable_supply


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
                            topic3 = '{self.oodao_shemas['SET_PROPOSAL_TYPE']}'
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
                                                decoded_attestation->>'kwargs' as kwargs,
                                                data as proposal_id,
                                                block_number as created_block_number,
                                                attestation_time as created_time
                                                from auazure."eas_attestations_v2" ocp WHERE topic3 = '{self.oodao_shemas['CREATE_PROPOSAL']}' and topic1_cropped = '{self.oodao_dao_id}';""")
            return rows
    
    async def read_proposal_checks(self):

        pool = await self.pg.connect()
        async with pool.acquire() as connection:
            rows = await connection.fetch(f"""select 
                                                ref_uid as proposal_id,
                                                data as check_uid,
                                                attestation_time
                                                from auazure."eas_attestations_v2" ocp 
                                                WHERE topic3 = '{self.oodao_shemas['CHECK_PROPOSAL']}' 
                                                AND topic1_cropped = '{self.oodao_dao_id}'
                                                AND ref_uid IS NOT NULL;""")
            return rows


    async def validate_proposal(self, proposal_id: str, attester: str, tags: list) -> bool:
        api_url = get_proposal_check_api_url(self.infra_dao_slug)
        if not api_url or not PROPOSAL_CHECK_SECRET:
            return False

        try:
            response = await self.http_client.post(
                api_url,
                json={
                    "proposalId": proposal_id,
                    "attester": attester,
                    "tags": tags
                },
                headers={
                    "Authorization": f"Bearer {PROPOSAL_CHECK_SECRET}",
                    "Content-Type": "application/json"
                }
            )
            response.raise_for_status()
            result = response.json()
            return result.get("success", False)
        except Exception as e:
            print(f"Error validating proposal {proposal_id} with API: {e}")
            return False


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
                                                from auazure."eas_attestations_v2" ocp WHERE decoded_attestation->>'verb' = 'CREATE_PROPOSAL' and topic1_cropped = '{self.oodao_dao_id}';""")
            return rows

    async def refresh_list(self, gcs_client: 'GCSClient'):

        self.bc.clear_lru()
        self.delegate_metadata = None

        ######################################
        # Step 1 - Get a list of recent-ish proposals.  Think either the "full list of any proposal ever" OR "just stuff that may or may not be ready to archive"
        #

        proposals = await self.read_proposals()
        deletions = await self.read_proposal_deletions()
        checks = await self.read_proposal_checks()

        deletions = {row['ref_uid'] : dict(row) for row in deletions}
        checks = {row['proposal_id'] : dict(row) for row in checks}

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

            required_fields = ['tags', 'title', 'description']
            missing_fields = [f for f in required_fields if not proposal.get(f)]
            
            if missing_fields:
                print(f"Proposal {proposal_id}: Corrupted attestation data, missing fields: {missing_fields}")
                proposal['tags'] = []
                proposal['validation_failed'] = {
                    "failed_at": int(time.time()),
                    "reason": "corrupted_attestation_data"
                }
                proposal_hash = self.calculate_proposal_hash(proposal)
                await self.overwrite_proposal(proposal, proposal_hash, 'unqualified', gcs_client)
                skipped_count += 1
                continue

            try:
                blob, existing_liveness, existing_proposal_hash, existing_num_of_votes = await self.read_existing_raw_proposal_hash_if_exists(proposal_id, gcs_client)
            except SkipProposal as e:
                print(e)
                skipped_count += 1
                continue

            proposal_already_saved = existing_liveness != 'new'
            has_check_attestation = proposal_id in checks

            proposal['tags'] = proposal['tags'].split(',')
            assert isinstance(proposal['tags'], list), "Expected tags to be a list, but got %s" % type(proposal['tags'])

            # Check if this proposal has already failed validation before
            if not has_check_attestation and not proposal_already_saved:
                attester = to_eth_address(proposal_meta['author'])
                validation_passed = await self.validate_proposal(proposal_id, attester, proposal['tags'])
                if not validation_passed:
                    print(f"Proposal {proposal_id}: validation failed, marking as unqualified")
                    proposal['validation_failed'] = {
                        "failed_at": int(time.time()),
                        "reason": "validation_failed"
                    }
                    proposal_hash = self.calculate_proposal_hash(proposal)
                    await self.overwrite_proposal(proposal, proposal_hash, 'unqualified', gcs_client)
                    skipped_count += 1
                    continue

            authors_prop_type, approved_prop_type = await self.read_proposal_type(proposal_id)
            if 'kwargs' in proposal and proposal['kwargs'] is not None:
                # Normalize kwargs to a JSON object (dict) when possible.
                # Also lift voting_module to the top-level for convenience.
                orig_kwargs = proposal['kwargs']
                parsed_kwargs = orig_kwargs
                if isinstance(orig_kwargs, str):
                    try:
                        parsed_kwargs = json.loads(orig_kwargs)
                    except Exception:
                        parsed_kwargs = orig_kwargs

                if isinstance(parsed_kwargs, dict):
                    voting_module = parsed_kwargs.get('voting_module')
                    if voting_module is not None:
                        proposal['voting_module'] = voting_module
                    proposal['kwargs'] = parsed_kwargs
                else:
                    proposal['kwargs'] = orig_kwargs
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

            voting_module = proposal.get('voting_module')
            proposal_type_name = voting_module.upper() if isinstance(voting_module, str) and voting_module.lower() in ('standard', 'optimistic', 'approval') else 'STANDARD'
            
            proposal['proposer'] = to_eth_address(proposal_meta['author'])
            try:
                proposal['proposer_ens'] = await self.bc.get_ens_lru(proposal['proposer'])
            except:
                pass
            proposal['proposer_ens'] = None
            del proposal['author']            

            votes = await self.read_votes_from_db(proposal_id)
            num_of_votes = len(votes)

            # No new votes have come in, we can re-use the last tally
            reuse_tally = existing_num_of_votes > 0 and (existing_num_of_votes == num_of_votes) and (not self.reset)
            proposal['num_of_votes'] = num_of_votes

            if reuse_tally:
                existing_proposal_data = await gcs_client.read_dict(blob.name)

                if existing_proposal_data is None:
                    raise Exception("We got None for existing_proposal_data for %s, this shouldn't be possible" % blob.name)
                
                outcome = existing_proposal_data['outcome']

            if not reuse_tally:

                if self.delegate_metadata is None:
                    self.delegate_metadata = await self.get_delegate_metadata()

                if proposal_type_name in ('UNSET', 'OPTIMISTIC', 'STANDARD'):

                    outcome = defaultdict(lambda: defaultdict(int))

                    votes_out = []
                    voter_set = []

                    for vote in votes:

                        copy_of_vote = copy.deepcopy(dict(vote))
                        copy_of_vote['weight'] = str(int(vote['weight']))

                        voter_set.append(copy_of_vote['voter'])

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

                    voter_set = set(voter_set)

                    for key in outcome['token-holders'].keys():
                        outcome['token-holders'][key] = str(outcome['token-holders'][key])

                    await self.overwrite_votes(votes_out, proposal_id, gcs_client)

                elif proposal_type_name == 'APPROVAL': 

                    outcome = {
                        'token-holders': defaultdict(lambda: defaultdict(int)),
                        'no-param': defaultdict(int)
                    }

                    votes_out = []
                    voter_set = []

                    for vote in votes:

                        copy_of_vote = copy.deepcopy(dict(vote))
                        copy_of_vote['weight'] = str(int(vote['weight']))

                        voter_set.append(copy_of_vote['voter'])
                        addr = vote['voter'].lower()

                        try:
                            copy_of_vote['ens'] = await self.bc.get_ens_lru(addr)
                        except:
                            pass

                        delegate_meta = self.delegate_metadata.get(addr, {})
                        copy_of_vote.update(delegate_meta)

                        support = "1"
                        weight = int(vote['weight'])

                        try:
                            if isinstance(vote['support'], str):
                                if ',' in vote['support']:
                                    options = [int(x.strip()) for x in vote['support'].split(',')]
                                else:
                                    try:
                                        options = json.loads(vote['support'])
                                    except json.JSONDecodeError:
                                        options = [int(vote['support'])]
                            else:
                                options = support

                            if not isinstance(options, (list, tuple)):
                                options = [options]

                            for option in options:
                                outcome['token-holders'][str(option)][1] += weight

                            outcome['no-param'][1] += weight

                            copy_of_vote['params'] = options
                            copy_of_vote['support'] = "1"
                        except (TypeError, KeyError, ValueError) as e:
                            print(f"Warning: Failed to process vote for {copy_of_vote.get('voter', 'unknown')}: {e}")
                            print(f"Support value: {support}, type: {type(support)}")
                            copy_of_vote['params'] = []
                            copy_of_vote['support'] = "1"

                        votes_out.append(copy_of_vote)

                    voter_set = set(voter_set)

                    for option_key in outcome['token-holders'].keys():
                        for support_key in outcome['token-holders'][option_key].keys():
                            outcome['token-holders'][option_key][support_key] = str(outcome['token-holders'][option_key][support_key])

                    for support_key in outcome['no-param'].keys():
                        outcome['no-param'][support_key] = str(outcome['no-param'][support_key])

                    await self.overwrite_votes(votes_out, proposal_id, gcs_client)
                else:
                    raise NotImplementedError(f"Proposal Type {proposal_type_name} is not implemented yet.")

            proposal['outcome'] = outcome

            startts = int(proposal_meta['startts'])
            endts = int(proposal_meta['endts'])

            curts = int(time.time())

            # Cache-busting fields to ensure proposals in different time phases have different hashes
            proposal['after_start'] = curts >= startts
            proposal['after_end'] = curts >= endts

            try:
                proposal_hash = self.check_existing_proposal_hash(proposal, existing_proposal_hash)
            except SkipProposal as e:
                print(e)
                skipped_count += 1
                continue

            if curts >= startts:
                start_block = await self.bc.last_block_before_timestamp(proposal['chain_id'], startts)
            else:
                start_block = -1

            if curts >= endts:
                end_block = await self.bc.last_block_before_timestamp(proposal['chain_id'], endts)
            else:
                end_block = -1


            if start_block > 0: # For OODAO, this means a block number is known.
                # For syndicate, use total supply of the token instead of votable supply
                if self.infra_dao_slug in ('syndicate', 'towns'):
                    # Get total supply from the token contract at the start block
                    result = await self.bc.contract_call_encoded(
                        self.token_chain_id,
                        self.token_addr,
                        start_block,
                        'totalSupply()',
                        []
                    )
                    total_supply = int(result['result'], 16)
                    proposal['total_voting_power_at_start'] = str(total_supply)
                else:
                    # Default behavior for other tenants
                    proposal['total_voting_power_at_start'] = str(await self.read_snapshot_votable_supply(start_block, proposal['chain_id']))

                if self.delegate_metadata is None:
                    self.delegate_metadata = await self.get_delegate_metadata()

                if not reuse_tally:
                    snapshot_vp = await self.get_vp_snapshot_all_delegates(start_block, gcs_client, chain_id=proposal['chain_id'])

                    snapshot_vp_lookup = {row['addr'].lower(): row for row in snapshot_vp}

                    votes_out_updated = []

                    if proposal_type_name == 'APPROVAL':
                        outcome_updated = {
                            'token-holders': defaultdict(lambda: defaultdict(int)),
                            'no-param': defaultdict(int)
                        }

                        for vote in votes_out:
                            addr = vote['voter'].lower()
                            vp_entry = snapshot_vp_lookup.get(addr)

                            if vp_entry:
                                # Update vote weight with actual VP (delegation + nonivotes)
                                vote['weight'] = vp_entry['vp']
                                vote_weight = int(vp_entry['vp'])
                            else:
                                # Keep original weight if not in snapshot
                                vote_weight = int(vote['weight'])

                            # Recalculate outcome with correct VP for APPROVAL proposals
                            params = vote.get('params', [])
                            if params:
                                for option in params:
                                    outcome_updated['token-holders'][str(option)][1] += vote_weight
                                outcome_updated['no-param'][1] += vote_weight

                            votes_out_updated.append(vote)

                        # Update outcome with recalculated values
                        for option_key in outcome_updated['token-holders'].keys():
                            for support_key in outcome_updated['token-holders'][option_key].keys():
                                outcome['token-holders'][option_key][support_key] = str(outcome_updated['token-holders'][option_key][support_key])

                        for support_key in outcome_updated['no-param'].keys():
                            outcome['no-param'][support_key] = str(outcome_updated['no-param'][support_key])
                    else:
                        outcome_updated = defaultdict(lambda: defaultdict(int))

                        for vote in votes_out:
                            addr = vote['voter'].lower()
                            vp_entry = snapshot_vp_lookup.get(addr)

                            if vp_entry:
                                # Update vote weight with actual VP (delegation + nonivotes)
                                vote['weight'] = vp_entry['vp']
                                vote_weight = int(vp_entry['vp'])
                            else:
                                # Keep original weight if not in snapshot
                                vote_weight = int(vote['weight'])

                            # Recalculate outcome with correct VP for STANDARD/OPTIMISTIC proposals
                            outcome_updated['token-holders'][int(vote.get('support', 0))] += vote_weight
                            votes_out_updated.append(vote)

                        # Update outcome with recalculated values
                        for key in outcome_updated['token-holders'].keys():
                            outcome['token-holders'][str(key)] = str(outcome_updated['token-holders'][key])

                    # Overwrite votes with VP-enriched data
                    await self.overwrite_votes(votes_out_updated, proposal_id, gcs_client)

                    snapshot_vp_out = []
                    for row in snapshot_vp:

                        if (row['addr'] not in voter_set):

                            addr = row['addr'].lower()
                            record = copy.deepcopy(row)

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
            
            # Handle lifecycle states in priority order
            # CANCELLED takes precedence over all other states
            if 'delete_event' in proposal:
                proposal['lifecycle_stage'] = 'CANCELLED'
            # Time-based states
            elif curts < startts:
                proposal['lifecycle_stage'] = 'PENDING'
            elif startts <= curts < endts:
                proposal['lifecycle_stage'] = 'ACTIVE'
            elif curts >= endts:

                if proposal_type_name == 'UNSET':
                    proposal['lifecycle_stage'] = 'EXPIRED'
                    liveness = 'archived'
                elif proposal_type_name == 'OPTIMISTIC':
                    # For OPTIMISTIC type:
                    # Quorum = forVotes + abstainVotes (total votes)
                    # If quorum not met -> SUCCEDED (optimistic passes by default)
                    # If quorum met and against votes > threshold -> DEFEATED
                    # Otherwise -> SUCCEEDED

                    outcome_data = proposal['outcome']['token-holders']

                    for_votes = int(outcome_data.get('1', 0))
                    against_votes = int(outcome_data.get('0', 0))
                    abstain_votes = int(outcome_data.get('2', 0))
                    total_votes = for_votes + against_votes + abstain_votes
                    passing_quorum = (proposal['proposal_type']['quorum'] / 10000) * int(proposal['total_voting_power_at_start'])
                    quorum_check = total_votes >= passing_quorum
                    proposal['quorum_check'] = quorum_check

                    if not quorum_check:
                        # Optimistic proposals pass if quorum not met
                        proposal['lifecycle_stage'] = 'SUCCEEDED'
                    else:
                        # Check if against votes exceed threshold
                        threshold = proposal['proposal_type'].get('threshold', 0)
                        threshold_value = (threshold / 10000) * int(proposal['total_voting_power_at_start'])
                        
                        if against_votes > threshold_value:
                            proposal['lifecycle_stage'] = 'DEFEATED'
                        else:
                            proposal['lifecycle_stage'] = 'SUCCEEDED'
                elif proposal_type_name == 'APPROVAL':
                    # For APPROVAL type:
                    # Quorum = forVotes + abstainVotes (total votes)
                    # If quorum not met -> DEFEATED
                    # If criteria == THRESHOLD: any option > criteriaValue -> SUCCEEDED, else DEFEATED
                    # Otherwise -> SUCCEEDED

                    outcome_data = proposal['outcome']['token-holders']

                    # Sum all votes across options (for + abstain = total)
                    total_votes = 0
                    for option_key, support_dict in outcome_data.items():
                        try:
                            if isinstance(support_dict, dict):
                                for support_val in support_dict.values():
                                    total_votes += int(support_val)
                            else:
                                total_votes += int(support_dict)
                        except (ValueError, TypeError) as e:
                            print(f"Warning: Failed to process votes for option {option_key}: {e}")
                            print(f"Support dict value: {support_dict}, type: {type(support_dict)}")

                    passing_quorum = (proposal['proposal_type']['quorum'] / 10000) * int(proposal['total_voting_power_at_start'])
                    quorum_check = total_votes >= passing_quorum
                    proposal['quorum_check'] = quorum_check

                    if not quorum_check:
                        proposal['lifecycle_stage'] = 'DEFEATED'
                    else:
                        # Check if criteria is THRESHOLD
                        criteria = proposal['proposal_type'].get('criteria', None)
                        thresold = proposal['proposal_type'].get('threshold', 0)
                        
                        if criteria == 'THRESHOLD':
                            # Any option exceeding threshold -> SUCCEEDED
                            succeeded = False
                            for option_key, support_dict in outcome_data.items():
                                try:
                                    if isinstance(support_dict, dict):
                                        option_votes = sum(int(v) for v in support_dict.values())
                                    else:
                                        option_votes = int(support_dict)
                                    
                                    if option_votes > thresold:
                                        succeeded = True
                                        break
                                except (ValueError, TypeError) as e:
                                    print(f"Warning: Failed to process threshold check for option {option_key}: {e}")
                                    print(f"Support dict value: {support_dict}, type: {type(support_dict)}")
                            proposal['lifecycle_stage'] = 'SUCCEEDED' if succeeded else 'DEFEATED'
                        else:
                            proposal['lifecycle_stage'] = 'SUCCEEDED'
                else:

                    # TODO - Count Abstain?

                    passing_quorum = (proposal['proposal_type']['quorum'] / 10000) * int(proposal['total_voting_power_at_start'])
                    passing_approval_threshold = (proposal['proposal_type']['approval_threshold'] / 10000) * int(proposal['total_voting_power_at_start'])

                    quorum_check = sum([int(weight) for weight in proposal['outcome']['token-holders'].values()]) >= passing_quorum
                    approval_check = int(proposal['outcome']['token-holders'].get('1', 0)) >= passing_approval_threshold

                    proposal['quorum_check'] = quorum_check
                    proposal['approval_check'] = approval_check

                    if quorum_check and approval_check:
                        proposal['lifecycle_stage'] = 'SUCCEEDED'
                        
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

        if anything_changed or self.reset:
            await self.refresh_source_list(gcs_client)
            await self.refresh_full_list(gcs_client)

        return {
            'skipped': skipped_count,
            'refreshed': refreshed_count
        }

