import copy
import httpx, time
import asyncio
from .gcs import GCSClient
from .sync import Sync, SkipProposal, FIVE_MINUTES_IN_SECONDS

from .title_processor import get_title_from_proposal_description



class DaoNodeSync(Sync):

    SOURCE = 'dao_node'
    
    def __init__(self, infra_dao_slug, config=None, reset=False):

        super().__init__(infra_dao_slug, config, reset)

        try:
            self.index_tenant_prefix = self.config['index_tenant_prefix']

            self.gov_addr = self.config['deployment']['gov']['address']
            self.token_addr = self.config['deployment']['token']['address']
            self.chain_id = self.config['deployment']['chain_id']

            self.dao_slug = self.config['dao_slug'] # This is the capitals one, in the DB.  infra_dao_slug is the lowercase one that matches the DB schema and tenants config file names.

        except:
            raise Exception(f'problem with config: {self.config}')
    
    async def estimated_timestamp_from_future_block_number(self, block_number: int):

        return self.bc.get_estimated_blocktime(self.chain_id, block_number)
    
    async def read_snapshot_votable_supply(self, block_number: int):
        
        if self.infra_dao_slug == 'optimism':
            return -2 # await self.bc.votable_supply_at_block_with_oracle(self.chain_id, self.gov_addr, block_number)
        else:
            return -1
    
    async def read_govless_proposal_mappings(self):
        pool = await self.pg.connect()
        async with pool.acquire() as connection:
            rows = await connection.fetch(f"""select id as govless_proposal_id, onchain_proposalid as governor_proposalid from alltenant.offchain_proposals op where onchain_proposalid is not null;""")
            mapping = {r['governor_proposalid']: r['govless_proposal_id'] for r in rows}
            return mapping

    def govless_proposal_blob_name(self, proposal_id):
        return f"data/{self.infra_dao_slug}/proposal/eas-atlas/raw/{proposal_id}.json"

    def govless_votes_blob_name(self, proposal_id):
        return f"data/{self.infra_dao_slug}/votes/eas-atlas/{proposal_id}.ndjson"    
    def govless_hasnt_voted_blob_name(self, proposal_id):
        return f"data/{self.infra_dao_slug}/hasnt_voted/eas-atlas/{proposal_id}.ndjson"

    async def refresh_list(self, gcs_client: 'GCSClient'):

        self.bc.clear_lru()
        self.delegate_metadata = None

        if self.infra_dao_slug == 'optimism':
            mapping = await self.read_govless_proposal_mappings()
        else:
            mapping = {}

        async with httpx.AsyncClient() as http_client:
            chain_id = self.chain_id
            gov_addr = self.gov_addr

            response = await http_client.get(f"https://{self.infra_dao_slug}.prod.agoradata.xyz/v1/progress")
            some_pretty_recent_block = response.json()['block']

            response = await http_client.get(f"https://{self.infra_dao_slug}.prod.agoradata.xyz/v1/proposals")
            proposals = response.json()['proposals']

            anything_changed = False
            skipped_count = 0
            refreshed_count = 0

            for i, proposal_info in enumerate(proposals):
            
                proposal_id = proposal_info['id']

                print(proposal_id)

                OPTIMISM_TEST_PROPOSALS = ['90839767999322802375479087567202389126141447078032129455920633707568400402209',
                                           '28601282374834906210319879956567232553560898502158891728063939287236508034960',
                                           '89934444025525534467725222948723300602129924689317116631018191521555230364343']
                
                if proposal_id in OPTIMISM_TEST_PROPOSALS:
                    continue


                try:
                    blob, existing_liveness, existing_proposal_hash, existing_num_of_votes  = await self.read_existing_raw_proposal_hash_if_exists(proposal_id, gcs_client)
                except SkipProposal as e:
                    print(e)
                    skipped_count += 1
                    continue


                try:
                    response = await http_client.get(f"https://{self.infra_dao_slug}.prod.agoradata.xyz/v1/proposal/{proposal_info['id']}")
                    proposal = response.json()['proposal']
                except Exception as e:
                    print("Proposal fetch failed, we can't proceed, we're blind.  We don't want to corrupt in case of the source pruning.")
                    print(e)
                    skipped_count += 1
                    continue

                hybrid = proposal_id in mapping

                proposal['hybrid'] = hybrid
                
                if hybrid:
                    print("found a hybrid proposal!")
                    govless_proposal_blob_name = self.govless_proposal_blob_name(mapping[proposal_id])
                    proposal['govless_proposal'] = await gcs_client.read_dict(govless_proposal_blob_name)

                    # TODO - any other keys that can be deleted?
                    for key in ['title', 'description']:
                        if key in proposal['govless_proposal']:
                            del proposal['govless_proposal'][key]

                # These are needed for cache busting.
                proposal['after_start_block'] = proposal['start_block'] > some_pretty_recent_block
                proposal['after_end_block'] = proposal['end_block'] > some_pretty_recent_block

                try:
                    proposal_hash = self.check_existing_proposal_hash(proposal, existing_proposal_hash)
                except SkipProposal as e:
                    print(e)
                    skipped_count += 1
                    continue

                # We can do this after for DAO-node, rather than before the cache check in EAS, because DAO-node can count the votes for us.
                # We get it from the DB, rather than DAO-node, because the DB has txn hashes.

                votes = await self.read_votes_from_db(proposal_id)
                num_of_votes = len(votes)            
                proposal['num_of_votes'] = num_of_votes

                # No new votes have come in, we can re-use the last tally
                reuse_tally = existing_num_of_votes > 0 and (existing_num_of_votes == num_of_votes) and (not self.reset)

                start_block = proposal['start_block']
                start_blocktime = await self.get_timestamp(chain_id, start_block)
                proposal['start_blocktime'] = start_blocktime

                if reuse_tally:
                    pass
                else:
                    
                    if self.delegate_metadata is None:
                        self.delegate_metadata = await self.get_delegate_metadata()

                    votes_out = []
                    voter_set = []

                    # First pass: prepare votes and collect addresses
                    votes_data = []
                    for vote in votes:
                        copy_of_vote = copy.deepcopy(dict(vote))
                        copy_of_vote['weight'] = str(int(vote['weight']))

                        voter_set.append(copy_of_vote['voter'])

                        addr = vote['voter'].lower()
                        votes_data.append((copy_of_vote, addr))

                    """
                    # Gather all ENS lookups concurrently
                    async def get_ens_safe(addr):
                        try:
                            ans = await self.bc.get_ens_lru(addr)
                            return ans
                        except:
                            return None

                    ens_results = await asyncio.gather(*[get_ens_safe(addr) for _, addr in votes_data])
                    """

                    # Second pass: update votes with ENS and metadata
                    for copy_of_vote, addr in votes_data:
                        delegate_meta = self.delegate_metadata.get(addr, {})
                        copy_of_vote.update(delegate_meta)

                        votes_out.append(copy_of_vote)

                    voter_set = set(voter_set)

                    if hybrid:
                        govless_votes_blob_name = self.govless_votes_blob_name(mapping[proposal_id])
                        govless_votes = await gcs_client.read_ndjson(govless_votes_blob_name)
                        if govless_votes is None:
                            govless_votes = []
                    else:
                        govless_votes = []

                    await self.overwrite_votes(votes_out + govless_votes, proposal_id, gcs_client)

                proposal['title'] = get_title_from_proposal_description(proposal['description'])

                liveness = 'live'

                anything_changed = True
                refreshed_count += 1

                # This section here, enriches the proposal object, in a way that will only update,
                # if the hash for the proposal's state changes. Downstream consumers can either use it, accepting
                # the caveate, or re-calculate it.

                end_block = proposal['end_block']
                proposal['end_blocktime'] = await self.get_timestamp(chain_id, end_block)


                curtime = int(time.time())

                if curtime > start_blocktime:
                    proposal['total_voting_power_at_start'] = str(await self.read_snapshot_votable_supply(start_block))


                    if self.delegate_metadata is None:
                        self.delegate_metadata = await self.get_delegate_metadata()

                    if not reuse_tally:
                        snapshot_vp = await self.get_vp_snapshot_all_delegates(start_block, gcs_client)

                        snapshot_vp_out = []
                        for row in snapshot_vp:

                            if (row['addr'] not in voter_set) and int(row['vp']) > 0:

                                addr = row['addr'].lower()
                                record = copy.deepcopy(row)

                                delegate_metadata = self.delegate_metadata.get(addr, {})

                                record.update(delegate_metadata)
                                
                                snapshot_vp_out.append(record)                        


                        if hybrid:
                            govless_hasnt_voted_blob_name = self.govless_hasnt_voted_blob_name(mapping[proposal_id])
                            govless_hasnt_voted = await gcs_client.read_ndjson(govless_hasnt_voted_blob_name)
                            if govless_hasnt_voted is None:
                                govless_hasnt_voted = []
                        else:
                            govless_hasnt_voted = []

                        await self.overwrite_hasnt_voted(snapshot_vp_out + govless_hasnt_voted, proposal_id, gcs_client)

                """
                enum ProposalState {
                    Pending,   // 0 # HANDLED
                    Active,    // 1 # HANDLED
                    Canceled,  // 2 # HANDLED
                    Defeated,  // 3 # HANDLED
                    Succeeded, // 4 # Implicitly Handled
                    Queued,    // 5 # HANDLED
                    Expired,   // 6
                    Executed   // 7 # HANDLED
                }"""

                if 'queue_event' in proposal:
                    proposal['queue_event']['timestamp'] = await self.get_timestamp(chain_id, proposal['queue_event']['block_number'])
                    proposal['lifecycle_stage'] = 'QUEUED'

                if 'cancel_event' in proposal:
                    proposal['cancel_event']['timestamp'] = await self.get_timestamp(chain_id, proposal['cancel_event']['block_number'])
                    proposal['lifecycle_stage'] = 'CANCELLED'
                    liveness = 'archived'

                if 'execute_event' in proposal:
                    proposal['execute_event']['timestamp'] = await self.get_timestamp(chain_id, proposal['execute_event']['block_number'])
                    proposal['lifecycle_stage'] = 'EXECUTED'
                    liveness = 'archived'

                # TODO - Figure out how to detect "PASSED", in an automated way without polling. ie, the state of having a succeeded proposal
                #        That can't be queued successfully because, for instance, timelock permissions.
                CYBER_PASSED_PROPOSALS = ['75448677353223676977806688663112177034253117296912078788499719032483591083949',
                                          '112708518748775910354956954757181736583219137500135235919980324756977043503005',
                                          '50911998049910853136464694786245772785831547713228901636780656582528320804232']
                SCROLL_PASSED_PROPOSALS = ['545246063317575466165740766113143372181954164634411861681132968954652703515',
                                            '1247605826408291988137032745109360457689615424039552986782425232092434978933',
                                            '81939631158579841171219988954315753236293867421581097385921335841780903893992',
                                            '115203962227058139384278248635798144936351291766909949005270484145680009554500']
                OPTIMISM_PASSED_PROPOSALS = ['71928632649116715308847337447543955907072794738294227130170691217045092512147']

                # This is a known issue with Optimism, where the proposal is marked as "succeeded" but it's not really succeeded, the onchain stage is 1 (active) 1 block after it ended, but 3 (defeated) at a random block in Oct 2025.
                OPTIMISM_CORRUPTED_PROPOSALS_MARKED_SUCCEEDED_I_GUESS = ['103713749716503028671815481721039004389156473487450783632177114353117435138377',
                                                                         '2808108363564117434228597137832979672586627356483314020876637262618986508713',
                                                                         '29831001453379581627736734765818959389842109811221412662144194715522205098015',
                                                                         '80982553847843251343725022866904947381762263529096361834044805234222094077710',
                                                                         '64930538748268257621925093712454552173772860987977453334165023026835711650357',
                                                                         '51738314696473345172141808043782330430064117614433447104828853768775712054864',
                                                                         '114732572201709734114347859370226754519763657304898989580338326275038680037913', # For some reason, we can't get state for this proposal, as of the block number after the end of the proposal.
                                                                         '27878184270712708211495755831534918916136653803154031118511283847257927730426', 
                                                                         '103606400798595803012644966342403441743733355496979747669804254618774477345292',
                                                                         '32970701904870446614408373011942917680422376755229075190214017021915019093516',
                                                                         '94365805422398770067924881378455503928423439630602149628781926844759467250082',
                                                                         '103695324913424597802389181312722993037601032681914451632412140667432224173014']       

                if proposal['id'] in OPTIMISM_CORRUPTED_PROPOSALS_MARKED_SUCCEEDED_I_GUESS:
                    proposal['lifecycle_stage'] = 'SUCCEEDED'
                    liveness = 'archived'

                if proposal['id'] in CYBER_PASSED_PROPOSALS + SCROLL_PASSED_PROPOSALS + OPTIMISM_PASSED_PROPOSALS:
                    proposal['lifecycle_stage'] = 'PASSED'
                    liveness = 'archived'

                if liveness != 'archived':

                    if proposal['end_blocktime'] < curtime:

                        # if proposal_id == '17736716284166632362836192914377789635944846939193281594888966652732932587143':

                        # The goal of this section here, is to catch and mark the DEFEATED proposals and archive them.
                        # And then, we also have a few unquable proposals, that we manually mark as 'PASSED'.  
                        # The real way to check 'PASSED', is for proposals with invalid transaction call data. See Scroll
                        # We don't really care about any other state, but are happy to help the front-end.



                        encoded_state = await self.bc.contract_call_encoded(chain_id, gov_addr,  proposal['end_block'] + 1, 'state(uint256)', [int(proposal['id'])])
                        stage = encoded_state['result']

                        BLOCK_AROUND_TIME_WHEN_PROPOSALS_STARTED_GETTING_BORKED = 126449975
                        if self.infra_dao_slug == 'optimism' and (proposal['end_block'] <= BLOCK_AROUND_TIME_WHEN_PROPOSALS_STARTED_GETTING_BORKED):

                            SOME_BLOCK_IN_2025 = 142917636

                            encoded_state = await self.bc.contract_call_encoded(chain_id, gov_addr,  SOME_BLOCK_IN_2025, 'state(uint256)', [int(proposal['id'])])
                            fresh_stage = encoded_state['result']

                            if stage == fresh_stage:
                                pass # print(f'MATCH: {stage} for {proposal_id}')
                            else:
                                if stage == '0x0000000000000000000000000000000000000000000000000000000000000004':
                                    proposal['lifecycle_stage'] = 'SUCCEEDED'
                                    liveness = 'archived'

                                elif stage != fresh_stage:
                                    msg = (f"PROBLEM: {stage} vs {fresh_stage} for {proposal_id}")
                                    raise Exception(msg)


                        if stage == '0x0000000000000000000000000000000000000000000000000000000000000004' and proposal['voting_module_name'] in ('optimismtic', 'approval'):
                            proposal['lifecycle_stage'] = 'SUCCEEDED'
                            liveness = 'archived'


                        if stage == '0x0000000000000000000000000000000000000000000000000000000000000003':
                            
                            proposal['lifecycle_stage'] = 'DEFEATED'
                            
                            if (proposal['end_blocktime'] + FIVE_MINUTES_IN_SECONDS) > curtime:
                                liveness = 'archived'
                        
                        # Keep in mind, stage is as of the block right after the end of the voting period -- not latest.  
                        # And, since we archived earlier, if marked Cancelled or Executed, then the only state that it might 
                        # be in, would be queued OR succeeded.

                        # It might have been stage 4, but it could be queued by now.  We don't want to poll, we'll
                        # Just mark it SUCCEEDED, until the QUEUED event flows through.
                        elif stage == '0x0000000000000000000000000000000000000000000000000000000000000004':
                            if proposal.get('lifecycle_stage', None) != 'QUEUED':
                                proposal['lifecycle_stage'] = 'SUCCEEDED'
                        
                        else:
                            raise Exception(f"Unhandled proposal lifecycle stage {stage} vs {fresh_stage} for proposal_id: {proposal_id}")

                    elif proposal['start_blocktime'] < curtime < proposal['end_blocktime']:
                        # We know it's, because if it was cancelled, we would have picked up the CANCEL event.
                        proposal['lifecycle_stage'] = 'ACTIVE'
                    elif curtime < proposal['start_blocktime']:
                        # We know it's, because if it was cancelled, we would have picked up the CANCEL event.
                        proposal['lifecycle_stage'] = 'PENDING'
                    else:
                        raise Exception(f"Unhandled proposal lifecycle stage for proposal_id: {proposal_id}")

                await self.overwrite_proposal(proposal, proposal_hash, liveness, gcs_client)

            if anything_changed:
                await self.refresh_source_list(gcs_client)
                await self.refresh_full_list(gcs_client)

        print (f"Refreshed {refreshed_count} proposals, skipped {skipped_count}")
        return {
            'skipped': skipped_count,
            'refreshed': refreshed_count
        }


