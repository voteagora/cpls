import httpx, time
from .gcs import GCSClient
from .sync import Sync, SkipProposal, FIVE_MINUTES_IN_SECONDS

from .title_processor import get_title_from_proposal_description



class DaoNodeSync(Sync):

    SOURCE = 'dao_node'
    
    async def refresh_list(self, gcs_client: 'GCSClient'):

        async with httpx.AsyncClient() as http_client:
            config_response = await http_client.get(f"https://{self.infra_dao_slug}.prod.agoradata.xyz/deployment")
            deployment = config_response.json()['deployment']
            chain_id = deployment['chain_id']
            gov_addr = deployment['gov']['address']

            response = await http_client.get(f"https://{self.infra_dao_slug}.prod.agoradata.xyz/v1/proposals")
            proposals = response.json()['proposals']

            anything_changed = False
            skipped_count = 0
            refreshed_count = 0

            for proposal_info in proposals:
                proposal_id = proposal_info['id']

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

                try:
                    proposal_hash = self.check_existing_proposal_hash(proposal, existing_proposal_hash)
                except SkipProposal as e:
                    print(e)
                    skipped_count += 1
                    continue

                proposal['title'] = get_title_from_proposal_description(proposal['description'])

                liveness = 'live'

                anything_changed = True
                refreshed_count += 1

                # This section here, enriches the proposal object, in a way that will only update,
                # if the hash for the proposal's state changes. Downstream consumers can either use it, accepting
                # the caveate, or re-calculate it.

                start_block = proposal['start_block']
                start_blocktime = await self.get_timestamp(chain_id, start_block)
                proposal['start_blocktime'] = start_blocktime

                proposal['proposer_ens'] = await self.bc.get_ens(proposal['proposer'])

                end_block = proposal['end_block']
                proposal['end_blocktime'] = await self.get_timestamp(chain_id, end_block)

                curtime = time.time()
                
                print(proposal['start_blocktime'], curtime, proposal['end_blocktime'])

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

                # TODO - Figure out how to detect "PASSED", in an automated way without polling.
                CYBER_PASSED_PROPOSALS = ['75448677353223676977806688663112177034253117296912078788499719032483591083949']
                SCROLL_PASSED_PROPOSALS = ['545246063317575466165740766113143372181954164634411861681132968954652703515',
                                            '1247605826408291988137032745109360457689615424039552986782425232092434978933',
                                            '81939631158579841171219988954315753236293867421581097385921335841780903893992',
                                            '115203962227058139384278248635798144936351291766909949005270484145680009554500']

                if proposal['id'] in CYBER_PASSED_PROPOSALS + SCROLL_PASSED_PROPOSALS:
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
                            raise Exception(f"Unhandled proposal lifecycle stage {stage} for proposal_id: {proposal_id}")

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


