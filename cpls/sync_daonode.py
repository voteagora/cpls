import httpx
from .gcs import GCSClient
from .sync import Sync, SkipProposal

from .title_processor import get_title_from_proposal_description


class DaoNodeSync(Sync):

    SOURCE = 'dao_node'
    
    async def refresh_list(self, gcs_client: 'GCSClient'):

        async with httpx.AsyncClient() as http_client:
            config_response = await http_client.get(f"https://{self.infra_dao_slug}.prod.agoradata.xyz/deployment")
            chain_id = config_response.json()['deployment']['chain_id']

            response = await http_client.get(f"https://{self.infra_dao_slug}.prod.agoradata.xyz/v1/proposals")
            proposals = response.json()['proposals']

            anything_changed = False

            for proposal_info in proposals:
                proposal_id = proposal_info['id']

                try:
                    existing_proposal_hash = await self.read_existing_raw_proposal_hash_if_exists(proposal_id, gcs_client)
                except SkipProposal as e:
                    print(e)
                    continue

                try:
                    response = await http_client.get(f"https://{self.infra_dao_slug}.prod.agoradata.xyz/v1/proposal/{proposal_info['id']}")
                    proposal = response.json()['proposal']
                except Exception as e:
                    print("Proposal fetch failed, we can't proceed, we're blind.  We don't want to corrupt in case of the source pruning.")
                    print(e)
                    continue

                try:
                    proposal_hash = self.check_existing_proposal_hash(proposal, existing_proposal_hash)
                except SkipProposal as e:
                    print(e)
                    continue

                proposal['title'] = get_title_from_proposal_description(proposal['description'])

                liveness = 'live'
                if 'execute_event' in proposal or 'cancel_event' in proposal:
                    liveness = 'archived'

                anything_changed = True

                # This section here, enriches the proposal object, in a way that will only update,
                # if the hash for the proposal's state changes. Downstream consumers can either use it, accepting
                # the caveate, or re-calculate it.

                start_block = proposal['start_block']
                start_blocktime = await self.get_timestamp(chain_id, start_block)
                proposal['start_blocktime'] = start_blocktime

                proposal['proposer_ens'] = await self.bc.get_ens(proposal['proposer'])

                end_block = proposal['end_block']
                proposal['end_blocktime'] = await self.get_timestamp(chain_id, end_block)

                if 'cancel_event' in proposal:
                    proposal['cancel_event']['timestamp'] = await self.get_timestamp(chain_id, proposal['cancel_event']['block_number'])

                if 'execute_event' in proposal:
                    proposal['execute_event']['timestamp'] = await self.get_timestamp(chain_id, proposal['execute_event']['block_number'])

                if 'queue_event' in proposal:
                    proposal['queue_event']['timestamp'] = await self.get_timestamp(chain_id, proposal['queue_event']['block_number'])

                await self.overwrite_proposal(proposal, proposal_hash, liveness, gcs_client)

            if anything_changed:
                await self.refresh_source_list(gcs_client)
                await self.refresh_full_list(gcs_client)


