import requests as req

from ..gcs import GCSClient
from ..config import ENVIRONMENT, SCHEDULER_INTERVAL_MINUTES, ALCHEMY_API_KEY

from .utils import json_hash


class DaoNodeSync:
    def __init__(self, infra_dao_slug: str):
        self.infra_dao_slug = infra_dao_slug

    async def refresh_list(self, gcs_client: GCSClient) -> None:
        reset = False

        config_response = req.get(f"http://{self.infra_dao_slug}.prod.agoradata.xyz/deployment")
        chain_id = config_response.json()['deployment']['chain_id']

        response = req.get(f"http://{self.infra_dao_slug}.prod.agoradata.xyz/v1/proposals")
        proposals = response.json()['proposals']

        anything_changed = False

        for proposal_info in proposals:
            proposal_id = proposal_info['id']
            print(proposal_id)

            blob_name = f"data/{self.infra_dao_slug}/proposal/dao_node/raw/{proposal_id}.json"

            blob = await gcs_client.get_blob(blob_name)

            try:
                exists = blob.exists()
            except Exception as exc:
                print("Existence check failed; skipping to avoid corruption during source pruning.")
                print(exc)
                continue

            if exists:
                try:
                    blob.reload()
                except Exception as exc:
                    print("Reload failed; skipping to avoid corruption during source pruning.")
                    print(exc)
                    continue
                existing_liveness = blob.metadata['liveness']
                existing_proposal_hash = blob.metadata['hash']
            else:
                existing_liveness = 'new'
                existing_proposal_hash = 'no-hash'

            if existing_liveness == 'archived' and not reset:
                print(f"Skipping proposal {proposal_id} on archival state.")
                continue

            try:
                response = req.get(f"http://{self.infra_dao_slug}.prod.agoradata.xyz/v1/proposal/{proposal_info['id']}")
                proposal = response.json()['proposal']
            except Exception as exc:
                print("Proposal fetch failed; skipping to avoid corruption during source pruning.")
                print(exc)
                continue

            proposal_hash = json_hash(proposal) + '1'

            if existing_proposal_hash == proposal_hash and not reset:
                print(f"Skipping proposal {proposal['id']} on content-unchanged state.")
                continue

            liveness = 'live'
            if 'execute_event' in proposal or 'cancel_event' in proposal:
                liveness = 'archived'

            anything_changed = True

            headers = {'alchemy-api-key': ALCHEMY_API_KEY}

            start_block = proposal['start_block']
            start_blocktime = req.get(
                f"https://blacache-production.up.railway.app/exact_blocktime/{chain_id}/{start_block}",
                headers=headers,
            ).text
            proposal['start_blocktime'] = start_blocktime

            end_block = proposal['end_block']
            end_blocktime = req.get(
                f"https://blacache-production.up.railway.app/exact_blocktime/{chain_id}/{end_block}",
                headers=headers,
            ).text
            proposal['end_blocktime'] = end_blocktime

            data_eng_properties = {
                'liveness': liveness,
                'source': 'dao_node',
                'hash': proposal_hash,
            }

            metadata = {'proposal_id': proposal['id']}
            metadata.update(data_eng_properties)

            proposal['data_eng_properties'] = data_eng_properties

            if liveness == 'live':
                if ENVIRONMENT == 'production':
                    max_age = 30 * SCHEDULER_INTERVAL_MINUTES
                else:
                    max_age = 10 * SCHEDULER_INTERVAL_MINUTES
            elif liveness == 'archived':
                if ENVIRONMENT == 'production':
                    max_age = 365 * 24 * 60 * 60
                else:
                    max_age = 2 * 60
            else:
                max_age = SCHEDULER_INTERVAL_MINUTES * 60

            cache_contr = 'public, max-age=' + str(max_age)

            await gcs_client.upload_dict(proposal, blob_name, metadata=metadata, cache_control=cache_contr)

        if anything_changed:
            await self._refresh_collections(gcs_client)

    async def _refresh_collections(self, gcs_client: GCSClient) -> None:
        blobs = await gcs_client.list_blobs(prefix=f"data/{self.infra_dao_slug}/proposal/dao_node/raw/")

        proposal_list = []
        for blob in blobs:
            if not blob.name.endswith('.json'):
                continue

            blob.reload()
            data = await gcs_client.read_dict(blob.name)

            del data['description']
            del data['data_eng_properties']['hash']

            proposal_list.append(data)

        proposal_list.sort(key=lambda item: item['end_block'], reverse=True)

        await gcs_client.upload_ndjson(
            proposal_list,
            f"data/{self.infra_dao_slug}/proposal_list/dao_node/raw.ndjson",
        )

        blobs = await gcs_client.list_blobs(prefix=f"data/{self.infra_dao_slug}/proposal_list/")

        proposal_list = []
        for blob in blobs:
            if not blob.name.endswith('.ndjson'):
                continue

            blob.reload()
            data = await gcs_client.read_ndjson(blob.name)

            proposal_list.extend(data)

        await gcs_client.upload_ndjson(
            proposal_list,
            f"data/{self.infra_dao_slug}/proposal_list/dao_node/raw.ndjson",
        )
