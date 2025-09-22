
import os

import requests as req

from gcs import GCSClient
from config import GCS_BUCKET_NAME, ENVIRONMENT, SCHEDULER_INTERVAL_MINUTES, ALCHEMY_API_KEY

import hashlib
import json

def json_hash(obj, algo="sha256"):
    # Serialize with sorted keys and no whitespace differences
    encoded = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.new(algo, encoded).hexdigest()

class DaoNodeSync:
    def __init__(self, infra_dao_slug):
        self.infra_dao_slug = infra_dao_slug
    
    async def refresh_list(self, gcs_client: 'GCSClient'): 

        RESET = False

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
            except Exception as e:
                print("Existence check failed, we can't proceed, we're blind.  We don't want to corrupt in case of the source pruning.")
                print(e)
                continue

            if exists:
                try:
                    blob.reload()
                except Exception as e:
                    print("Reload failed, we can't proceed, we're blind.  We don't want to corrupt in case of the source pruning.")
                    print(e)
                    continue
                existing_liveness = blob.metadata['liveness']
                existing_proposal_hash = blob.metadata['hash']
            else:                
                existing_liveness = 'new'
                existing_proposal_hash = 'no-hash'

            if existing_liveness == 'archived' and not RESET:
                print(f"Skipping proposal {proposal_id} on archival state.")
                continue
            
            try:
                response = req.get(f"http://{self.infra_dao_slug}.prod.agoradata.xyz/v1/proposal/{proposal_info['id']}")
                proposal = response.json()['proposal']
            except Exception as e:
                print("Proposal fetch failed, we can't proceed, we're blind.  We don't want to corrupt in case of the source pruning.")
                print(e)
                continue

            proposal_hash = json_hash(proposal) + '1'

            if existing_proposal_hash == proposal_hash and not RESET:
                print(f"Skipping proposal {proposal['id']} on content-unchanged state.")
                continue

            liveness = 'live'
            if 'execute_event' in proposal or 'cancel_event' in proposal:
                liveness = 'archived'

            anything_changed = True

            # This section here, enriches the proposal object, in a way that will only update, 
            # if the hash for the proposal's state changes. Downstream consumers can either use it, accepting 
            # the caveate, or re-calculate it.

            headers = {'alchemy-api-key': ALCHEMY_API_KEY}

            start_block = proposal['start_block']
            start_blocktime = req.get(f"https://blacache-production.up.railway.app/exact_blocktime/{chain_id}/{start_block}", headers=headers).text
            proposal['start_blocktime'] = start_blocktime

            end_block = proposal['end_block']
            end_blocktime = req.get(f"https://blacache-production.up.railway.app/exact_blocktime/{chain_id}/{end_block}", headers=headers).text
            proposal['end_blocktime'] = end_blocktime

            data_eng_properties = {
                'liveness': liveness,
                'source' : 'dao_node',
                'hash' : proposal_hash
            }

            metadata = {'proposal_id': proposal['id']}
            metadata.update(data_eng_properties)

            proposal['data_eng_properties'] = data_eng_properties
            
            if liveness == 'live':
                if ENVIRONMENT == 'production':
                    max_age = 30 * SCHEDULER_INTERVAL_MINUTES # half a scheduler cycle
                else:
                    max_age = 10 * SCHEDULER_INTERVAL_MINUTES # 1/6th of a scheduler cycle - just enough to see if it's working.
                
            elif liveness == 'archived':
                if ENVIRONMENT == 'production':
                    max_age = 365 * 24 * 60 * 60 # 1 year
                else:
                    max_age = 2 * 60 # 2 minutes

            cache_contr = 'public, max-age=' + str(max_age)
            
            await gcs_client.upload_dict(proposal, blob_name, metadata=metadata, cache_control=cache_contr)

        if anything_changed:
           
            blobs = await gcs_client.list_blobs(prefix=f"data/{self.infra_dao_slug}/proposal/dao_node/raw/") 

            proposal_list = []

            for blob in blobs:

                if not blob.name.endswith('.json'):
                    continue
                
                blob.reload() # refreshes metadata from server
                data = await gcs_client.read_dict(blob.name)

                del data['description']
                del data['data_eng_properties']['hash']

                proposal_list.append(data)

            proposal_list.sort(key=lambda x: x['end_block'], reverse=True)

            await gcs_client.upload_ndjson(proposal_list, f"data/{self.infra_dao_slug}/proposal_list/dao_node/raw.ndjson")



            blobs = await gcs_client.list_blobs(prefix=f"data/{self.infra_dao_slug}/proposal_list/") 

            proposal_list = []

            for blob in blobs:

                if not blob.name.endswith('.ndjson'):
                    continue
                
                blob.reload() # refreshes metadata from server
                data = await gcs_client.read_ndjson(blob.name)

                proposal_list.extend(data)

            await gcs_client.upload_ndjson(proposal_list, f"data/{self.infra_dao_slug}/proposal_list/dao_node/raw.ndjson")






                



if __name__ == "__main__":


    
    dns = DaoNodeSync('scroll')

    gcs_client = GCSClient(GCS_BUCKET_NAME)
    dns.refresh_list(gcs_client)
