
from typing import TYPE_CHECKING
import requests as req

from gcs import GCSClient
from config import GCS_BUCKET_NAME, ENVIRONMENT, SCHEDULER_INTERVAL_MINUTES

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

        response = req.get(f"http://{self.infra_dao_slug}.prod.agoradata.xyz/v1/proposals")
        proposals = response.json()['proposals']

        for proposal_info in proposals:
            proposal_id = proposal_info['id']
            print(proposal_id)

            blob_name = f"data/{self.infra_dao_slug}/proposal/dao_node/raw/{proposal_id}.json"

            blob = await gcs_client.get_blob(blob_name)

            try:
                blob.reload()
                existing_liveness = blob.metadata['liveness']
                existing_proposal_hash = blob.metadata['hash']
                
            except Exception as e:
                existing_liveness = 'new'
                existing_proposal_hash = 'no-hash'

            if existing_liveness == 'archived':
                print(f"Skipping proposal {proposal_id} on archival state.")
                continue
            
            response = req.get(f"http://{self.infra_dao_slug}.prod.agoradata.xyz/v1/proposal/{proposal_info['id']}")
            proposal = response.json()['proposal']

            liveness = 'live'
            if 'execute_event' in proposal or 'cancel_event' in proposal:
                liveness = 'archived'

            proposal_hash = json_hash(proposal)

            if existing_proposal_hash == proposal_hash:
                print(f"Skipping proposal {proposal['id']} on content-unchanged state.")
                continue

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


if __name__ == "__main__":


    
    dns = DaoNodeSync('scroll')

    gcs_client = GCSClient(GCS_BUCKET_NAME)
    dns.refresh_list(gcs_client)
