
from .gcs import GCSClient
from .postgres import PostgreSQLClient
from .blockcache import BlockCacheClient

from .config import GCS_BUCKET_NAME, ENVIRONMENT, SCHEDULER_INTERVAL_MINUTES, ALCHEMY_API_KEY, DATABASE_URL, BLOCKCACHE_URL, load_tenant_config

import hashlib
import json
import time

FIVE_MINUTES_IN_SECONDS = 5 * 60

from eth_utils import to_checksum_address

def json_hash(obj, algo="sha256"):
    # Serialize with sorted keys and no whitespace differences
    encoded = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.new(algo, encoded).hexdigest()

def to_eth_address(hex_string: str) -> str:
    """
    Convert a 32-byte hex string literal (e.g. 0x0000...a622279f76ddbed4f2cc986c09244262dba8f4ba)
    to a valid Ethereum address (EIP-55 checksum format).
    """
    # Strip 0x prefix if present
    hex_str = hex_string.lower().removeprefix("0x")

    # The address is always the last 20 bytes (40 hex chars)
    address_hex = hex_str[-40:]

    # Convert to checksum address
    return to_checksum_address("0x" + address_hex)

class SkipProposal(Exception):
    def __init__(self, reason, proposal_id):
        self.reason = reason
        self.proposal_id = proposal_id
    
    def __str__(self):
        return f"[PROP-{self.proposal_id}] {self.reason}"

class Sync:
    def __init__(self, infra_dao_slug, config=None, reset=False):

        self.infra_dao_slug = infra_dao_slug

        if config is None:
            config = load_tenant_config(infra_dao_slug)

        self.config = config
        self.reset = reset

        self.pg = PostgreSQLClient(DATABASE_URL)

        self.bc = BlockCacheClient(BLOCKCACHE_URL, ALCHEMY_API_KEY)
    def calc_cache_control(self, liveness):

        if liveness == 'live':
            if ENVIRONMENT == 'prod':
                max_age = 30 * SCHEDULER_INTERVAL_MINUTES # half a scheduler cycle
            else:
                max_age = 10 * SCHEDULER_INTERVAL_MINUTES # 1/6th of a scheduler cycle - just enough to see if it's working.
            
        elif liveness == 'archived':
            if ENVIRONMENT == 'prod':
                max_age = 365 * 24 * 60 * 60 # 1 year
            else:
                max_age = 2 * 60 # 2 minutes

        else:
            raise Exception(f"Unknown liveness: {liveness}")

        return 'public, max-age=' + str(max_age)
        
    async def refresh_source_list(self, gcs_client: 'GCSClient'):

        blobs = await gcs_client.list_blobs(prefix=f"data/{self.infra_dao_slug}/proposal/{self.SOURCE}/raw/") 

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

        await gcs_client.upload_ndjson(proposal_list, f"data/{self.infra_dao_slug}/proposal_list/{self.SOURCE}/raw.ndjson")
            
    async def refresh_full_list(self, gcs_client: 'GCSClient'):

        blobs = await gcs_client.list_blobs(prefix=f"data/{self.infra_dao_slug}/proposal_list/") 

        proposal_list = []

        for blob in blobs:

            if not blob.name.endswith('.ndjson'):
                continue
            
            blob.reload() # refreshes metadata from server
            data = await gcs_client.read_ndjson(blob.name)

            proposal_list.extend(data)
        
        proposal_list.sort(key=lambda x: int(x['end_blocktime']), reverse=True)

        await gcs_client.upload_ndjson(proposal_list, f"data/{self.infra_dao_slug}/proposal_list.full.ndjson")

    def proposal_blob_name(self, proposal_id):
        return f"data/{self.infra_dao_slug}/proposal/{self.SOURCE}/raw/{proposal_id}.json"
    
    def votes_blob_name(self, proposal_id):
        return f"data/{self.infra_dao_slug}/votes/{proposal_id}.ndjson"
    
    def hasnt_voted_blob_name(self, proposal_id):
        return f"data/{self.infra_dao_slug}/hasnt_voted/{proposal_id}.ndjson"

    def calculate_proposal_hash(self, proposal):
        return json_hash(proposal)
    
    
    def check_existing_proposal_hash(self, proposal, existing_proposal_hash):

        proposal_hash = self.calculate_proposal_hash(proposal)

        if existing_proposal_hash == proposal_hash and not self.reset:
            msg = f"content state is unchanged"
            raise SkipProposal(msg, proposal_id=proposal['id'])

        return proposal_hash

    async def read_existing_raw_proposal_hash_if_exists(self, proposal_id, gcs_client: 'GCSClient'):

        blob_name = self.proposal_blob_name(proposal_id)

        blob = await gcs_client.get_blob(blob_name)
        try:
            exists = blob.exists()
        except Exception as e:
            msg = "Existence check failed, we can't proceed, we're blind.  We don't want to corrupt in case of the source pruning."
            print(e)
            raise SkipProposal(msg, proposal_id=proposal_id)

        if exists:
            try:
                blob.reload()
            except Exception as e:
                msg = "Reload failed, we can't proceed, we're blind.  We don't want to corrupt in case of the source pruning."
                print(e)
                raise SkipProposal(msg, proposal_id=proposal_id)
    
            existing_liveness = blob.metadata['liveness']
            existing_proposal_hash = blob.metadata['hash']
            existing_num_of_votes = int(blob.metadata.get('num_of_votes', 0))
            if existing_proposal_hash is None:
                raise Exception("Proposal hash cannot be None if the proposal exists.  This is a bug.")
        else:                
            existing_liveness = 'new'
            existing_proposal_hash = 'no-hash'
            existing_num_of_votes = 0

        if existing_liveness == 'archived' and not self.reset:
            msg = f"Proposal is in archival state."
            raise SkipProposal(msg, proposal_id=proposal_id)

        return blob, existing_liveness, existing_proposal_hash, existing_num_of_votes
        
    async def get_timestamp(self, chain_id, block_number):
        blocktime = await self.bc.get_blocktime(chain_id, block_number)
        return blocktime
    
    async def overwrite_votes(self, votes, proposal_id, gcs_client: 'GCSClient'):

        blob_name = self.votes_blob_name(proposal_id)

        await gcs_client.upload_ndjson(votes, blob_name)

    async def overwrite_hasnt_voted(self, hasnt_voted, proposal_id, gcs_client: 'GCSClient'):

        blob_name = self.hasnt_voted_blob_name(proposal_id)

        await gcs_client.upload_ndjson(hasnt_voted, blob_name)
    

    async def overwrite_proposal(self, proposal, proposal_hash, liveness, gcs_client: 'GCSClient'):

        if proposal_hash is None:
            raise Exception("Proposal hash cannot be None")

        data_eng_properties = {
            'liveness': liveness,
            'source' : self.SOURCE,
            'hash' : proposal_hash
        }

        metadata = {'proposal_id': proposal['id']}
        metadata.update(data_eng_properties)

        metadata['num_of_votes'] = proposal.get('num_of_votes', 0)

        proposal['data_eng_properties'] = data_eng_properties
    
        cache_contr = self.calc_cache_control(liveness)

        blob_name = self.proposal_blob_name(proposal['id'])
        
        await gcs_client.upload_dict(proposal, blob_name, metadata=metadata, cache_control=cache_contr)

if __name__ == "__main__":

    import asyncio

    from .sync_daonode import DaoNodeSync
    from .sync_eas_atlas import EASAtlasSync
    from .sync_eas_oodao import EASOoDaoSync

    dns = EASOoDaoSync('jeffdao', reset=True)
    # dns = EASAtlasSync('optimism', reset=True)
    # dns = DaoNodeSync('scroll', reset=False)

    gcs_client = GCSClient(GCS_BUCKET_NAME)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(dns.refresh_list(gcs_client))

    loop.close()

    # BlockCacheClient(ALCHEMY_API_KEY).get_blocktime(10, 141699303)
