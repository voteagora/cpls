
# Current proflems with existing EAS
# - based off blocks, not actual time
# - end block not in schema, so we need to upgrade the schema

from collections import defaultdict
import time

import httpx
import asyncpg

from .gcs import GCSClient
from .config import GCS_BUCKET_NAME, ENVIRONMENT, SCHEDULER_INTERVAL_MINUTES, ALCHEMY_API_KEY, DATABASE_URL, BLOCKCACHE_URL

import hashlib
import json

from pprint import pprint
from eth_utils import to_checksum_address
from .title_processor import get_title_from_proposal_description

FIVE_MINUTES_IN_SECONDS = 5 * 60 * 60

class PostgreSQLClient:
    def __init__(self, database_url):
        self.database_url = database_url
        self.pool = None

    async def connect(self):
        if not self.pool:
            self.pool = await asyncpg.create_pool(self.database_url, min_size=1, max_size=10)
        return self.pool

    async def disconnect(self):
        if self.pool:
            await self.pool.close()
            self.pool = None

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

class BlockNotFound(Exception):
    pass

class BlockCacheClient:
    def __init__(self, base_url, alchemy_api_key):
        self.base_url = base_url
        self.alchemy_api_key = alchemy_api_key
        self.client = httpx.AsyncClient()

    def headers(self):
        return {'alchemy-api-key': self.alchemy_api_key} 
    
    def return_ts(self, resp):

        ts = resp.get('ts', None)
        
        if ts:
            return ts
        
        msg = resp.get('msg', None)

        if msg == 'block not found':
            raise BlockNotFound()
        
        raise Exception("Unhandled client response")

    async def get_exact_blocktime(self, chain_id, block_number):
        headers = self.headers()
        resp = await self.client.get(self.base_url + f"/exact_blocktime/{chain_id}/{block_number}", headers=headers)
        return self.return_ts(resp.json())

    async def get_estimated_blocktime(self, chain_id, block_number):
        headers = self.headers()
        resp = await self.client.get(self.base_url + f"/estimated_blocktime/{chain_id}/{block_number}", headers=headers)
        return self.return_ts(resp.json())

    async def get_blocktime(self, chain_id, block_number):
        try:
            return await self.get_exact_blocktime(chain_id, block_number)
        except BlockNotFound:
            return await self.get_estimated_blocktime(chain_id, block_number)

    async def last_block_before_timestamp(self, chain_id, unixts):
        headers = self.headers()
        url = self.base_url + f"/last_block_before_timestamp/{chain_id}/{unixts}"
        resp = await self.client.get(url, headers=headers)
        data = resp.json()
        return data['block_number']

    async def get_ens(self, address, chain_id=1):
        headers = self.headers()
        url = self.base_url + f"/ens/{chain_id}/{address}"
        resp = await self.client.get(url, headers=headers)

        if resp.status_code == 404:
            return None
        else:
            data = resp.json()
            return data

class Sync:
    def __init__(self, infra_dao_slug, reset=False):

        self.infra_dao_slug = infra_dao_slug
        self.reset = reset

        self.pg = PostgreSQLClient(DATABASE_URL)

        self.bc = BlockCacheClient(BLOCKCACHE_URL, ALCHEMY_API_KEY)
    def calc_cache_control(self, liveness):

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

    def calculate_proposal_hash(self, proposal):
        return json_hash(proposal)
    
    def check_existing_proposal_hash(self, proposal, existing_proposal_hash):

        proposal_hash = self.calculate_proposal_hash(proposal)

        if existing_proposal_hash == proposal_hash and not self.reset:
            msg = f"content state is unchanged"
            raise SkipProposal(msg, proposal_id=proposal['id'])

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
        else:                
            existing_liveness = 'new'
            existing_proposal_hash = 'no-hash'

        if existing_liveness == 'archived' and not self.reset:
            msg = f"Proposal is in archival state."
            raise SkipProposal(msg, proposal_id=proposal_id)

        return blob, existing_liveness, existing_proposal_hash
        
    async def get_timestamp(self, chain_id, block_number):
        blocktime = await self.bc.get_blocktime(chain_id, block_number)
        return blocktime
    
    async def overwrite_proposal(self, proposal, proposal_hash, liveness, gcs_client: 'GCSClient'):

        data_eng_properties = {
            'liveness': liveness,
            'source' : self.SOURCE,
            'hash' : proposal_hash
        }

        metadata = {'proposal_id': proposal['id']}
        metadata.update(data_eng_properties)

        proposal['data_eng_properties'] = data_eng_properties
    
        cache_contr = self.calc_cache_control(liveness)

        blob_name = self.proposal_blob_name(proposal['id'])
        
        await gcs_client.upload_dict(proposal, blob_name, metadata=metadata, cache_control=cache_contr)


class EASAtlasSync(Sync):

    SOURCE = 'eas-atlas'

    async def read_votes(self, proposal_id):
        pool = await self.pg.connect()
        async with pool.acquire() as connection:
            rows = await connection.fetch(f"""select * from atlas."OffChainVote" ocv where "proposalId" = '{proposal_id}';""")
            return rows

    async def refresh_list(self, gcs_client: 'GCSClient'):


        ######################################
        # Step 1 - Get a list of recent-ish proposals.  Think either the "full list of any proposal ever" OR "just stuff that may or may not be ready to archive"
        #

        # THIS IS TOTALLY FLAWED LOGIC, the point is to make a file format that can be consumed as a second source.
        known_create_attestations = {}
        easa = """0xc89066cf84cc86c3cbb9cc148dbf7514a1b897ad5dbaf878716f6beee89fd6ff
                0xe73cdaca33221711fddfe6c7302b0f1d1d9bf093a9d04643710890d74b865fec
                0xffeefe5d1263a0b1275e407c4c8ecbf87031d7bb2f25b5a9911b14eacee974bc
                0x46e273e2820a4254c6d3b79cf101d82dac8a25abb4eb0e8dd940c4758553fac0
                0xd70f9590ca82e2d263d95ba72a76c9bc9e2a6007c5c906a699794776a2f47d4d
                0x0701b609c2904f1b05bdaa38ab898ad74478c3a3eff4e0691a60509090ef2af2
                0x42247c00390396ad0d598e3ec39bc349c6a3f17fe44bcf574707a904960c127e""".split("\n")
        known_create_attestations['optimism'] = [e.strip() for e in easa]
        # End of flawed logic

        headers = {'alchemy-api-key': ALCHEMY_API_KEY}

        anything_changed = False
        for proposals_uid in known_create_attestations[self.infra_dao_slug]:

            for chain_id in [10, 1]:

                url = f"https://blacache-production.up.railway.app/decoded_eas/{chain_id}/attestation/{proposals_uid}"
                async with httpx.AsyncClient() as client:
                    response = await client.get(url, headers=headers)

                if response.status_code != 200:
                    print(f"Failed to fetch proposal {proposals_uid}")
                    continue

                proposal_attestation = response.json()

                if proposal_attestation['attestation']['uid'] == '0x0000000000000000000000000000000000000000000000000000000000000000':
                    print(f"Failed to fetch proposal {proposals_uid}")
                    continue

                proposal = proposal_attestation['attestation']
                proposal['chain_id'] = proposal_attestation['chain_id']
                proposal.update(proposal_attestation['decoded_data'])
                del proposal['data']
                proposal['resolver'] = proposal_attestation['schema']['resolver']

                proposal_type = proposal['proposal_type']

                proposal['id'] = str(proposal['id'])

                proposal_id = proposal['id']

                try:
                    existing_proposal_hash = await self.read_existing_raw_proposal_hash_if_exists(proposal_id, gcs_client)
                except SkipProposal as e:
                    print(e)
                    continue

                proposal['title'] = get_title_from_proposal_description(proposal['description'])
                proposal['proposer_ens'] = await self.bc.get_ens(proposal['proposer'])

                votes = await self.read_votes(proposal_id)

                

                if proposal_type in ('OPTIMISTIC', 'STANDARD'):

                    outcome = defaultdict(lambda: defaultdict(int))

                    for vote in votes:
                        support = vote['vote']
                        support = json.loads(support)
                        for elem in support:
                            outcome[vote['citizenCategory']][elem] += 1

                elif proposal_type == 'APPROVAL': 

                    outcome = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
                   
                    for vote in votes:
                        support = vote['vote']
                        options, supports =json.loads(support)
                        
                        for option, support in zip(options, supports):
                            outcome[vote['citizenCategory']][option][support] += 1

                proposal['outcome'] = outcome
            
                try:
                    proposal_hash = self.check_existing_proposal_hash(proposal, existing_proposal_hash)
                except SkipProposal as e:
                    print(e)
                    continue


                # This section here, enriches the proposal object, in a way that will only update,
                # if the hash for the proposal's state changes. Downstream consumers can either use it, accepting
                # the caveate, or re-calculate it.

                start_block = proposal['start_block']
                start_blocktime = await self.get_timestamp(chain_id, start_block)
                proposal['start_blocktime'] = start_blocktime

                end_block = start_block + 259200 # TODO - the EAS Attestation needs the end-block timestamp.

                proposal['end_block'] = end_block
                end_blocktime = await self.get_timestamp(chain_id, end_block)
                proposal['end_blocktime'] = end_blocktime

                liveness = 'live'

                cur_time = int(time.time())                

                if cur_time > proposal['end_blocktime'] + FIVE_MINUTES_IN_SECONDS:
                    liveness = 'archived'

                anything_changed = True

                await self.overwrite_proposal(proposal, proposal_hash, liveness, gcs_client)

            if anything_changed:
                await self.refresh_source_list(gcs_client)
                await self.refresh_full_list(gcs_client)

class EASOoDaoSync(Sync):

    SOURCE = 'eas-oodao'

    async def read_votes(self, proposal_id):
        pool = await self.pg.connect()
        async with pool.acquire() as connection:
            qry = f"""select * from auazure."eas_attestations_v2" ocv WHERE topic3 in ('0xffcc8fe77f55448bee5f0e24844ee76f83c3c2718dcf8a75de750cf4797ad0bc', '0x04cb5678af613212e584cf8d117ee3fcd038a9ab657ecf0c596cabe1e6ebd9f0') and decoded_attestation->'proposal_id' = '{proposal_id}';"""
            print(qry)
            rows = await connection.fetch(qry)
            return rows
    
    async def read_proposals(self):
        pool = await self.pg.connect()
        async with pool.acquire() as connection:
            rows = await connection.fetch(f"""select 
                                                transaction_hash,
                                                topic1 as dao_id,
                                                data as uid,
                                                topic2 as author,
                                                chain_id,
                                                decoded_attestation->>'tags' as tags,
                                                decoded_attestation->'endts' as endts,
                                                decoded_attestation->>'title' as title,
                                                decoded_attestation->'startts' as startts,
                                                decoded_attestation->>'description' as description,
                                                decoded_attestation->'proposal_id' as proposal_id
                                                from auazure."eas_attestations_v2" ocp WHERE topic3 = '0x12e8600c9bb57b5b436fa09735cfc63e95098552122001c465b610261eea8a93';""")
            return rows

    async def refresh_list(self, gcs_client: 'GCSClient'):


        ######################################
        # Step 1 - Get a list of recent-ish proposals.  Think either the "full list of any proposal ever" OR "just stuff that may or may not be ready to archive"
        #

        proposals = await self.read_proposals()

        anything_changed = False

        for proposal_meta in proposals:

            proposal = dict(proposal_meta)

            proposal_id = proposal_meta['proposal_id']

            proposal['id'] = proposal_id

            proposal_type = proposal.get('proposal_type', None)

            if proposal_type is None:
                proposal_type_name = 'UNSET'
            else:
                proposal_type_name = proposal_type.get('name')
            
            proposal['proposer'] = to_eth_address(proposal_meta['author'])
            proposal['proposer_ens'] = await self.bc.get_ens(proposal['proposer'])
            del proposal['author']

            try:
                existing_proposal_hash = await self.read_existing_raw_proposal_hash_if_exists(proposal_id, gcs_client)
            except SkipProposal as e:
                print(e)
                continue

            votes = await self.read_votes(proposal_id)
            
            if proposal_type_name in ('UNSET', 'OPTIMISTIC', 'STANDARD'):

                outcome = defaultdict(lambda: defaultdict(int))

                for vote in votes:
                    vote = json.loads(vote['decoded_attestation']) #vote['decoded_attestation']
                    print(vote)
                    choice = vote['choice']
                    outcome['token-holders'][choice] += 1 * 1000000000000000000 # TODO, bring in actual VP
            
                for key in outcome['token-holders'].keys():
                    outcome['token-holders'][key] = str(outcome['token-holders'][key])

            elif proposal_type == 'APPROVAL': 
                raise NotImplementedError("Approval Types are Not implemented yet.")

            proposal['outcome'] = outcome
            proposal['tags'] = proposal['tags'].split(',')
        
            try:
                proposal_hash = self.check_existing_proposal_hash(proposal, existing_proposal_hash)
            except SkipProposal as e:
                print(e)
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

            proposal['start_blocktime'] = endts
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

            await self.overwrite_proposal(proposal, proposal_hash, liveness, gcs_client)

        if anything_changed:
            await self.refresh_source_list(gcs_client)
            await self.refresh_full_list(gcs_client)


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



if __name__ == "__main__":

    import asyncio

    # dns = EASOoDaoSync('jeffdao', reset=True)
    # dns = EASAtlasSync('optimism', reset=True)
    dns = DaoNodeSync('cyber', reset=True)

    gcs_client = GCSClient(GCS_BUCKET_NAME)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(dns.refresh_list(gcs_client))

    loop.close()

    # BlockCacheClient(ALCHEMY_API_KEY).get_blocktime(10, 141699303)
