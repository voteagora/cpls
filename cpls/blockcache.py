
import httpx
from eth_abi import encode
import copy
import logging
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
    after_log
)

class BlockNotFound(Exception):
    pass

# Configure retry decorator for HTTP operations
# Retries on: ReadError, ConnectError, TimeoutException, RemoteProtocolError
# Strategy: Exponential backoff starting at 1s, max 10s between attempts
# Max attempts: 3
http_retry = retry(
    retry=retry_if_exception_type((
        httpx.ReadError,
        httpx.ConnectError,
        httpx.TimeoutException,
        httpx.RemoteProtocolError
    )),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    stop=stop_after_attempt(3),
    before_sleep=before_sleep_log(logging.getLogger("cpls.blockcache"), logging.WARNING),
    after=after_log(logging.getLogger("cpls.blockcache"), logging.DEBUG)
)

class BlockCacheClient:
    def __init__(self, base_url, alchemy_api_key, http_client: httpx.AsyncClient = None):
        self.base_url = base_url
        self.alchemy_api_key = alchemy_api_key
        self.client = http_client if http_client is not None else httpx.AsyncClient(timeout=30.0)

        self.logr = logging.getLogger("cpls.blockcache")


        self.cached_ens = {}

    def clear_lru(self):
        self.cached_ens = {}

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

    @http_retry
    async def get_exact_blocktime(self, chain_id, block_number):
        headers = self.headers()
        resp = await self.client.get(self.base_url + f"/exact_blocktime/{chain_id}/{block_number}", headers=headers)
        return self.return_ts(resp.json())

    @http_retry
    async def get_estimated_blocktime(self, chain_id, block_number):
        headers = self.headers()
        resp = await self.client.get(self.base_url + f"/estimated_blocktime/{chain_id}/{block_number}", headers=headers)
        return self.return_ts(resp.json())

    async def get_blocktime(self, chain_id, block_number):
        try:
            return await self.get_exact_blocktime(chain_id, block_number)
        except BlockNotFound:
            return await self.get_estimated_blocktime(chain_id, block_number)

    @http_retry
    async def last_block_before_timestamp(self, chain_id, unixts):
        headers = self.headers()
        url = self.base_url + f"/last_block_before_timestamp/{chain_id}/{unixts}"
        resp = await self.client.get(url, headers=headers)
        data = resp.json()
        return data['block_number']

    @http_retry
    async def get_ens(self, address, chain_id=1):
        headers = self.headers()
        url = self.base_url + f"/ens/{chain_id}/{address}"
        resp = await self.client.get(url, headers=headers)

        if resp.status_code == 404:
            return None
        else:
            data = resp.json()
            return data
    
    async def get_ens_lru(self, address, chain_id=1):

        cache_key = str([address, chain_id])

        cached_ens = self.cached_ens.get(cache_key, None)

        if cached_ens:
            return cached_ens

        ans = await self.get_ens(address, chain_id)

        self.cached_ens[cache_key] = copy.deepcopy(ans)

        return ans
    
    @http_retry
    async def get_decoded_eas(self, chain_id, attestation_id):

        headers = self.headers()
        url = self.base_url + f"/decoded_eas/{chain_id}/attestation/{attestation_id}"
        resp = await self.client.get(url, headers=headers)

        if resp.status_code == 200:
            data = resp.json()
            return data

    @http_retry
    async def contract_call(self, chain_id, contract_address, method_signature, block_number, data):
        headers = self.headers()
        url = self.base_url + f"/contract_call/{chain_id}/{contract_address}"
        payload = {
            "block_number": block_number,
            "data": data,
            "method_signature": method_signature
        }
        resp = await self.client.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        return resp.json()

    async def contract_call_encoded(self, chain_id, contract_address, block_number, method_signature, values):
        
        method_name = method_signature.split("(")[0]

        if method_signature[-2:] != "()":
            method_schema = method_signature.replace(method_name + "(", "")[:-1].split(",")
            encoded_params = encode(method_schema, values)
            # Convert to hex string with 0x prefix
            data = str(encoded_params.hex()).removeprefix("0x")
        else:
            data = ''

        self.logr.info(f"Calling contract {contract_address} with method {method_signature} and data {data} (from {values}) at block {block_number} for chain {chain_id} and address {contract_address}")

        # Call the lower-level contract_call method
        result = await self.contract_call(chain_id, contract_address, method_signature, block_number, data)

        return result
    
    async def votable_supply_at_block_with_oracle(self, contract_address, block_number):

        BLOCK_ON_JAN_18_2024 = 114968612
        as_of_block_number = max([BLOCK_ON_JAN_18_2024, block_number])
        result = await self.contract_call_encoded(10, contract_address, as_of_block_number, 'votableSupply(uint256)', [block_number])
        vs = int(result['result'], 16)
        return vs

    async def votable_supply_at_block(self, chain_id, contract_address, block_number):
        result = await self.contract_call_encoded(chain_id, contract_address, block_number, 'votableSupply()', [])
        vs = int(result['result'], 16)

        return vs

    @http_retry
    async def get_transaction_by_index(self, chain_id, block_number, transaction_index):
        headers = self.headers()
        url = self.base_url + f"/transaction/{chain_id}/{block_number}/{transaction_index}"
        resp = await self.client.get(url, headers=headers)
        resp.raise_for_status()
        return resp.json()

    
if __name__ == '__main__':

    # print(encode(['uint256'], [72632202831118589040926236054696792722825676801797403282713292252351025742144]).hex())

    from .config import BLOCKCACHE_URL, ALCHEMY_API_KEY
    import asyncio
    loop = asyncio.get_event_loop()

    BLOCKCACHE_URL = f"https://blockcache-production.up.railway.app"
    BLOCKCACHE_URL = 'http://0.0.0.0:8002'
    cache = BlockCacheClient(BLOCKCACHE_URL, ALCHEMY_API_KEY)
    # loop.run_until_complete(cache.contract_call_encoded(10, '0xcDF27F107725988f2261Ce2256bDfCdE8B382B10', 143355510, 'votableSupply(uint256)', [143355510]))
    loop.run_until_complete(cache.votable_supply_at_block(10, '0xcDF27F107725988f2261Ce2256bDfCdE8B382B10', 143355510))
