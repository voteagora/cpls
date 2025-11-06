import httpx
from eth_abi import encode
import copy

class BlockNotFound(Exception):
    pass

class BlockCacheClient:
    def __init__(self, base_url, alchemy_api_key):
        self.base_url = base_url
        self.alchemy_api_key = alchemy_api_key
        self.client = httpx.AsyncClient(timeout=30.0)
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
    
    async def get_ens_lru(self, address, chain_id=1):

        cache_key = str([address, chain_id])

        cached_ens = self.cached_ens.get(cache_key, None)

        if cached_ens:
            return cached_ens

        ans = await self.get_ens(address, chain_id)

        self.cached_ens[cache_key] = copy.deepcopy(ans)

        return ans
    
    async def get_decoded_eas(self, chain_id, attestation_id):

        headers = self.headers()
        url = self.base_url + f"/decoded_eas/{chain_id}/attestation/{attestation_id}"
        resp = await self.client.get(url, headers=headers)

        if resp.status_code == 200:
            data = resp.json()
            return data

    async def contract_call(self, chain_id, contract_address, method_signature, block_number, data):
        headers = self.headers()
        url = self.base_url + f"/contract_call/{chain_id}/{contract_address}/{method_signature}"
        payload = {
            "block_number": block_number,
            "data": data
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

        # Call the lower-level contract_call method
        result = await self.contract_call(chain_id, contract_address, method_signature, block_number, data)

        return result
    
    async def votable_supply_at_block_with_oracle(self, chain_id, contract_address, block_number):

        result = await self.contract_call_encoded(chain_id, contract_address, block_number, 'votableSupply(uint256)', [block_number])
        vs = int(result['result'], 16)
        return vs

    async def votable_supply_at_block(self, chain_id, contract_address, block_number):
        result = await self.contract_call_encoded(chain_id, contract_address, block_number, 'votableSupply()', [])
        vs = int(result['result'], 16)

        return vs

    
if __name__ == '__main__':

    # print(encode(['uint256'], [72632202831118589040926236054696792722825676801797403282713292252351025742144]).hex())

    from .config import BLOCKCACHE_URL, ALCHEMY_API_KEY
    import asyncio
    loop = asyncio.get_event_loop()
    cache = BlockCacheClient('http://0.0.0.0:8002', ALCHEMY_API_KEY)
    loop.run_until_complete(cache.contract_call_encoded(7560, '0x58E53131c339aA3cBA35904538eA5948f751050a', 23986826, 'state(uint256)', [72632202831118589040926236054696792722825676801797403282713292252351025742144]))
