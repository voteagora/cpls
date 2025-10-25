import httpx

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
