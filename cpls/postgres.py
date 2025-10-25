import asyncpg

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
