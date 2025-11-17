import asyncio
import gzip
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.abspath("."))

class FakeBlob:
    def __init__(self, name, store):
        self.name = name
        self.store = store
        self.metadata = store.get(name, {}).get("metadata", {})

    def exists(self):
        return self.name in self.store

    def reload(self):
        entry = self.store.get(self.name, None)
        if entry:
            self.metadata = entry.get("metadata", {})

class FakeGCSClient:
    def __init__(self):
        self.store = {}
        self.ndjson_uploads = {}
        self.dict_uploads = {}

    async def get_blob(self, blob_name):
        return FakeBlob(blob_name, self.store)

    async def list_blobs(self, prefix):
        return [FakeBlob(name, self.store) for name in list(self.store.keys()) if name.startswith(prefix)]

    async def upload_dict(self, data, blob_name, cache_control=None, metadata=None):
        payload = json.dumps(data)
        self.store[blob_name] = {"data": payload, "metadata": metadata or {}, "cache_control": cache_control}
        self.dict_uploads[blob_name] = {"data": data, "metadata": metadata or {}, "cache_control": cache_control}
        return True

    async def read_dict(self, blob_name):
        entry = self.store.get(blob_name)
        if entry is None:
            raise Exception("not found")
        data = entry["data"]
        if blob_name.endswith(".gz"):
            if isinstance(data, bytes):
                decoded = gzip.decompress(data).decode()
            else:
                decoded = data
        else:
            decoded = data
        return json.loads(decoded)

    async def upload_ndjson(self, data, blob_name, cache_control=None, metadata=None):
        ndjson = "\n".join(json.dumps(item) for item in data)
        if blob_name.endswith(".gz"):
            payload = gzip.compress(ndjson.encode())
        else:
            payload = ndjson.encode()
        self.store[blob_name] = {"data": payload, "metadata": metadata or {}, "cache_control": cache_control}
        self.ndjson_uploads[blob_name] = {"items": data, "metadata": metadata or {}, "cache_control": cache_control}
        return True

    async def read_ndjson(self, blob_name):
        entry = self.store.get(blob_name)
        if entry is None:
            raise Exception("not found")
        data = entry["data"]
        if blob_name.endswith(".gz"):
            decoded = gzip.decompress(data).decode()
        else:
            decoded = data.decode()
        items = []
        for line in decoded.strip().split("\n"):
            if line.strip():
                items.append(json.loads(line))
        return items

class FakeBlockCacheClient:
    def __init__(self, *args, **kwargs):
        pass

    def clear_lru(self):
        return None

class _AsyncConnCtx:
    def __init__(self, conn):
        self.conn = conn
    async def __aenter__(self):
        return self.conn
    async def __aexit__(self, exc_type, exc, tb):
        return False

class FakeConnection:
    def __init__(self, fetch_map=None, fetchrow_map=None):
        self.fetch_map = fetch_map or {}
        self.fetchrow_map = fetchrow_map or {}
    async def fetch(self, qry):
        rows = self.fetch_map.get(qry, [])
        return rows
    async def fetchrow(self, qry):
        return self.fetchrow_map.get(qry, None)

class FakePool:
    def __init__(self, conn):
        self._conn = conn
    async def acquire(self):
        return _AsyncConnCtx(self._conn)

class FakePostgresClient:
    def __init__(self):
        self._fetch_map = {}
        self._fetchrow_map = {}
    def register_fetch(self, qry, rows):
        self._fetch_map[qry] = rows
    def register_fetchrow(self, qry, row):
        self._fetchrow_map[qry] = row
    async def connect(self):
        conn = FakeConnection(self._fetch_map, self._fetchrow_map)
        return FakePool(conn)

def pytest_configure(config):
    return None