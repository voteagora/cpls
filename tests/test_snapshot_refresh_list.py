import pytest
from cpls.sync_snapshot import SnapshotSync
import cpls.blockcache as bc_mod
from tests.conftest import FakeGCSClient, FakeBlockCacheClient

class StubSnapshotGraphQL:
    def __init__(self, *args, **kwargs):
        pass
    async def get_proposals(self):
        return [
            {
                "id": "p1",
                "body": "b",
                "start": 1000,
                "end": 1100,
                "votes": 3,
                "type": "simple",
                "state": "closed"
            }
        ]

@pytest.mark.asyncio
async def test_refresh_list_archived(monkeypatch):
    monkeypatch.setattr(bc_mod, "BlockCacheClient", FakeBlockCacheClient)
    cfg = {
        "index_tenant_prefix": "ens",
        "deployment": {"token": {"address": "0x0000000000000000000000000000000000000000"}},
        "dao_slug": "ens"
    }
    s = SnapshotSync("ens", config=cfg, reset=True, http_client=None)
    s.sc = StubSnapshotGraphQL()
    g = FakeGCSClient()
    out = await s.refresh_list(g)
    assert out["refreshed"] == 1
    blob = f"data/ens/proposal/snapshot/raw/p1.json.gz"
    assert blob in g.store
    meta = g.store[blob]["metadata"]
    assert meta["liveness"] == "archived"
    assert meta["source"] == "snapshot"