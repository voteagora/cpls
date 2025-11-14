import pytest
from cpls.sync import Sync
from tests.conftest import FakeGCSClient, FakeBlob

@pytest.mark.asyncio
async def test_refresh_source_list_builds_ndjson(monkeypatch):
    class DummySync(Sync):
        SOURCE = "snapshot"
    s = DummySync("ens", config={"deployments": {"main": {}}, "schema": "ens"})
    g = FakeGCSClient()
    blob1 = "data/ens/proposal/snapshot/raw/a.json.gz"
    blob2 = "data/ens/proposal/snapshot/raw/b.json.gz"
    d1 = {
        "id": "a",
        "description": "x",
        "end_blocktime": 2,
        "data_eng_properties": {"hash": "h1", "source": "snapshot", "liveness": "archived"}
    }
    d2 = {
        "id": "b",
        "description": "y",
        "end_blocktime": 3,
        "data_eng_properties": {"hash": "h2", "source": "snapshot", "liveness": "archived"}
    }
    await g.upload_dict(d1, blob1)
    await g.upload_dict(d2, blob2)
    await s.refresh_source_list(g)
    out_blob = "data/ens/proposal_list/snapshot/raw.ndjson.gz"
    assert out_blob in g.ndjson_uploads
    items = g.ndjson_uploads[out_blob]["items"]
    assert [i["id"] for i in items] == ["b", "a"]