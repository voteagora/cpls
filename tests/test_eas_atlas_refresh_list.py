import pytest
from cpls.sync_eas_atlas import EASAtlasSync
from tests.conftest import FakeGCSClient

class StubBC:
    async def get_decoded_eas(self, chain_id, att_uid):
        return {
            "attestation": {
                "uid": "0x1",
                "id": "atlas-1",
                "description": "Desc",
                "proposer": "0x0000000000000000000000000000000000000001",
                "start_block": 100,
                "data": "ignored"
            },
            "decoded_data": {
                "proposal_type": "STANDARD",
                "start_block": 100
            },
            "schema": {"resolver": "0xresolver"},
            "chain_id": chain_id
        }
    async def get_ens(self, addr):
        return None

@pytest.mark.asyncio
async def test_eas_atlas_refresh_list_archived(monkeypatch):
    cfg = {"deployments": {"main": {}}, "schema": "ens"}
    s = EASAtlasSync("ens", config=cfg, reset=True, http_client=None)
    s.bc = StubBC()

    async def read_votes_stub(proposal_id):
        return [{"voter": "0xabc", "support": 1, "weight": "2", "citizen_type": "USER"}]
    async def read_citizens_stub():
        return [{"addr": "0xdef", "vp": 1, "citizen_type": "USER", "name": "n", "image": "i"}]
    async def read_attestations_stub():
        return ["0xatt"]
    async def read_govless_stub():
        return {"atlas-1"}
    async def get_ts_stub(chain_id, block_number):
        return 100  # force past end to mark archived

    s.read_votes = read_votes_stub
    s.read_citizens = read_citizens_stub
    s.read_proposal_create_attestations = read_attestations_stub
    s.read_govless_proposal_set = read_govless_stub
    s.get_timestamp = get_ts_stub

    g = FakeGCSClient()
    out = await s.refresh_list(g)
    assert out["refreshed"] == 2
    blob = "data/ens/proposal/eas-atlas/raw/atlas-1.json.gz"
    assert blob in g.store
    meta = g.store[blob]["metadata"]
    assert meta["source"] == "eas-atlas"
    assert meta["liveness"] == "archived"