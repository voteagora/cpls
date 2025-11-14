import pytest
from cpls.sync_eas_oodao import EASOoDaoSync
from tests.conftest import FakeGCSClient

class StubBC:
    async def last_block_before_timestamp(self, chain_id, ts):
        return 123456
    async def get_ens_lru(self, addr):
        return None
    def clear_lru(self):
        return None

@pytest.mark.asyncio
async def test_eas_oodao_refresh_list_archived():
    cfg = {
        "index_tenant_prefix": "ens",
        "deployment": {
            "oodao": {"address": "0xdao", "chain_id": 1},
            "token": {"address": "0x0000000000000000000000000000000000000000"},
            "chain_id": 1
        },
        "dao_slug": "ens"
    }

    s = EASOoDaoSync("ens", config=cfg, reset=True, http_client=None)
    s.bc = StubBC()

    async def read_proposals_stub():
        return [{
            "transaction_hash": "0xth",
            "dao_id": "0xdao",
            "uid": "0xuid",
            "author": "0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            "chain_id": 1,
            "tags": "gov-proposal",
            "endts": 10,
            "title": "t",
            "startts": 0,
            "description": "d",
            "proposal_id": "ood-1",
            "created_block_number": 1,
            "created_time": 1
        }]

    async def read_deletions_stub():
        return []

    async def read_type_range_stub():
        return {"min_quorum_pct": 0, "max_quorum_pct": 10000, "min_approval_threshold_pct": 0, "max_approval_threshold_pct": 10000}

    async def read_type_stub(proposal_id):
        return ({"eas_uid": "0xA", "class": "STANDARD", "quorum": 1000, "approval_threshold": 1000}, None)

    async def read_votes_stub(proposal_id):
        return [{"voter": "0xabc", "support": "1", "weight": 10}]

    async def read_snapshot_vp(block_number):
        return 1000

    async def get_vp_snapshot_stub(block_number, gcs_client, reset=False):
        return [{"addr": "0xdef", "vp": 1}]

    async def get_delegate_metadata_stub():
        return {}

    s.read_proposals = read_proposals_stub
    s.read_proposal_deletions = read_deletions_stub
    s.read_proposal_type_range = read_type_range_stub
    s.read_proposal_type = read_type_stub
    s.read_votes_from_db = read_votes_stub
    s.read_snapshot_votable_supply = read_snapshot_vp
    s.get_vp_snapshot_all_delegates = get_vp_snapshot_stub
    s.get_delegate_metadata = get_delegate_metadata_stub

    g = FakeGCSClient()
    out = await s.refresh_list(g)
    assert out["refreshed"] == 1
    blob = "data/ens/proposal/eas-oodao/raw/ood-1.json.gz"
    assert blob in g.store
    meta = g.store[blob]["metadata"]
    assert meta["source"] == "eas-oodao"
    assert meta["liveness"] == "archived"