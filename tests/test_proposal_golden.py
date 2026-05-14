"""
Golden-file tests for proposal data stored in GCS (public HTTP, no auth needed).

Each entry in PROPOSALS maps directly to a known (dao_slug, source, proposal_id) —
no cross-source scanning needed.

Generate / refresh fixtures from live GCS:
    pytest tests/test_proposal_golden.py -v --update-fixtures -s

Normal comparison run:
    pytest tests/test_proposal_golden.py -v
"""

import gzip
import json
import urllib.request
import urllib.error

import pytest
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BUCKET_BASE = "https://storage.googleapis.com/cpls-usmr-prd-25q4"
FIXTURES_DIR = Path(__file__).parent / "fixtures" / "proposals"

# Direct (dao_slug, source, proposal_id) mapping — no scanning needed.
# Each tuple is the known location of a specific proposal in GCS.
PROPOSALS = [
    # Optimism — dao_node
    ("optimism", "dao_node", "95125315478676153337636309965804486010918292377915044655013986825087199254978"),
    ("optimism", "dao_node", "28197030874936103651584757576099649781961082558352101632047737121219887503363"),
    ("optimism", "dao_node", "43611390841042156127733279917289923399354155784945103358272334363949369459237"),
    ("optimism", "dao_node", "77379844029098348047245706083901850540159595802129942495264753179306805786028"),
    ("optimism", "dao_node", "104658512477211447238723406913978051219515164565395855005009394415444207632959"),
    ("optimism", "dao_node", "32872683835969469583703720873380428072981331285364097246290907925181946140808"),
    # Optimism — eas-atlas (govless / offchain proposal)
    ("optimism", "eas-atlas", "104254402796183118613790552174556993080165650973960750641671478192868760878324"),
    # ENS — dao_node
    ("ens", "dao_node", "12950686153984121876325788121804936905339482144562527684056466889156345680789"),
    # ENS — snapshot (offchain)
    ("ens", "snapshot", "0x98c65ac02f738ddb430fcd723ea5852a45168550b3daf20f75d5d508ecf28aa1"),
    # Cyber — dao_node
    ("cyber", "dao_node", "51864681802578431055051774372360255768364008881797769364315800242780425500096"),
    # Scroll — dao_node
    ("scroll", "dao_node", "17736716284166632362836192914377789635944846939193281594888966652732932587143"),
]

_FIXTURE_MISSING = object()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _public_url(dao_slug: str, source: str, proposal_id: str) -> str:
    return f"{BUCKET_BASE}/data/{dao_slug}/proposal/{source}/raw/{proposal_id}.json.gz"


def _fixture_path(dao_slug: str, source: str, proposal_id: str) -> Path:
    return FIXTURES_DIR / dao_slug / source / f"{proposal_id}.json"


def _fetch(dao_slug: str, source: str, proposal_id: str) -> dict:
    url = _public_url(dao_slug, source, proposal_id)
    try:
        with urllib.request.urlopen(url) as resp:
            return json.loads(gzip.decompress(resp.read()).decode())
    except urllib.error.HTTPError as e:
        raise AssertionError(
            f"Expected blob not found: {dao_slug}/{source}/{proposal_id} (HTTP {e.code})\n"
            f"URL: {url}"
        ) from e


def _save_fixture(dao_slug: str, source: str, proposal_id: str, data: dict) -> None:
    path = _fixture_path(dao_slug, source, proposal_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True))


def _load_fixture(dao_slug: str, source: str, proposal_id: str):
    path = _fixture_path(dao_slug, source, proposal_id)
    if not path.exists():
        return _FIXTURE_MISSING
    return json.loads(path.read_text())


def _case_id(triple):
    dao, source, pid = triple
    short = pid[:12] + "..." if len(pid) > 15 else pid
    return f"{dao}/{source}/{short}"


# ---------------------------------------------------------------------------
# Golden comparison tests
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("dao_slug,source,proposal_id", PROPOSALS, ids=[_case_id(c) for c in PROPOSALS])
def test_proposal_golden(dao_slug, source, proposal_id, update_fixtures):
    """
    Fetch proposal data via public HTTPS and compare against the stored golden fixture.

    - No fixture file: downloads from GCS, writes fixture, passes.
    - --update-fixtures: overwrites fixture with current GCS data, passes.
    - Fixture exists: strict equality against live GCS data.
    """
    live = _fetch(dao_slug, source, proposal_id)
    fixture = _load_fixture(dao_slug, source, proposal_id)

    if fixture is _FIXTURE_MISSING or update_fixtures:
        action = "Updated" if fixture is not _FIXTURE_MISSING else "Created"
        _save_fixture(dao_slug, source, proposal_id, live)
        print(f"\n  [{action} fixture] {dao_slug}/{source}/{proposal_id[:24]}... → {len(json.dumps(live))} bytes")
        return

    assert live == fixture, (
        f"Proposal data changed in GCS for {dao_slug}/{source}/{proposal_id}.\n"
        f"Run with --update-fixtures to accept the new data as the new baseline."
    )
