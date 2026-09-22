"""
Read-only helpers for looking up a proposal's raw blob (metadata + contents) in GCS.

Used by the /proposals UI page. Kept separate from server.py so it can be tested
without importing the server module (which instantiates a GCSClient at import time).
"""

import re
from datetime import datetime
from typing import Dict, List, Optional

# Mirrors the SOURCE class constants in sync_daonode.py, sync_snapshot.py,
# sync_eas_atlas.py and sync_eas_oodao.py. Kept as a plain list here to avoid
# importing those modules (and their Postgres / BlockCache dependencies).
PROPOSAL_SOURCES: List[str] = ["dao_node", "snapshot", "eas-atlas", "eas-oodao"]

# dao_node IDs are decimal uint256 strings; snapshot / EAS IDs are 0x-prefixed hex.
PROPOSAL_ID_PATTERN = re.compile(r"^[A-Za-z0-9_\-]+$")
TENANT_PATTERN = re.compile(r"^[a-z0-9_\-]+$")

# Standard GCS blob attributes worth surfacing. Populated by blob.reload().
_METADATA_ATTRS = [
    "generation",
    "metageneration",
    "size",
    "content_type",
    "content_encoding",
    "cache_control",
    "md5_hash",
    "etag",
    "time_created",
    "updated",
]


def is_valid_tenant(tenant: str) -> bool:
    return bool(TENANT_PATTERN.match(tenant or ""))


def is_valid_proposal_id(proposal_id: str) -> bool:
    return bool(PROPOSAL_ID_PATTERN.match(proposal_id or ""))


def proposal_blob_name(tenant: str, source: str, proposal_id: str) -> str:
    """Same formula as Sync.proposal_blob_name in sync.py."""
    return f"data/{tenant}/proposal/{source}/raw/{proposal_id}.json.gz"


def blob_metadata(blob) -> Dict:
    """
    Extract the standard + custom metadata from a (reloaded) google-cloud-storage Blob
    into a plain, JSON-serialisable dict.
    """
    out: Dict = {}
    for attr in _METADATA_ATTRS:
        value = getattr(blob, attr, None)
        if isinstance(value, datetime):
            value = value.isoformat()
        out[attr] = value
    out["custom_metadata"] = dict(blob.metadata or {})
    return out


async def lookup_proposal(gcs_client, tenant: str, proposal_id: str, sources: Optional[List[str]] = None) -> List[Dict]:
    """
    Probe each source's raw proposal blob for the given tenant + proposal id.

    Returns a list (one entry per source where the blob exists) of:
        {
            "source": str,
            "blob_name": str,
            "metadata": dict,        # see blob_metadata()
            "proposal": dict | None, # decoded JSON body, or None if the body is JSON null
            "error": str | None,     # set if the body could not be read/decoded
        }

    Exceptions from get_blob()/exists()/reload() propagate to the caller; failures
    reading the body are captured per-entry so the metadata can still be shown.
    """
    if sources is None:
        sources = PROPOSAL_SOURCES

    results: List[Dict] = []

    for source in sources:
        name = proposal_blob_name(tenant, source, proposal_id)
        blob = await gcs_client.get_blob(name)

        if not blob.exists():
            continue

        blob.reload()  # fetch latest generation + metadata

        entry: Dict = {
            "source": source,
            "blob_name": name,
            "metadata": blob_metadata(blob),
            "proposal": None,
            "error": None,
        }

        try:
            entry["proposal"] = await gcs_client.read_dict(name)
        except Exception as e:  # read_dict raises rather than returning None
            entry["error"] = str(e)

        results.append(entry)

    return results
