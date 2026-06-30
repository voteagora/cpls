import hashlib
import json


def json_hash(obj, algo: str = "sha256") -> str:
    """Generate a stable hash for arbitrary JSON-serialisable objects."""
    encoded = json.dumps(obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.new(algo, encoded).hexdigest()
