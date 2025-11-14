import pytest
from cpls.sync import Sync
import cpls.sync as sync_mod

def test_calc_cache_control_dev_live(monkeypatch):
    monkeypatch.setattr(sync_mod, "ENVIRONMENT", "dev")
    monkeypatch.setattr(sync_mod, "SCHEDULER_INTERVAL_MINUTES", 10)
    s = Sync("ens", config={"deployments": {"main": {}}, "schema": "ens"})
    out = s.calc_cache_control("live")
    assert out == "public, max-age=100"

def test_calc_cache_control_prod_archived(monkeypatch):
    monkeypatch.setattr(sync_mod, "ENVIRONMENT", "prod")
    s = Sync("ens", config={"deployments": {"main": {}}, "schema": "ens"})
    out = s.calc_cache_control("archived")
    assert out == "public, max-age=31536000"