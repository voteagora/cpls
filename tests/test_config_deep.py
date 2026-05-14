"""
Tests for config.py: load_tenant_configs (path not found, glob, deployment missing,
error in file), load_tenant_config, and create_http_client.
"""

import pytest
import yaml
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, MagicMock


class TestLoadTenantConfigs:

    def test_returns_empty_when_path_not_exist(self):
        with patch("cpls.config.TENANTS_CONFIG_PATH", Path("/nonexistent/path/xyz")):
            from cpls.config import load_tenant_configs
            result = load_tenant_configs()
            assert result == {}

    def test_loads_valid_yaml_file(self, tmp_path):
        config_data = {
            "schema": "testdao",
            "dao_slug": "TestDAO",
            "index_tenant_prefix": "td",
            "features": {},
            "deployments": {
                "test": {
                    "chain_id": 1,
                    "gov": {"address": "0xGov"},
                    "token": {"address": "0xToken"},
                }
            }
        }
        yaml_file = tmp_path / "testdao.yaml"
        yaml_file.write_text(yaml.dump(config_data))

        with patch("cpls.config.TENANTS_CONFIG_PATH", tmp_path), \
             patch("cpls.config.DEPLOYMENT", "test"):
            from cpls.config import load_tenant_configs
            result = load_tenant_configs()
            assert "testdao" in result
            assert result["testdao"]["schema"] == "testdao"
            assert result["testdao"]["deployment"]["chain_id"] == 1
            assert "deployments" not in result["testdao"]

    def test_skips_yaml_with_missing_deployment(self, tmp_path):
        config_data = {
            "schema": "otherdao",
            "deployments": {
                "prod": {"chain_id": 10}
            }
        }
        yaml_file = tmp_path / "otherdao.yaml"
        yaml_file.write_text(yaml.dump(config_data))

        with patch("cpls.config.TENANTS_CONFIG_PATH", tmp_path), \
             patch("cpls.config.DEPLOYMENT", "staging"):
            from cpls.config import load_tenant_configs
            result = load_tenant_configs()
            assert "otherdao" not in result

    def test_handles_malformed_yaml_gracefully(self, tmp_path):
        yaml_file = tmp_path / "broken.yaml"
        yaml_file.write_text(": invalid: yaml: content: [[[")

        with patch("cpls.config.TENANTS_CONFIG_PATH", tmp_path), \
             patch("cpls.config.DEPLOYMENT", "test"):
            from cpls.config import load_tenant_configs
            result = load_tenant_configs()
            assert "broken" not in result

    def test_loads_multiple_yaml_files(self, tmp_path):
        for name in ["dao_a", "dao_b"]:
            config_data = {
                "schema": name,
                "deployments": {"dev": {"chain_id": 1}}
            }
            (tmp_path / f"{name}.yaml").write_text(yaml.dump(config_data))

        with patch("cpls.config.TENANTS_CONFIG_PATH", tmp_path), \
             patch("cpls.config.DEPLOYMENT", "dev"):
            from cpls.config import load_tenant_configs
            result = load_tenant_configs()
            assert "dao_a" in result
            assert "dao_b" in result


class TestLoadTenantConfig:

    def test_returns_single_config(self, tmp_path):
        config_data = {
            "schema": "myprotocol",
            "deployments": {"prod": {"chain_id": 42}}
        }
        (tmp_path / "myprotocol.yaml").write_text(yaml.dump(config_data))

        with patch("cpls.config.TENANTS_CONFIG_PATH", tmp_path), \
             patch("cpls.config.DEPLOYMENT", "prod"):
            from cpls.config import load_tenant_config
            result = load_tenant_config("myprotocol")
            assert result["schema"] == "myprotocol"
            assert result["deployment"]["chain_id"] == 42


class TestCreateHttpClient:

    def test_creates_async_client(self):
        from cpls.config import create_http_client
        import httpx
        client = create_http_client()
        assert isinstance(client, httpx.AsyncClient)
        assert client.timeout.read == 60
