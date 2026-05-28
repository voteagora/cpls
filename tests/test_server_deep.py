"""
Tests for server.py: scheduled_proposal_job, scheduled_ens_job,
and additional endpoint coverage.
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime


@pytest.fixture
def patched_server():
    """Patch GCSClient and other globals before importing server module."""
    with patch("cpls.gcs.storage"), \
         patch("cpls.gcs.Path"), \
         patch("cpls.config.INFRA_DAO_SLUGS", []):
        from cpls import server
        yield server


class TestScheduledProposalJob:

    @pytest.mark.asyncio
    async def test_oodao_source_added(self, patched_server):
        config = {
            "features": {"oodao": True, "snapshot_proposals": False, "dao_node_proposals": False}
        }
        with patch.object(patched_server.job_queue, "add_job", new_callable=AsyncMock) as mock_add:
            mock_add.return_value = "job-1"
            patched_server.reset_tracker = {"testdao": False}
            await patched_server.scheduled_proposal_job(config, "testdao")
            payload = mock_add.call_args[1]["payload"]
            assert "eas-oodao" in payload["sources"]
            assert "snapshot" not in payload["sources"]
            assert "dao_node" not in payload["sources"]

    @pytest.mark.asyncio
    async def test_snapshot_source_added(self, patched_server):
        config = {
            "features": {"oodao": False, "snapshot_proposals": True, "dao_node_proposals": False}
        }
        with patch.object(patched_server.job_queue, "add_job", new_callable=AsyncMock) as mock_add:
            mock_add.return_value = "job-2"
            patched_server.reset_tracker = {"testdao": False}
            await patched_server.scheduled_proposal_job(config, "testdao")
            payload = mock_add.call_args[1]["payload"]
            assert "snapshot" in payload["sources"]

    @pytest.mark.asyncio
    async def test_dao_node_source_added(self, patched_server):
        config = {
            "features": {"oodao": False, "snapshot_proposals": False, "dao_node_proposals": True}
        }
        with patch.object(patched_server.job_queue, "add_job", new_callable=AsyncMock) as mock_add:
            mock_add.return_value = "job-3"
            patched_server.reset_tracker = {"testdao": False}
            await patched_server.scheduled_proposal_job(config, "testdao")
            payload = mock_add.call_args[1]["payload"]
            assert "dao_node" in payload["sources"]

    @pytest.mark.asyncio
    async def test_optimism_adds_eas_atlas(self, patched_server):
        config = {
            "features": {"oodao": False, "snapshot_proposals": False, "dao_node_proposals": True}
        }
        with patch.object(patched_server.job_queue, "add_job", new_callable=AsyncMock) as mock_add:
            mock_add.return_value = "job-4"
            patched_server.reset_tracker = {"optimism": False}
            await patched_server.scheduled_proposal_job(config, "optimism")
            payload = mock_add.call_args[1]["payload"]
            assert "eas-atlas" in payload["sources"]

    @pytest.mark.asyncio
    async def test_reset_tracker_set_false_after_job(self, patched_server):
        config = {
            "features": {"oodao": False, "snapshot_proposals": False, "dao_node_proposals": False}
        }
        with patch.object(patched_server.job_queue, "add_job", new_callable=AsyncMock) as mock_add:
            mock_add.return_value = "job-5"
            patched_server.reset_tracker = {"testdao": True}
            await patched_server.scheduled_proposal_job(config, "testdao")
            assert patched_server.reset_tracker["testdao"] is False

    @pytest.mark.asyncio
    async def test_all_sources_combined(self, patched_server):
        config = {
            "features": {"oodao": True, "snapshot_proposals": True, "dao_node_proposals": True}
        }
        with patch.object(patched_server.job_queue, "add_job", new_callable=AsyncMock) as mock_add:
            mock_add.return_value = "job-6"
            patched_server.reset_tracker = {"optimism": False}
            await patched_server.scheduled_proposal_job(config, "optimism")
            payload = mock_add.call_args[1]["payload"]
            assert set(payload["sources"]) == {"eas-oodao", "snapshot", "eas-atlas", "dao_node"}


class TestLifespanStartup:

    @pytest.mark.asyncio
    async def test_all_slugs_expands_to_tenant_keys(self):
        """Line 125: 'all' in INFRA_DAO_SLUGS → infra_dao_slugs = list(tenants_config.keys())."""
        tenants = {
            "optimism": {"schema": "optimism", "dao_slug": "Optimism"},
            "ens": {"schema": "ens", "dao_slug": "ENS"},
        }
        with patch("cpls.gcs.storage"), patch("cpls.gcs.Path"), \
             patch("cpls.config.INFRA_DAO_SLUGS", ["all"]), \
             patch("cpls.server.INFRA_DAO_SLUGS", ["all"]), \
             patch("cpls.server.load_tenant_configs", return_value=tenants), \
             patch("cpls.server.scheduler") as mock_sched, \
             patch("cpls.server.job_queue") as mock_jq:

            from cpls import server
            mock_jq.process_jobs = AsyncMock()
            mock_sched.add_job = MagicMock()
            mock_sched.start = MagicMock()

            app_mock = MagicMock()
            async with server.lifespan(app_mock):
                pass

            # scheduler.add_job should be called once per tenant (2 total)
            assert mock_sched.add_job.call_count == 2
            job_ids = [c[1]["id"] for c in mock_sched.add_job.call_args_list]
            assert any("optimism" in jid for jid in job_ids)
            assert any("ens" in jid for jid in job_ids)

    @pytest.mark.asyncio
    async def test_explicit_slugs_uses_infra_dao_slugs(self):
        """Lines 133-137: explicit INFRA_DAO_SLUGS → scheduler.add_job called per slug."""
        tenants = {
            "optimism": {"schema": "optimism", "dao_slug": "Optimism"},
        }
        with patch("cpls.gcs.storage"), patch("cpls.gcs.Path"), \
             patch("cpls.config.INFRA_DAO_SLUGS", ["optimism"]), \
             patch("cpls.server.INFRA_DAO_SLUGS", ["optimism"]), \
             patch("cpls.server.load_tenant_configs", return_value=tenants), \
             patch("cpls.server.scheduler") as mock_sched, \
             patch("cpls.server.job_queue") as mock_jq:

            from cpls import server
            mock_jq.process_jobs = AsyncMock()
            mock_sched.add_job = MagicMock()
            mock_sched.start = MagicMock()

            app_mock = MagicMock()
            async with server.lifespan(app_mock):
                pass

            assert mock_sched.add_job.call_count == 1
            call_kwargs = mock_sched.add_job.call_args[1]
            assert call_kwargs["id"] == "scheduled-proposal-job-optimism"


class TestScheduledEnsJob:

    @pytest.mark.asyncio
    async def test_ens_job_adds_sources(self, patched_server):
        config = {
            "features": {"oodao": False, "snapshot_proposals": True, "dao_node_proposals": True}
        }
        with patch.object(patched_server.job_queue, "add_job", new_callable=AsyncMock) as mock_add:
            mock_add.return_value = "ens-job-1"
            await patched_server.scheduled_ens_job(config, "ens")
            payload = mock_add.call_args[1]["payload"]
            assert payload["logic"] == "refresh_ens"
            assert "snapshot" in payload["sources"]
            assert "dao_node" in payload["sources"]

    @pytest.mark.asyncio
    async def test_ens_job_optimism_adds_eas_atlas(self, patched_server):
        config = {
            "features": {"oodao": True, "snapshot_proposals": False, "dao_node_proposals": False}
        }
        with patch.object(patched_server.job_queue, "add_job", new_callable=AsyncMock) as mock_add:
            mock_add.return_value = "ens-job-2"
            await patched_server.scheduled_ens_job(config, "optimism")
            payload = mock_add.call_args[1]["payload"]
            assert "eas-atlas" in payload["sources"]
            assert "eas-oodao" in payload["sources"]
