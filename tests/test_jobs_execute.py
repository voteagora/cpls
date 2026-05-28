import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from cpls.jobs import Job, JobStatus, JobQueue


class TestExecuteJob:

    @pytest.fixture
    def queue(self):
        with patch("cpls.jobs.create_http_client") as mock_http:
            mock_http.return_value = AsyncMock()
            q = JobQueue(num_workers=1)
            return q

    @pytest.mark.asyncio
    @patch("cpls.jobs.GCSClient")
    @patch("cpls.jobs.DaoNodeSync")
    async def test_execute_dao_node_source(self, MockDaoNodeSync, MockGCSClient, queue):
        mock_sync = MagicMock()
        mock_sync.refresh_list = AsyncMock(return_value={"skipped": 3, "refreshed": 2})
        MockDaoNodeSync.return_value = mock_sync

        job = Job(
            id="test-1",
            type="scheduled",
            payload={
                "infra_dao_slug": "optimism",
                "config": {"some": "config"},
                "sources": ["dao_node"],
                "logic": "refresh_list",
                "reset": False,
            },
            created_at=datetime.now(),
        )

        await queue._execute_job(job)
        MockDaoNodeSync.assert_called_once_with("optimism", {"some": "config"}, False, queue.http_client)
        mock_sync.refresh_list.assert_called_once()
        assert job.stats["total_skipped"] == 3
        assert job.stats["total_refreshed"] == 2

    @pytest.mark.asyncio
    @patch("cpls.jobs.GCSClient")
    @patch("cpls.jobs.SnapshotSync")
    async def test_execute_snapshot_source(self, MockSnapshotSync, MockGCSClient, queue):
        mock_sync = MagicMock()
        mock_sync.refresh_list = AsyncMock(return_value={"skipped": 1, "refreshed": 4})
        MockSnapshotSync.return_value = mock_sync

        job = Job(
            id="test-2",
            type="scheduled",
            payload={
                "infra_dao_slug": "ens",
                "config": {},
                "sources": ["snapshot"],
                "logic": "refresh_list",
                "reset": True,
            },
            created_at=datetime.now(),
        )

        await queue._execute_job(job)
        MockSnapshotSync.assert_called_once_with("ens", {}, True, queue.http_client)
        assert job.stats["total_refreshed"] == 4

    @pytest.mark.asyncio
    @patch("cpls.jobs.GCSClient")
    @patch("cpls.jobs.EASAtlasSync")
    async def test_execute_eas_atlas_source(self, MockEASAtlasSync, MockGCSClient, queue):
        mock_sync = MagicMock()
        mock_sync.refresh_list = AsyncMock(return_value={"skipped": 0, "refreshed": 1})
        MockEASAtlasSync.return_value = mock_sync

        job = Job(
            id="test-3",
            type="scheduled",
            payload={
                "infra_dao_slug": "optimism",
                "config": {},
                "sources": ["eas-atlas"],
                "logic": "refresh_list",
                "reset": False,
            },
            created_at=datetime.now(),
        )

        await queue._execute_job(job)
        MockEASAtlasSync.assert_called_once()
        assert job.stats["total_refreshed"] == 1

    @pytest.mark.asyncio
    @patch("cpls.jobs.GCSClient")
    @patch("cpls.jobs.EASOoDaoSync")
    async def test_execute_eas_oodao_source(self, MockEASOoDaoSync, MockGCSClient, queue):
        mock_sync = MagicMock()
        mock_sync.refresh_list = AsyncMock(return_value={"skipped": 5, "refreshed": 3})
        MockEASOoDaoSync.return_value = mock_sync

        job = Job(
            id="test-4",
            type="scheduled",
            payload={
                "infra_dao_slug": "optimism",
                "config": {},
                "sources": ["eas-oodao"],
                "logic": "refresh_list",
                "reset": False,
            },
            created_at=datetime.now(),
        )

        await queue._execute_job(job)
        MockEASOoDaoSync.assert_called_once()
        assert job.stats["total_skipped"] == 5
        assert job.stats["total_refreshed"] == 3

    @pytest.mark.asyncio
    @patch("cpls.jobs.GCSClient")
    async def test_execute_unknown_source_raises(self, MockGCSClient, queue):
        job = Job(
            id="test-5",
            type="scheduled",
            payload={
                "infra_dao_slug": "optimism",
                "config": {},
                "sources": ["unknown_source"],
                "logic": "refresh_list",
                "reset": False,
            },
            created_at=datetime.now(),
        )

        with pytest.raises(Exception, match="Unknown source: unknown_source"):
            await queue._execute_job(job)

    @pytest.mark.asyncio
    @patch("cpls.jobs.GCSClient")
    @patch("cpls.jobs.DaoNodeSync")
    @patch("cpls.jobs.SnapshotSync")
    async def test_execute_multiple_sources(self, MockSnapshotSync, MockDaoNodeSync, MockGCSClient, queue):
        mock_dn = MagicMock()
        mock_dn.refresh_list = AsyncMock(return_value={"skipped": 2, "refreshed": 3})
        MockDaoNodeSync.return_value = mock_dn

        mock_ss = MagicMock()
        mock_ss.refresh_list = AsyncMock(return_value={"skipped": 1, "refreshed": 5})
        MockSnapshotSync.return_value = mock_ss

        job = Job(
            id="test-6",
            type="scheduled",
            payload={
                "infra_dao_slug": "ens",
                "config": {},
                "sources": ["dao_node", "snapshot"],
                "logic": "refresh_list",
                "reset": False,
            },
            created_at=datetime.now(),
        )

        await queue._execute_job(job)
        assert job.stats["total_skipped"] == 3  # 2 + 1
        assert job.stats["total_refreshed"] == 8  # 3 + 5
        assert "dao_node" in job.stats["by_source"]
        assert "snapshot" in job.stats["by_source"]
