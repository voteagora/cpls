import pytest
from datetime import datetime
from cpls.jobs import Job, JobStatus
from cpls.ui import generate_dashboard_html


class TestGenerateDashboardHtml:

    def _make_job(self, status=JobStatus.COMPLETED, dao="optimism", source="dao_node",
                  error=None, stats=None):
        return Job(
            id="abcdef12-3456-7890-abcd-ef1234567890",
            type="scheduled",
            payload={"infra_dao_slug": dao, "source": source},
            status=status,
            created_at=datetime(2024, 6, 1, 12, 0, 0),
            started_at=datetime(2024, 6, 1, 12, 0, 1),
            completed_at=datetime(2024, 6, 1, 12, 5, 0) if status == JobStatus.COMPLETED else None,
            error=error,
            stats=stats,
        )

    def test_returns_html_string(self):
        html = generate_dashboard_html([], JobStatus)
        assert isinstance(html, str)
        assert "Job Processing Dashboard" in html

    def test_empty_jobs_shows_no_jobs(self):
        html = generate_dashboard_html([], JobStatus)
        assert "No jobs yet" in html

    def test_renders_job_row(self):
        jobs = [self._make_job()]
        html = generate_dashboard_html(jobs, JobStatus)
        assert "abcdef12" in html
        assert "optimism" in html
        assert "dao_node" in html
        assert "COMPLETED" in html or "completed" in html

    def test_status_colors(self):
        for status, color in [
            (JobStatus.PENDING, "#FFA500"),
            (JobStatus.PROCESSING, "#4169E1"),
            (JobStatus.COMPLETED, "#32CD32"),
            (JobStatus.FAILED, "#DC143C"),
        ]:
            job = self._make_job(status=status)
            html = generate_dashboard_html([job], JobStatus)
            assert color in html

    def test_stats_display(self):
        job = self._make_job(stats={"total_refreshed": 10, "total_skipped": 5})
        html = generate_dashboard_html([job], JobStatus)
        assert "10" in html
        assert "5" in html

    def test_error_display(self):
        job = self._make_job(status=JobStatus.FAILED, error="Connection timeout")
        html = generate_dashboard_html([job], JobStatus)
        assert "Connection timeout" in html

    def test_dao_filter_options(self):
        jobs = [
            self._make_job(dao="optimism"),
            self._make_job(dao="ens"),
        ]
        html = generate_dashboard_html(jobs, JobStatus)
        assert '<option value="ens">' in html
        assert '<option value="optimism">' in html

    def test_stat_cards_count(self):
        jobs = [
            self._make_job(status=JobStatus.COMPLETED),
            self._make_job(status=JobStatus.PENDING),
            self._make_job(status=JobStatus.FAILED, error="err"),
        ]
        html = generate_dashboard_html(jobs, JobStatus)
        assert "Total Jobs" in html
        assert "Pending" in html
        assert "Completed" in html
        assert "Failed" in html

    def test_no_stats_shows_dash(self):
        job = self._make_job(stats=None)
        html = generate_dashboard_html([job], JobStatus)
        # Stats columns should show '-' when no stats
        assert ">-<" in html
