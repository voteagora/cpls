import json
import pytest
from datetime import datetime
from pathlib import Path
from cpls.jobs import Job, JobStatus
from cpls.ui import generate_dashboard_html, generate_proposal_html

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "proposals"
DAO_NODE_FIXTURE = (
    FIXTURE_DIR / "optimism" / "dao_node"
    / "104658512477211447238723406913978051219515164565395855005009394415444207632959.json"
)


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


    def test_links_to_proposal_lookup(self):
        html = generate_dashboard_html([], JobStatus)
        assert 'href="/proposals"' in html


class TestGenerateProposalHtml:

    @pytest.fixture
    def proposal(self):
        return json.loads(DAO_NODE_FIXTURE.read_text())

    def _metadata(self, **overrides):
        md = {
            "generation": 1758000000000000,
            "metageneration": 3,
            "size": 4321,
            "content_type": "application/gzip",
            "content_encoding": None,
            "cache_control": "public, max-age=60",
            "md5_hash": "md5hash==",
            "etag": "etag-1",
            "time_created": "2025-07-24T19:28:07+00:00",
            "updated": "2025-08-01T00:00:00+00:00",
            "custom_metadata": {
                "proposal_id": "104658512477211447238723406913978051219515164565395855005009394415444207632959",
                "liveness": "live",
                "source": "dao_node",
                "hash": "2f49492949c2d552833198544bac2ef4629d4622028ae35dafd4af233a7e8092",
                "num_of_votes": "4753",
            },
        }
        md.update(overrides)
        return md

    def _entry(self, proposal, **overrides):
        entry = {
            "source": "dao_node",
            "blob_name": "data/optimism/proposal/dao_node/raw/104658512477211447238723406913978051219515164565395855005009394415444207632959.json.gz",
            "metadata": self._metadata(),
            "proposal": proposal,
            "error": None,
        }
        entry.update(overrides)
        return entry

    def test_blank_form(self):
        html = generate_proposal_html("", "", "", [], tenant_options=["ens", "optimism"])
        assert "Proposal Lookup" in html
        assert 'name="tenant"' in html
        assert 'name="proposal_id"' in html
        assert 'name="source"' in html
        for src in ("dao_node", "snapshot", "eas-atlas", "eas-oodao"):
            assert f'<option value="{src}">{src}</option>' in html
        assert '<option value="ens"></option>' in html
        assert "No proposal found" not in html
        assert 'href="/"' in html

    def test_all_option_excluded_from_datalist(self):
        html = generate_proposal_html("", "", "", [], tenant_options=["all", "ens"])
        assert '<option value="all"></option>' not in html
        assert '<option value="ens"></option>' in html

    def test_not_found_notice(self):
        html = generate_proposal_html("optimism", "999", "", [])
        assert "No proposal found" in html
        assert "<code>optimism</code>" in html
        assert "<code>999</code>" in html
        assert "dao_node, snapshot, eas-atlas, eas-oodao" in html

    def test_not_found_notice_single_source(self):
        html = generate_proposal_html("optimism", "999", "snapshot", [])
        assert "No proposal found" in html
        assert "in sources: snapshot." in html
        assert '<option value="snapshot" selected>' in html

    def test_form_prefilled(self):
        html = generate_proposal_html("optimism", "123", "dao_node", [])
        assert 'value="optimism"' in html
        assert 'value="123"' in html
        assert '<option value="dao_node" selected>' in html

    def test_renders_found_result(self, proposal):
        html = generate_proposal_html("optimism", proposal["id"], "", [self._entry(proposal)])
        # metadata
        assert "Latest blob metadata" in html
        assert "1758000000000000" in html
        assert "2025-08-01T00:00:00+00:00" in html
        assert "4321 bytes" in html
        assert "4.2 KB" in html
        assert "public, max-age=60" in html
        assert "Custom metadata" in html
        assert "live" in html
        assert "2f49492949c2d552833198544bac2ef4629d4622028ae35dafd4af233a7e8092" in html
        # blob name
        assert "data/optimism/proposal/dao_node/raw/" in html
        # proposal details
        assert "Developer Advisory Board Election: Members" in html
        assert "0xe4553b743e74da3424ac51f8c1e586fd43ae226f" in html
        assert "SUCCEEDED" in html
        assert "34810871407579755069545456" in html  # quorum kept as a string
        assert "1753385287 (2025-07-24T19:28:07Z)" in html  # blocktime + UTC
        assert "data_eng_properties.liveness" in html
        assert "<th>totals</th>" in html
        # description + full JSON
        assert "Description" in html
        assert 'class="description"' in html
        assert "Following the" in html
        assert "<details><summary>Full proposal JSON</summary>" in html
        assert "<details><summary>Raw blob metadata JSON</summary>" in html
        assert "Proposal body is null" not in html

    def test_escapes_user_and_blob_content(self):
        evil = {"id": "1", "title": "<script>alert(1)</script>", "description": "<img src=x onerror=alert(2)>"}
        entry = self._entry(evil, blob_name='data/<b>x</b>/raw/1.json.gz')
        entry["metadata"]["custom_metadata"]["hash"] = "<i>h</i>"
        html = generate_proposal_html("<b>tenant</b>", '1" onmouseover="x', "", [entry])
        assert "<script>" not in html
        assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html
        assert "<img src=x" not in html
        assert "&lt;b&gt;tenant&lt;/b&gt;" in html
        assert "<b>tenant</b>" not in html
        assert '1&quot; onmouseover=&quot;x' in html
        assert "<i>h</i>" not in html
        assert "data/<b>x</b>" not in html

    def test_error_banner(self):
        html = generate_proposal_html("optimism", "1", "", [], error="GCS error: boom <x>")
        assert 'class="error"' in html
        assert "GCS error: boom &lt;x&gt;" in html
        assert "No proposal found" not in html

    def test_null_proposal_body(self):
        html = generate_proposal_html("optimism", "1", "", [self._entry(None)])
        assert "Proposal body is null" in html
        assert "Latest blob metadata" in html
        assert "Full proposal JSON" not in html

    def test_per_entry_error_shows_warning_and_metadata(self):
        entry = self._entry(None, error="Failed to read JSON data")
        html = generate_proposal_html("optimism", "1", "", [entry])
        assert 'class="warning"' in html
        assert "Failed to read JSON data" in html
        assert "1758000000000000" in html
        assert "Proposal body is null" not in html

    def test_multiple_sources_render_multiple_sections(self, proposal):
        snap = {"id": "0xabc", "title": "Snap title", "state": "closed", "choices": ["For", "Against"], "scores": [1, 2]}
        entries = [
            self._entry(proposal),
            self._entry(snap, source="snapshot", blob_name="data/optimism/proposal/snapshot/raw/0xabc.json.gz"),
        ]
        html = generate_proposal_html("optimism", "x", "", entries)
        assert html.count('<section class="card result"') == 2
        assert "Source: dao_node" in html
        assert "Source: snapshot" in html
        assert "Snap title" in html
        assert "<th>choices</th>" in html
        assert "&quot;For&quot;" in html

    def test_missing_blocktime_is_tolerated(self):
        entry = self._entry({"id": "1", "title": "t", "start_blocktime": "not-a-number", "end_blocktime": None})
        html = generate_proposal_html("optimism", "1", "", [entry])
        assert "not-a-number" in html
        assert "<th>end_blocktime</th><td>-</td>" in html
