"""
UI module for Job Processing Dashboard
"""

import html as html_lib
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence

from .proposal_lookup import PROPOSAL_SOURCES


def generate_dashboard_html(jobs: List, JobStatus) -> str:
    """
    Generate HTML dashboard for job monitoring

    Args:
        jobs: List of Job objects to display
        JobStatus: Enum class for job statuses

    Returns:
        HTML string for the dashboard
    """
    # Extract unique DAO slugs for filter
    dao_slugs = set()
    for job in jobs:
        slug = job.payload.get('infra_dao_slug')
        if slug:
            dao_slugs.add(slug)
    dao_slugs = sorted(dao_slugs)

    # Generate HTML for job list
    job_rows = ""
    for job in jobs:
        status_color = {
            JobStatus.PENDING: "#FFA500",
            JobStatus.PROCESSING: "#4169E1",
            JobStatus.COMPLETED: "#32CD32",
            JobStatus.FAILED: "#DC143C"
        }.get(job.status, "#808080")

        # Extract stats if available
        refreshed = job.stats.get('total_refreshed', '-') if job.stats else '-'
        skipped = job.stats.get('total_skipped', '-') if job.stats else '-'

        # Format timestamps for JavaScript conversion
        created_at_iso = job.created_at.isoformat() if job.created_at else ''
        started_at_iso = job.started_at.isoformat() if job.started_at else ''
        completed_at_iso = job.completed_at.isoformat() if job.completed_at else ''

        job_rows += f"""
        <tr>
            <td>
                <span class="job-id" title="{job.id}">{job.id[:8]}...</span>
                <button class="copy-btn" onclick="copyToClipboard('{job.id}')" title="Copy full ID">📋</button>
            </td>
            <td>{job.type}</td>
            <td>{job.payload.get('infra_dao_slug', '-')}</td>
            <td>{job.payload.get('source', '-')}</td>
            <td style="color: {status_color}; font-weight: bold;">{job.status}</td>
            <td>{refreshed}</td>
            <td>{skipped}</td>
            <td class="timestamp" data-timestamp="{created_at_iso}">{job.created_at.strftime('%Y-%m-%d %H:%M:%S')}</td>
            <td class="timestamp" data-timestamp="{started_at_iso}">{job.started_at.strftime('%H:%M:%S') if job.started_at else '-'}</td>
            <td class="timestamp" data-timestamp="{completed_at_iso}">{job.completed_at.strftime('%H:%M:%S') if job.completed_at else '-'}</td>
            <td>{job.error if job.error else '-'}</td>
        </tr>
        """

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Job Processing Dashboard</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 20px;
                background-color: #f5f5f5;
            }}
            h1 {{
                color: #333;
            }}
            .stats {{
                display: flex;
                gap: 20px;
                margin-bottom: 20px;
            }}
            .stat-card {{
                background: white;
                padding: 15px;
                border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }}
            .stat-value {{
                font-size: 24px;
                font-weight: bold;
                color: #4169E1;
            }}
            .stat-label {{
                color: #666;
                font-size: 14px;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                background: white;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }}
            th {{
                background-color: #4169E1;
                color: white;
                padding: 12px;
                text-align: left;
            }}
            td {{
                padding: 10px;
                border-bottom: 1px solid #ddd;
            }}
            tr:hover {{
                background-color: #f9f9f9;
            }}
            .filter-container {{
                margin-bottom: 20px;
                display: flex;
                align-items: center;
                gap: 10px;
            }}
            .filter-label {{
                font-weight: bold;
                color: #333;
            }}
            .filter-select {{
                padding: 8px 12px;
                border: 1px solid #ccc;
                border-radius: 4px;
                background-color: white;
                cursor: pointer;
                font-size: 14px;
            }}
            .filter-select:hover {{
                border-color: #4169E1;
            }}
            .copy-btn {{
                background-color: transparent;
                border: 1px solid #ccc;
                padding: 4px 8px;
                margin-left: 8px;
                border-radius: 4px;
                cursor: pointer;
                font-size: 12px;
                transition: all 0.2s;
            }}
            .copy-btn:hover {{
                background-color: #f0f0f0;
                border-color: #4169E1;
            }}
            .copy-btn.copied {{
                background-color: #32CD32;
                border-color: #32CD32;
                color: white;
            }}
            .job-id {{
                font-family: monospace;
            }}
        </style>
        <script>
            function copyToClipboard(text) {{
                // Create a temporary textarea element
                const textarea = document.createElement('textarea');
                textarea.value = text;
                textarea.style.position = 'fixed';
                textarea.style.opacity = '0';
                document.body.appendChild(textarea);

                // Select and copy the text
                textarea.select();
                document.execCommand('copy');

                // Remove the temporary element
                document.body.removeChild(textarea);

                // Find the button that was clicked and provide visual feedback
                event.target.textContent = '✅';
                event.target.classList.add('copied');

                // Reset button after 2 seconds
                setTimeout(() => {{
                    event.target.textContent = '📋';
                    event.target.classList.remove('copied');
                }}, 2000);
            }}

            function filterByDao() {{
                const filterValue = document.getElementById('dao-filter').value;
                const table = document.querySelector('table tbody');
                const rows = table.querySelectorAll('tr');

                rows.forEach(row => {{
                    // Skip the "No jobs yet" row
                    if (row.cells.length < 11) {{
                        return;
                    }}

                    const daoSlugCell = row.cells[2]; // Infra DAO Slug column (0-indexed)
                    const daoSlug = daoSlugCell.textContent.trim();

                    if (filterValue === 'all' || daoSlug === filterValue) {{
                        row.style.display = '';
                    }} else {{
                        row.style.display = 'none';
                    }}
                }});
            }}

            function convertTimestampsToLocalTime() {{
                const timestampElements = document.querySelectorAll('.timestamp');

                timestampElements.forEach(element => {{
                    const isoTimestamp = element.getAttribute('data-timestamp');

                    if (isoTimestamp && isoTimestamp !== '') {{
                        const date = new Date(isoTimestamp);

                        // Check if this is a "created" timestamp (has full date) or just time
                        const isCreatedColumn = element.cellIndex === 7; // Created column

                        if (isCreatedColumn) {{
                            // Format as YYYY-MM-DD HH:MM:SS in local timezone
                            const year = date.getFullYear();
                            const month = String(date.getMonth() + 1).padStart(2, '0');
                            const day = String(date.getDate()).padStart(2, '0');
                            const hours = String(date.getHours()).padStart(2, '0');
                            const minutes = String(date.getMinutes()).padStart(2, '0');
                            const seconds = String(date.getSeconds()).padStart(2, '0');
                            element.textContent = `${{year}}-${{month}}-${{day}} ${{hours}}:${{minutes}}:${{seconds}}`;
                        }} else {{
                            // Format as HH:MM:SS in local timezone
                            const hours = String(date.getHours()).padStart(2, '0');
                            const minutes = String(date.getMinutes()).padStart(2, '0');
                            const seconds = String(date.getSeconds()).padStart(2, '0');
                            element.textContent = `${{hours}}:${{minutes}}:${{seconds}}`;
                        }}
                    }}
                }});
            }}

            // Convert timestamps when page loads
            document.addEventListener('DOMContentLoaded', convertTimestampsToLocalTime);
        </script>
    </head>
    <body>
        <h1>Job Processing Dashboard</h1>
        <p><a href="/proposals">Proposal Lookup</a></p>

        <div class="stats">
            <div class="stat-card">
                <div class="stat-value">{len(jobs)}</div>
                <div class="stat-label">Total Jobs</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{len([j for j in jobs if j.status == JobStatus.PENDING])}</div>
                <div class="stat-label">Pending</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{len([j for j in jobs if j.status == JobStatus.PROCESSING])}</div>
                <div class="stat-label">Processing</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{len([j for j in jobs if j.status == JobStatus.COMPLETED])}</div>
                <div class="stat-label">Completed</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{len([j for j in jobs if j.status == JobStatus.FAILED])}</div>
                <div class="stat-label">Failed</div>
            </div>
        </div>

        <div class="filter-container">
            <span class="filter-label">Filter by DAO:</span>
            <select id="dao-filter" class="filter-select" onchange="filterByDao()">
                <option value="all">All DAOs</option>
                {''.join(f'<option value="{slug}">{slug}</option>' for slug in dao_slugs)}
            </select>
        </div>

        <h2>Job List</h2>
        <table>
            <thead>
                <tr>
                    <th>Job ID</th>
                    <th>Type</th>
                    <th>Infra DAO Slug</th>
                    <th>Source</th>
                    <th>Status</th>
                    <th>Refreshed</th>
                    <th>Skipped</th>
                    <th>Created</th>
                    <th>Started</th>
                    <th>Completed</th>
                    <th>Error</th>
                </tr>
            </thead>
            <tbody>
                {job_rows if job_rows else '<tr><td colspan="11" style="text-align: center;">No jobs yet</td></tr>'}
            </tbody>
        </table>
    </body>
    </html>
    """

    return html

# ---------------------------------------------------------------------------
# Proposal lookup page
# ---------------------------------------------------------------------------

# Keys from the proposal body to surface in the "Proposal details" table, in order.
_PROPOSAL_DETAIL_KEYS = [
    "id",
    "title",
    "proposer",
    "proposer_ens",
    "lifecycle_stage",
    "state",
    "start_block",
    "start_blocktime",
    "end_block",
    "end_blocktime",
    "num_of_votes",
    "quorum",
    "voting_module_name",
    "proposal_type_id",
    "transaction_hash",
]

# Nested / structured keys shown as compact JSON in the details table.
_PROPOSAL_JSON_KEYS = ["totals", "choices", "scores", "proposal_type_info"]

# Standard blob attributes to show in the metadata table, in order.
_BLOB_METADATA_KEYS = [
    "generation",
    "metageneration",
    "updated",
    "time_created",
    "size",
    "content_type",
    "content_encoding",
    "cache_control",
    "md5_hash",
    "etag",
]


def _esc(value) -> str:
    """HTML-escape any value (None -> '-')."""
    if value is None:
        return "-"
    return html_lib.escape(str(value), quote=True)


def _human_size(num_bytes) -> str:
    try:
        n = float(num_bytes)
    except (TypeError, ValueError):
        return ""
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return ""


def _format_blocktime(value) -> str:
    """Render an epoch-seconds value as 'epoch (UTC ISO)'; fall back to the raw value."""
    try:
        ts = datetime.fromtimestamp(int(value), tz=timezone.utc)
        return f"{value} ({ts.strftime('%Y-%m-%dT%H:%M:%SZ')})"
    except (TypeError, ValueError, OverflowError, OSError):
        return str(value)


def _row(label, value_html: str) -> str:
    return f"<tr><th>{_esc(label)}</th><td>{value_html}</td></tr>"


def _kv_table(rows: List[str]) -> str:
    if not rows:
        return '<p class="muted">Nothing to show.</p>'
    return f'<table class="kv">{"".join(rows)}</table>'


def _render_metadata(metadata: Dict) -> str:
    rows = []
    for key in _BLOB_METADATA_KEYS:
        value = metadata.get(key)
        if key == "size" and value is not None:
            human = _human_size(value)
            shown = f"{_esc(value)} bytes" + (f" ({_esc(human)})" if human else "")
        else:
            shown = _esc(value)
        rows.append(_row(key, shown))

    custom = metadata.get("custom_metadata") or {}
    if custom:
        rows.append('<tr class="subhead"><th colspan="2">Custom metadata</th></tr>')
        for key in sorted(custom):
            rows.append(_row(key, _esc(custom[key])))

    return _kv_table(rows)


def _render_proposal_details(proposal: Dict) -> str:
    rows = []
    for key in _PROPOSAL_DETAIL_KEYS:
        if key not in proposal:
            continue
        value = proposal[key]
        if key.endswith("_blocktime") and value is not None:
            shown = _esc(_format_blocktime(value))
        else:
            shown = _esc(value)
        rows.append(_row(key, shown))

    dep = proposal.get("data_eng_properties")
    if isinstance(dep, dict):
        for key in sorted(dep):
            rows.append(_row(f"data_eng_properties.{key}", _esc(dep[key])))

    for key in _PROPOSAL_JSON_KEYS:
        if key in proposal:
            compact = json.dumps(proposal[key], sort_keys=True, default=str)
            rows.append(_row(key, f"<code>{_esc(compact)}</code>"))

    return _kv_table(rows)


def _render_result(entry: Dict) -> str:
    source = entry.get("source", "")
    blob_name = entry.get("blob_name", "")
    metadata = entry.get("metadata") or {}
    proposal = entry.get("proposal")
    error = entry.get("error")

    parts = [
        f'<section class="card result" id="source-{_esc(source)}">',
        f"<h2>Source: {_esc(source)}</h2>",
        f'<p class="blob-name"><code>{_esc(blob_name)}</code></p>',
        "<h3>Latest blob metadata</h3>",
        _render_metadata(metadata),
    ]

    if error:
        parts.append(f'<div class="warning">Could not read proposal body: {_esc(error)}</div>')

    parts.append("<h3>Proposal details</h3>")

    if isinstance(proposal, dict):
        parts.append(_render_proposal_details(proposal))

        description = proposal.get("description")
        if description:
            parts.append("<h3>Description</h3>")
            parts.append(f'<pre class="description">{_esc(description)}</pre>')

        full_json = json.dumps(proposal, indent=2, sort_keys=True, default=str)
        parts.append(
            "<details><summary>Full proposal JSON</summary>"
            f"<pre class=\"json\">{_esc(full_json)}</pre></details>"
        )
    elif proposal is None and not error:
        parts.append('<p class="muted">Proposal body is null (blob exists but contains JSON <code>null</code>).</p>')
    elif not error:
        compact = json.dumps(proposal, indent=2, sort_keys=True, default=str)
        parts.append(f'<pre class="json">{_esc(compact)}</pre>')

    metadata_json = json.dumps(metadata, indent=2, sort_keys=True, default=str)
    parts.append(
        "<details><summary>Raw blob metadata JSON</summary>"
        f"<pre class=\"json\">{_esc(metadata_json)}</pre></details>"
    )

    parts.append("</section>")
    return "\n".join(parts)


def generate_proposal_html(
    tenant: str,
    proposal_id: str,
    source: str,
    results: List[Dict],
    error: Optional[str] = None,
    tenant_options: Sequence[str] = (),
) -> str:
    """
    Generate the HTML page for looking up a proposal's raw blob by tenant + proposal id.

    Args:
        tenant: infra_dao_slug entered by the user ('' for the blank form)
        proposal_id: proposal id entered by the user ('' for the blank form)
        source: selected source filter ('' means all sources)
        results: list of entries from proposal_lookup.lookup_proposal()
        error: optional error message to show in a banner
        tenant_options: known tenant slugs, offered as <datalist> suggestions

    Returns:
        HTML string
    """
    searched = bool(tenant and proposal_id)

    source_options = ['<option value="">All sources</option>']
    for s in PROPOSAL_SOURCES:
        selected = " selected" if s == source else ""
        source_options.append(f'<option value="{_esc(s)}"{selected}>{_esc(s)}</option>')
    source_options_html = "".join(source_options)

    tenant_datalist_html = "".join(
        f'<option value="{_esc(t)}"></option>' for t in tenant_options if t and t != "all"
    )

    banner_html = ""
    if error:
        banner_html = f'<div class="error">{_esc(error)}</div>'
    elif searched and not results:
        probed = source if source else ", ".join(PROPOSAL_SOURCES)
        banner_html = (
            f'<div class="notice">No proposal found for tenant <code>{_esc(tenant)}</code>, '
            f'proposal ID <code>{_esc(proposal_id)}</code> in sources: {_esc(probed)}.</div>'
        )

    results_html = "\n".join(_render_result(entry) for entry in results)

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Proposal Lookup</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 20px;
                background-color: #f5f5f5;
            }}
            h1, h2, h3 {{
                color: #333;
            }}
            h2 {{
                margin-top: 0;
            }}
            h3 {{
                margin-bottom: 8px;
                font-size: 16px;
            }}
            a {{
                color: #4169E1;
            }}
            .card {{
                background: white;
                padding: 15px 20px;
                border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                margin-bottom: 20px;
            }}
            form.lookup {{
                display: flex;
                flex-wrap: wrap;
                gap: 12px;
                align-items: flex-end;
            }}
            form.lookup label {{
                display: flex;
                flex-direction: column;
                font-weight: bold;
                color: #333;
                font-size: 14px;
            }}
            form.lookup input, form.lookup select {{
                margin-top: 4px;
                padding: 8px 12px;
                border: 1px solid #ccc;
                border-radius: 4px;
                font-size: 14px;
                font-weight: normal;
                background: white;
            }}
            form.lookup input:focus, form.lookup select:focus {{
                border-color: #4169E1;
                outline: none;
            }}
            form.lookup input[name="proposal_id"] {{
                width: 520px;
                max-width: 100%;
                font-family: monospace;
            }}
            form.lookup button {{
                padding: 9px 18px;
                background-color: #4169E1;
                color: white;
                border: none;
                border-radius: 4px;
                cursor: pointer;
                font-size: 14px;
            }}
            form.lookup button:hover {{
                background-color: #3050c0;
            }}
            .error, .warning, .notice {{
                padding: 12px 16px;
                border-radius: 6px;
                margin-bottom: 20px;
                word-break: break-word;
            }}
            .error {{
                background: #fdecea;
                border: 1px solid #DC143C;
                color: #8b1a2b;
            }}
            .warning {{
                background: #fff4e0;
                border: 1px solid #FFA500;
                color: #7a4b00;
                margin: 12px 0;
            }}
            .notice {{
                background: #eef2ff;
                border: 1px solid #4169E1;
                color: #223377;
            }}
            .muted {{
                color: #666;
            }}
            table.kv {{
                width: 100%;
                border-collapse: collapse;
                margin-bottom: 12px;
            }}
            table.kv th {{
                text-align: left;
                background-color: #f0f3ff;
                color: #333;
                padding: 8px 10px;
                width: 240px;
                border-bottom: 1px solid #ddd;
                vertical-align: top;
                font-family: monospace;
                font-weight: normal;
            }}
            table.kv td {{
                padding: 8px 10px;
                border-bottom: 1px solid #ddd;
                word-break: break-all;
            }}
            table.kv tr.subhead th {{
                background-color: #4169E1;
                color: white;
                font-family: Arial, sans-serif;
                font-weight: bold;
            }}
            code {{
                font-family: monospace;
                word-break: break-all;
            }}
            .blob-name {{
                color: #666;
                font-size: 13px;
            }}
            pre {{
                background: #f8f8f8;
                border: 1px solid #ddd;
                border-radius: 4px;
                padding: 12px;
                overflow: auto;
                font-size: 13px;
            }}
            pre.description {{
                white-space: pre-wrap;
                word-break: break-word;
                max-height: 400px;
            }}
            pre.json {{
                max-height: 600px;
            }}
            details {{
                margin: 10px 0;
            }}
            details summary {{
                cursor: pointer;
                color: #4169E1;
                font-weight: bold;
            }}
        </style>
    </head>
    <body>
        <h1>Proposal Lookup</h1>
        <p><a href="/">&larr; Back to dashboard</a></p>

        <div class="card">
            <form class="lookup" method="get" action="/proposals">
                <label>Tenant (infra_dao_slug)
                    <input type="text" name="tenant" list="tenant-options" value="{_esc(tenant)}" placeholder="optimism" required>
                    <datalist id="tenant-options">{tenant_datalist_html}</datalist>
                </label>
                <label>Proposal ID
                    <input type="text" name="proposal_id" value="{_esc(proposal_id)}" placeholder="uint256 or 0x… hash" required>
                </label>
                <label>Source
                    <select name="source">{source_options_html}</select>
                </label>
                <button type="submit">Look up</button>
            </form>
        </div>

        {banner_html}

        {results_html}
    </body>
    </html>
    """

    return html
