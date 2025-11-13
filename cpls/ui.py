"""
UI module for Job Processing Dashboard
"""

from typing import List


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