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
    # Generate HTML for job list
    job_rows = ""
    for job in jobs:
        status_color = {
            JobStatus.PENDING: "#FFA500",
            JobStatus.PROCESSING: "#4169E1",
            JobStatus.COMPLETED: "#32CD32",
            JobStatus.FAILED: "#DC143C"
        }.get(job.status, "#808080")

        job_rows += f"""
        <tr>
            <td>
                <span class="job-id" title="{job.id}">{job.id[:8]}...</span>
                <button class="copy-btn" onclick="copyToClipboard('{job.id}')" title="Copy full ID">📋</button>
            </td>
            <td>{job.type}</td>
            <td style="color: {status_color}; font-weight: bold;">{job.status}</td>
            <td>{job.created_at.strftime('%Y-%m-%d %H:%M:%S')}</td>
            <td>{job.started_at.strftime('%H:%M:%S') if job.started_at else '-'}</td>
            <td>{job.completed_at.strftime('%H:%M:%S') if job.completed_at else '-'}</td>
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
            .refresh-btn {{
                background-color: #4169E1;
                color: white;
                border: none;
                padding: 10px 20px;
                border-radius: 4px;
                cursor: pointer;
                margin-bottom: 20px;
            }}
            .refresh-btn:hover {{
                background-color: #3151B1;
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
            function refreshPage() {{
                location.reload();
            }}

            // Auto-refresh every 5 seconds
            setInterval(refreshPage, 5000);

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

        <button class="refresh-btn" onclick="refreshPage()">= Refresh Now</button>
        <small style="color: #666;">(Auto-refreshes every 5 seconds)</small>

        <h2>Job List</h2>
        <table>
            <thead>
                <tr>
                    <th>Job ID</th>
                    <th>Type</th>
                    <th>Status</th>
                    <th>Created</th>
                    <th>Started</th>
                    <th>Completed</th>
                    <th>Error</th>
                </tr>
            </thead>
            <tbody>
                {job_rows if job_rows else '<tr><td colspan="7" style="text-align: center;">No jobs yet</td></tr>'}
            </tbody>
        </table>
    </body>
    </html>
    """

    return html