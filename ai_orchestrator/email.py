import os
from datetime import datetime
from typing import Dict, Any, List

def generate_and_print_email_vars(
    plan: Dict[str, Any],
    summary: Dict[str, Any],
    apm_rows: List[Dict[str, Any]],
    output_dir: str,
    actual_start_time: datetime | None,
    actual_end_time: datetime | None,
    has_waves_run: bool,
    report_links=None
) -> None:
    """
    Generates an HTML email summary of the patching process and prints variables
    for GitHub Actions.
    """
    meta = plan["metadata"]
    email_cfg = plan.get("integrations", {}).get("communications", {}).get("email", {})
    aap_cfg = plan["integrations"]["aap"]
    snow_cfg = plan["integrations"].get("servicenow", {})

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    email_html_path = os.path.join(output_dir, f"CHG{meta['change_id']}_email.html")

    # AAP Template url
    aap_url = aap_cfg.get("controller_url", "")
    job_template_id = aap_cfg.get("job_template_id", "")
    aap_template_url = f"{aap_url}/#/templates/job_templates/{job_template_id}/jobs"

    # ServiceNow Change URL
    snow_instance = snow_cfg.get("instance_url", "")
    chg_no = meta.get("change_id", "")
    servicenow_change_url = f"{snow_instance}/change_request.do?sysparam_query=number={chg_no}"

    # Github Run Id
    github_run_id = os.getenv("GITHUB_RUN_ID", "")
    github_repo = os.getenv("GITHUB_REPOSITORY", "")
    github_url = ""
    if github_run_id and github_repo:
        github_url = f"https://github.com/{github_repo}/actions/runs/{github_run_id}"

    support_email = email_cfg.get("support_email", "")
    patch_url = report_links.get("patch_report") if report_links else None
    apm_url = report_links.get("apm_report") if report_links else None
    patch_link_html = f'<a href="{patch_url}">Patch Report</a>' if patch_url else "<i>Patch Report Not Available</i>"
    apm_link_html = f'<a href="{apm_url}">APM Report</a>' if apm_url else "<i>APM Report Not Available</i>"
    nr_reporting = 0

    if has_waves_run:
        counts = summary.get("counts", {})
        total = int(counts.get("TOTAL", 0))
        success = int(counts.get("SUCCESS", 0))
        failed = int(counts.get("FAILED", 0))
        apm_total = len(apm_rows)
        apm_green = sum(1 for r in apm_rows if str(r.get("apm_post_alert", "")).lower() in ["green", "ok", "not_alerting"])
        apm_red = sum(1 for r in apm_rows if str(r.get("apm_post_alert", "")).lower() in ["red", "critical", "alerting"])
        nr_reporting = sum(1 for r in apm_rows if r.get("infra_post_reporting") is True)
        formatted_start = actual_start_time.strftime("%m/%d/%Y %I:%M %p") if actual_start_time else "N/A"
        formatted_end = actual_end_time.strftime("%m/%d/%Y %I:%M %p") if actual_end_time else "N/A"
        html_content = f"""
    <html>
    <body style="font-family: Arial, sans-serif; font-size: 14px; color: #333;">
    <!-- ===================== HEADER ===================== -->
    <h3 style="color:#2F5597;">Summary of Maintenance</h3>
    <p>
        Based on <a href="{servicenow_change_url}"><b>{meta['change_id']}</b></a>, AI-assisted Linux OS patching has been completed successfully.
        Change Started at {formatted_start} and ended at {formatted_end}
    </p>
    <!-- ===================== PATCHING STATUS ===================== -->
    <h3 style="color:#2F5597; margin-top:20px;">Patching Status</h3>
    <table style="border-collapse: collapse; width: 60%; border: 1px solid #ccc;">
      <tr style="background-color: #2F5597; color: white;">
        <th style="padding: 8px; text-align: left;">No. of Servers</th>
        <th style="padding: 8px; text-align: left;">Success</th>
        <th style="padding: 8px; text-align: left;">Failed</th>
        <th style="padding: 8px; text-align: left;">Remarks</th>
      </tr>
      <tr>
        <td style="border-top:1px solid #ccc; padding: 8px;">{total}</td>
        <td style="border-top:1px solid #ccc; padding: 8px; color: green;"><b>{success}</b></td>
        <td style="border-top:1px solid #ccc; padding: 8px; color: red;"><b>{failed}</b></td>
        <td style="border-top:1px solid #ccc; padding: 8px;">{patch_link_html if patch_link_html else "No report available"}</td>
      </tr>
    </table>
    <!-- ===================== APM STATUS ===================== -->
    <h3 style="color:#2F5597; margin-top:25px;">New Relic Infra & APM Status</h3>
    <table style="border-collapse: collapse; width: 50%; border: 1px solid #ccc;">
      <tr style="background-color: #2F5597; color: white;">
        <th style="padding: 8px;">NR Reporting</th>
        <th style="padding: 8px;">APM</th>
        <th style="padding: 8px;">Green</th>
        <th style="padding: 8px;">Red</th>
      </tr>
      <tr>
        <td style="border-top:1px solid #ccc; padding: 8px;">{nr_reporting}</td>
        <td style="border-top:1px solid #ccc; padding: 8px;">{apm_total}</td>
        <td style="border-top:1px solid #ccc; padding: 8px; color: green;"><b>{apm_green}</b></td>
        <td style="border-top:1px solid #ccc; padding: 8px; color: red;"><b>{apm_red}</b></td>
      </tr>
    </table>
    <!-- ===================== REPORT LINKS ===================== -->
    <h3 style="color:#2F5597; margin-top:20px;">Reports</h3>
    <ul>
      <li>{patch_link_html}</li>
      <li>{apm_link_html}</li>
    </ul>
    <!-- ===================== ACTION ===================== -->
    <p style="margin-top:20px;">
        Operations team, please review the reports using the links above and take action for failed servers.
    </p>
    <!-- ===================== LINKS ===================== -->
    <h3 style="color:#2F5597; margin-top:20px;">Execution Links</h3>
    <p>
      <a href="{servicenow_change_url}" style="color:#1a73e8;">Change Request</a><br>
      <a href="{aap_template_url}" style="color:#1a73e8;">Ansible Job Template</a><br>
      <a href="{github_url}" style="color:#1a73e8;">Pipeline Execution</a>
    </p>
    <!-- ===================== FOOTER ===================== -->
    <p style="margin-top:10px; font-size: 13px; color: #555;">
       Questions? For more information, please contact Linux Operations Team: {support_email}
    </p>
    </body>
    </html>
    """
    else:
        html_content = f"""
    <html>
    <body style="font-family: Arial, sans-serif; font-size: 14px; color: #333;">
    <!-- ===================== HEADER ===================== -->
    <h3 style="color:#2F5597;">Summary of Maintenance</h3>
    <p>
        Patching Information for Change: <a href="{servicenow_change_url}"><b>{meta['change_id']}</b></a><br>
        No servers fell under the current change window or matched the inventory criteria, so no patching operations were executed.
    </p>
    <p>
        Operations team, please review the inventory list and change window if you intended to patch servers.
    </p>
    <!-- ===================== LINKS ===================== -->
    <h3 style="color:#2F5597; margin-top:20px;">Reports</h3>
    <ul>
      <li>{patch_link_html}</li>
      <li>{apm_link_html}</li>
    </ul>
    <h3 style="color:#2F5597; margin-top:20px;">Execution Links</h3>
    <p>
      <a href="{servicenow_change_url}" style="color:#1a73e8;">Change Request</a><br>
      <a href="{aap_template_url}" style="color:#1a73e8;">Ansible Job Template</a><br>
      <a href="{github_url}" style="color:#1a73e8;">Pipeline Execution</a>
    </p>
    <!-- ===================== FOOTER ===================== -->
    <p style="margin-top:10px; font-size: 13px; color: #555;">
       Questions? For more information, please contact Linux Operations Team: {support_email}
    </p>
    </body>
    </html>
    """

    with open(email_html_path, "w") as f:
        f.write(html_content)

    # Prepare email variables for GitHub Actions
    email_to_list = email_cfg.get("to", [])
    email_cc_list = email_cfg.get("cc", [])
    email_from = email_cfg.get("from", "")
    change_id = meta["change_id"]
    email_to = ",".join(email_to_list)
    email_cc = ",".join(email_cc_list)

    print(f"EMAIL_HTML_PATH={email_html_path}")
    print(f"EMAIL_TO={email_to}")
    print(f"EMAIL_CC={email_cc}")
    print(f"EMAIL_FROM={email_from}")
    print(f"CHANGE_ID={change_id}")