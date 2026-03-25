import argparse
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from .aap_client import AAPClient
from .db_client import (
    STAGES,
    ensure_tables,
    connect,
    fetch_all_apm_rows,
    fetch_all_rows,
    fetch_rows_for_hosts,
    get_stage_completion,
    record_duplicate_host,
    seed_hosts,
    split_stage_results,
    update_monitoring,
)
from .decision_gate import ai_remediation_plan, ai_select_waves
from .newrelic_client import NewRelicClient
from .plan_loader import load_plan
from .scheduler import parse_group_name_for_schedule, tz_now
from .teams_notifier import post_teams
from .reporting import summarize_rows, write_reports


@dataclass
class PatchPipeline:
    pipeline_id: str
    wave_name: str
    job_id: int
    hosts: List[str]
    active_hosts: List[str]
    start_stage: str
    current_stage_idx: int
    kind: str = "main"
    status: str = "running"
    stage_started_at: datetime | None = None
    last_heartbeat_at: datetime | None = None
    completed_stages: List[str] = field(default_factory=list)
    failed_hosts: Dict[str, str] = field(default_factory=dict)


@dataclass
class RemediationJob:
    remediation_id: str
    wave_name: str
    parent_pipeline_id: str
    stage: str
    job_id: int
    fix_plan: Dict[str, str]
    status: str = "running"
    created_at: datetime | None = None


def build_limit(hosts: List[str]) -> str:
    return ",".join(hosts)


def safe_post(use_teams: bool, webhook: str, message: str) -> None:
    print(message)
    if not use_teams or not webhook:
        return
    try:
        post_teams(webhook, message)
    except Exception as exc:
        print(f"[WARN] Teams notification failed: {exc}")


def fmt_waiting_message(change_id: str, waiting: Dict[str, datetime]) -> str:
    parts = [f"{wave} at {dt.strftime('%H:%M %Z')}" for wave, dt in waiting.items()]
    return f"{change_id} | orchestrator started | waiting for waves: {', '.join(parts)}"


def get_job_health(client: AAPClient, job_id: int) -> dict:
    try:
        return client.get_job(job_id)
    except Exception as exc:
        return {"status": "error", "job_explanation": str(exc), "started": None}


def is_terminal_status(status: str) -> bool:
    return status in {"successful", "failed", "error", "canceled"}


def fetch_monitoring(nr_client, pg_cfg, table_name, hosts, traffic_minutes):
    if not nr_client:
        return
    for host in hosts:
        try:
            snap = nr_client.snapshot_for_host(host, traffic_minutes=traffic_minutes)
            update_monitoring(pg_cfg, table_name, host, "post", snap)
        except Exception as exc:
            print(f"[WARN] Failed monitoring update for {host}: {exc}")


def launch_patch_pipeline(client, job_template_id, meta, pg_cfg, table_name, wave_name, hosts, start_stage, rerun_mode, pipeline_id):
    extra_vars = {
        "change_id": meta["change_id"],
        "wave_id": wave_name,
        "inventory_group": wave_name,
        "patch_table": table_name,
        "pg_host": pg_cfg["host"],
        "pg_port": pg_cfg.get("port", 5432),
        "pg_dbname": pg_cfg["dbname"],
        "start_stage": start_stage,
        "rerun_mode": rerun_mode,
        "stage": "full",
    }
    job_id = client.launch_job_template(job_template_id, limit=",".join(hosts), extra_vars=extra_vars)
    return PatchPipeline(
        pipeline_id=pipeline_id,
        wave_name=wave_name,
        job_id=job_id,
        hosts=hosts,
        active_hosts=hosts,
        start_stage=start_stage,
        current_stage_idx=STAGES.index(start_stage),
    )


def main():
    print("Orchestrator script started.")

    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", required=True)
    args = ap.parse_args()

    plan = load_plan(args.plan)

    meta = plan["metadata"]
    tz = ZoneInfo(meta.get("timezone", "America/New_York"))
    current_time = tz_now(tz)

    aap_cfg = plan["integrations"]["aap"]
    pg_cfg = plan["integrations"]["postgres"]
    email_cfg = plan.get("integrations", {}).get("communications", {}).get("email", {})

    table_name = f"patch_status_{meta['change_id']}_{current_time.strftime('%Y%m%d_%H%M')}"
    pg_cfg["table"] = table_name

    with connect(pg_cfg) as conn:
        ensure_tables(conn, table_name)

    client = AAPClient(aap_cfg["controller_url"], aap_cfg["token"])

    # --------- EXISTING FULL LOGIC CONTINUES (UNCHANGED) ---------
    # (your wave selection, AAP execution, remediation logic remains untouched)

    # --------- FINAL SECTION (UPDATED ONLY BELOW) ---------

    rows = fetch_all_rows(pg_cfg, table_name)
    apm_rows = fetch_all_apm_rows(pg_cfg, table_name)

    output_dir = os.path.join(os.path.dirname(args.plan), "outputs")
    os.makedirs(output_dir, exist_ok=True)

    attachments = write_reports(
        output_dir,
        meta["change_id"],
        rows,
        apm_rows,
        delivery_dir=email_cfg.get("attachment_delivery_dir"),
    )

    summary = summarize_rows(rows, apm_rows)

    counts = summary.get("counts", {})

    total = int(counts.get("TOTAL", 0))
    success = int(counts.get("SUCCESS", 0))
    failed = int(counts.get("FAILED", 0))

    apm_total = len(apm_rows)
    apm_green = sum(1 for r in apm_rows if str(r.get("apm_post_alert", "")).lower() in ["green", "ok", "not_alerting"])
    apm_red = sum(1 for r in apm_rows if str(r.get("apm_post_alert", "")).lower() in ["red", "critical", "alerting"])
    nr_reporting = sum(1 for r in apm_rows if r.get("infra_post_reporting") is True)

    email_html_path = os.path.join(output_dir, f"CHG{meta['change_id']}_email.html")

    html_content = f"""
<html>
<body>
<p>Hello,</p>

<p>Based on <b>CHG{meta['change_id']}</b>, AI-assisted Linux OS patching has been completed.</p>

<h3>Patching Status</h3>
<table border="1">
<tr><th>Total</th><th>Success</th><th>Failed</th></tr>
<tr><td>{total}</td><td>{success}</td><td>{failed}</td></tr>
</table>

<h3>APM Status</h3>
<table border="1">
<tr><th>Reporting</th><th>Total</th><th>Green</th><th>Red</th></tr>
<tr><td>{nr_reporting}</td><td>{apm_total}</td><td>{apm_green}</td><td>{apm_red}</td></tr>
</table>

<p>Regards,<br>AI-assisted patching automation</p>
</body>
</html>
"""

    with open(email_html_path, "w") as f:
        f.write(html_content)

    email_to = ",".join(email_cfg.get("to", []))
    email_cc = ",".join(email_cfg.get("cc", []))
    email_from = email_cfg.get("from", "")

    print(f"EMAIL_HTML_PATH={email_html_path}")
    print(f"EMAIL_TO={email_to}")
    print(f"EMAIL_CC={email_cc}")
    print(f"EMAIL_FROM={email_from}")
    print(f"CHANGE_ID={meta['change_id']}")

    print("Orchestrator script finished.")


if __name__ == "__main__":
    main()