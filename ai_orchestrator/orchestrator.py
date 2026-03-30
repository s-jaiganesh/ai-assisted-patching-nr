import argparse
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional
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
from .reporting import summarize_rows, write_reports
from .email import generate_and_print_email_vars
from .teams_notifier import notify_event
from .sharepoint_client import SharePointClient

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
    aap_retry_done: bool = False


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


def get_job_health(client: AAPClient, job_id: int) -> dict:
    try:
        return client.get_job(job_id)
    except Exception as exc:
        return {"status": "error", "job_explanation": str(exc), "started": None}


def is_terminal_status(status: str) -> bool:
    return status in {"successful", "failed", "error", "canceled"}


def fetch_monitoring(
    nr_client: Optional[NewRelicClient],
    pg_cfg: dict,
    table_name: str,
    hosts: List[str],
    traffic_minutes: int,
) -> None:
    if not nr_client:
        return
    for host in hosts:
        try:
            snap = nr_client.snapshot_for_host(host, traffic_minutes=traffic_minutes)
            update_monitoring(pg_cfg, table_name, host, "post", snap)
        except Exception as exc:
            print(f"[WARN] Failed monitoring update for {host}: {exc}")


def launch_patch_pipeline(
    client: AAPClient,
    job_template_id: int,
    meta: dict,
    pg_cfg: dict,
    table_name: str,
    wave_name: str,
    hosts: List[str],
    start_stage: str,
    rerun_mode: bool,
    pipeline_id: str,
) -> PatchPipeline:
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
    limit = build_limit(hosts)
    job_id = client.launch_job_template(job_template_id, limit=limit, extra_vars=extra_vars)
    idx = STAGES.index(start_stage)
    return PatchPipeline(
        pipeline_id=pipeline_id,
        wave_name=wave_name,
        job_id=job_id,
        hosts=list(hosts),
        active_hosts=list(hosts),
        start_stage=start_stage,
        current_stage_idx=idx,
        kind="rerun" if rerun_mode else "main",
        stage_started_at=datetime.now(tz=ZoneInfo(meta.get("timezone", "America/New_York"))),
    )


def launch_remediation_job(
    client: AAPClient,
    remediation_template_id: int,
    meta: dict,
    pg_cfg: dict,
    table_name: str,
    wave_name: str,
    stage: str,
    fix_plan: Dict[str, str],
) -> int:
    extra_vars = {
        "change_id": meta["change_id"],
        "wave_id": wave_name,
        "inventory_group": wave_name,
        "patch_table": table_name,
        "pg_host": pg_cfg["host"],
        "pg_port": pg_cfg.get("port", 5432),
        "pg_dbname": pg_cfg["dbname"],
        "failed_stage": stage,
        "remediation_plan": fix_plan,
    }
    return client.launch_job_template(
        remediation_template_id,
        limit=build_limit(sorted(fix_plan.keys())),
        extra_vars=extra_vars,
    )


def main():
    print("Orchestrator script started.")
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", required=True)
    args = ap.parse_args()

    plan = load_plan(args.plan)
    DEBUG = plan.get("debug", False)
    meta = plan["metadata"]
    tz_name = meta.get("timezone", "America/New_York")
    tz = ZoneInfo(tz_name)
    current_time = tz_now(tz)
    cw_start = datetime.fromisoformat(meta["change_window"]["start"])
    cw_end = datetime.fromisoformat(meta["change_window"]["end"])

    # ServiceNow Change URL
    snow_cfg = plan["integrations"].get("servicenow", {})
    snow_instance = snow_cfg.get("instance_url", "")
    chg_no = meta.get("change_id", "")
    servicenow_change_url = f"{snow_instance}/change_request.do?sysparam_query=number={chg_no}"
    actual_start_time: datetime | None = None
    actual_end_time: datetime | None = None

    aap_cfg = plan["integrations"]["aap"]
    aap_token = aap_cfg.get("token") or os.getenv("AAP_TOKEN", "")
    pg_cfg = plan["integrations"]["postgres"]
    teams_cfg = plan.get("integrations", {}).get("communications", {}).get("teams", {})
    email_cfg = plan.get("integrations", {}).get("communications", {}).get("email", {})
    sp_cfg = plan.get("integrations", {}).get("communications", {}).get("sharepoint", {})
    sp_enabled = bool(sp_cfg.get("enabled", False))
    use_teams = bool(teams_cfg.get("enabled", False))
    webhook = teams_cfg.get("webhook_url", "")
    nr_cfg = plan["integrations"].get("newrelic", {})
    nr_enabled = bool(nr_cfg.get("enabled", False))
    nr_key = nr_cfg.get("api_key", "")

    table_timestamp = current_time.strftime("%Y%m%d_%H%M")
    table_name = f"patch_status_{meta['change_id']}_{table_timestamp}"
    pg_cfg["table"] = table_name
    with connect(pg_cfg) as conn:
        ensure_tables(conn, table_name)

    client = AAPClient(
        aap_cfg["controller_url"],
        aap_token,
        verify_ssl=bool(aap_cfg.get("verify_ssl", True)),
    )

    nr_client = None
    if nr_enabled:
        nr_client = NewRelicClient(
            api_key=nr_key,
            graphql_url=nr_cfg.get("graphql_url", "https://api.newrelic.com/graphql"),
            debug=DEBUG,
        )

    sp_client = None
    if sp_enabled:
        sp_client = SharePointClient(
            tenant_id=sp_cfg["tenant_id"],
            client_id=sp_cfg["client_id"],
            client_secret=sp_cfg["client_secret"],
            hostname=sp_cfg["hostname"],
            site_path=sp_cfg["site_path"],
            drive_name=sp_cfg.get("drive_name", "Documents"),
        )
    inventory_id = int(aap_cfg["inventory_id"])
    patch_job_template_id = int(aap_cfg["job_template_id"])
    remediation_job_template_id = int(aap_cfg.get("remediation_job_template_id", patch_job_template_id))
    poll_interval = int(aap_cfg.get("poll_interval_seconds", 15))
    timeout_seconds = int(aap_cfg.get("job_timeout_seconds", 14400))
    heartbeat_seconds = int(aap_cfg.get("teams_update_every_seconds", 600))
    traffic_minutes = int(nr_cfg.get("traffic_minutes", 5))

    all_groups = client.get_all_group_names(inventory_id)
    candidate_groups = []
    waiting_by_wave: Dict[str, datetime] = {}
    for group_name in all_groups:
        scheduled_dt = parse_group_name_for_schedule(group_name, cw_start, tz)
        if scheduled_dt is None:
            continue
        if not (cw_start <= scheduled_dt <= cw_end):
            continue
        delay_tolerance = timedelta(minutes=meta.get("wave_delay_tolerance_minutes", 15))
        if not (current_time >= (scheduled_dt - delay_tolerance) and current_time <= (scheduled_dt + delay_tolerance)):
            continue
        candidate_groups.append(group_name)
        waiting_by_wave[group_name] = scheduled_dt

    wave_ctx = {
        "timezone": tz_name,
        "current_time": current_time.isoformat(),
        "delay_tolerance_minutes": meta.get("wave_delay_tolerance_minutes", 15),
        "change_window": {"start": meta["change_window"]["start"], "end": meta["change_window"]["end"]},
        "inventory_groups": all_groups,
        "candidate_groups": candidate_groups,
    }
    wave_sel = ai_select_waves(plan, wave_ctx)
    selected_groups = wave_sel.get("waves_to_run", [])
    selected_groups = sorted(
        selected_groups,
        key=lambda g: parse_group_name_for_schedule(g, cw_start, tz) or datetime.max.replace(tzinfo=tz),
    )

    if DEBUG:
        print(f"[DEBUG] Candidate groups: {candidate_groups}")
        print(f"[DEBUG] Selected groups from AI: {selected_groups}")

    notify_event(
        use_teams,
        webhook,
        "orchestrator_started",
        {
            "change_id": meta["change_id"],
            "servicenow_url": servicenow_change_url,
            "waiting_waves": [
                f"{g} at {waiting_by_wave[g].strftime('%H:%M')}"
                for g in selected_groups if g in waiting_by_wave
            ],
            "no_waves": not bool(selected_groups)
        }
    )

    wave_hosts: Dict[str, List[str]] = {g: client.get_hosts_in_group(inventory_id, g) for g in selected_groups}
    host_first_wave: Dict[str, str] = {}
    wave_hosts_dedup: Dict[str, List[str]] = {}
    for inv_group in selected_groups:
        deduped = []
        for h in wave_hosts.get(inv_group, []):
            if h in host_first_wave:
                record_duplicate_host(
                    pg_cfg,
                    table_name,
                    original_host=h,
                    dup_server_key=f"{h}_dup_{inv_group}",
                    current_wave=inv_group,
                    patched_wave=host_first_wave[h],
                )
                continue
            host_first_wave[h] = inv_group
            deduped.append(h)
        wave_hosts_dedup[inv_group] = deduped
        seed_hosts(pg_cfg, table_name, deduped, wave_name=inv_group)

    launched_waves = set()
    pipelines: Dict[str, PatchPipeline] = {}
    remediation_jobs: Dict[str, RemediationJob] = {}
    failed_terminal: Dict[str, Dict[str, str]] = {}
    remediated_once: set[tuple[str, str]] = set()
    last_waiting_heartbeat_at: datetime | None = None

    while True:
        now = tz_now(tz)

        pending_waves = [g for g in selected_groups if g not in launched_waves]
        if pending_waves:
            next_wave = min(
                pending_waves,
                key=lambda g: parse_group_name_for_schedule(g, cw_start, tz) or datetime.max.replace(tzinfo=tz),
            )
            next_wave_dt = parse_group_name_for_schedule(next_wave, cw_start, tz)

            if (
                last_waiting_heartbeat_at is None
                or (now - last_waiting_heartbeat_at).total_seconds() >= heartbeat_seconds
            ):
                notify_event(
                    use_teams,
                    webhook,
                    "orchestrator_waiting",
                    {
                        "change_id": meta["change_id"],
                        "servicenow_url": servicenow_change_url,
                        "next_wave": next_wave,
                        "scheduled_time": next_wave_dt.strftime("%Y-%m-%d %H:%M %Z") if next_wave_dt else "unknown",
                        "pending_wave_count": len(pending_waves),
                    }
                )
                last_waiting_heartbeat_at = now

        for inv_group in selected_groups:
            if inv_group in launched_waves:
                continue
            scheduled_dt = parse_group_name_for_schedule(inv_group, cw_start, tz)
            if scheduled_dt and now >= scheduled_dt:
                hosts = wave_hosts_dedup.get(inv_group, [])
                if not hosts:
                    launched_waves.add(inv_group)
                    notify_event(
                        use_teams,
                        webhook,
                        "wave_skipped",
                        {
                            "change_id": meta["change_id"],
                            "servicenow_url": servicenow_change_url,
                            "wave": inv_group,
                            "reason": "no unique hosts"
                        }
                    )
                    continue

                if actual_start_time is None:
                    actual_start_time = now

                notify_event(
                    use_teams,
                    webhook,
                    "wave_started",
                    {
                        "change_id": meta["change_id"],
                        "servicenow_url": servicenow_change_url,
                        "wave": inv_group,
                        "host_count": len(hosts),
                        "stage": "pre_check"
                    }
                )
                pipeline = launch_patch_pipeline(
                    client,
                    patch_job_template_id,
                    meta,
                    pg_cfg,
                    table_name,
                    inv_group,
                    hosts,
                    "pre_check",
                    False,
                    f"{inv_group}:main",
                )
                pipelines[pipeline.pipeline_id] = pipeline
                launched_waves.add(inv_group)

        for remediation_id, rem in list(remediation_jobs.items()):
            if rem.status in {"successful", "failed", "error", "canceled", "done"}:
                continue

            job = get_job_health(client, rem.job_id)
            status = str(job.get("status") or "unknown")
            started = job.get("started")
            if started is None and status not in {"new", "pending", "waiting", "running"}:
                status = "failed"

            if is_terminal_status(status):
                rem.status = status
                if status == "successful":
                    stage_rows = fetch_rows_for_hosts(pg_cfg, table_name, sorted(rem.fix_plan.keys()))
                    rerun_hosts: List[str] = []
                    still_failed: List[str] = []

                    for row in stage_rows:
                        host = row["server"]
                        stage_value = str(row.get(rem.stage) or "").strip().lower()
                        if stage_value in {"success", "skipped"}:
                            rerun_hosts.append(host)
                        else:
                            still_failed.append(host)
                            failed_terminal[host] = {
                                "stage": rem.stage,
                                "reason": row.get(f"{rem.stage}_reason") or "remediation_did_not_fix_stage",
                            }

                    if rerun_hosts:
                        notify_event(
                            use_teams,
                            webhook,
                            "remediation_success",
                            {
                                "change_id": meta["change_id"],
                                "servicenow_url": servicenow_change_url,
                                "wave": rem.wave_name,
                                "stage": rem.stage,
                                "hosts": sorted(rerun_hosts),
                                "rerun_triggered": True
                            }
                        )
                        rerun_pipeline = launch_patch_pipeline(
                            client,
                            patch_job_template_id,
                            meta,
                            pg_cfg,
                            table_name,
                            rem.wave_name,
                            sorted(rerun_hosts),
                            rem.stage,
                            True,
                            f"{rem.wave_name}:rerun:{rem.stage}:{int(time.time())}",
                        )
                        pipelines[rerun_pipeline.pipeline_id] = rerun_pipeline

                    if still_failed:
                        notify_event(
                            use_teams,
                            webhook,
                            "remediation_failed",
                            {
                                "change_id": meta["change_id"],
                                "servicenow_url": servicenow_change_url,
                                "wave": rem.wave_name,
                                "stage": rem.stage,
                                "hosts": sorted(still_failed),
                                "status": "failed",
                            }
                        )
                else:
                    for host, fix_type in rem.fix_plan.items():
                        failed_terminal[host] = {
                            "stage": rem.stage,
                            "reason": f"remediation_failed:{fix_type}; {job.get('job_explanation') or status}",
                        }
                    notify_event(
                        use_teams,
                        webhook,
                        "remediation_job_failed",
                        {
                            "change_id": meta["change_id"],
                            "servicenow_url": servicenow_change_url,
                            "wave": rem.wave_name,
                            "stage": rem.stage,
                            "hosts": sorted(rem.fix_plan.keys()),
                            "status": "failed",
                        }
                    )
                rem.status = "done"
            else:
                if rem.created_at and (now - rem.created_at).total_seconds() > timeout_seconds:
                    rem.status = "done"
                    for host, fix_type in rem.fix_plan.items():
                        failed_terminal[host] = {"stage": rem.stage, "reason": f"remediation_timeout:{fix_type}"}
                    notify_event(
                        use_teams,
                        webhook,
                        "remediation_timeout",
                        {
                            "change_id": meta["change_id"],
                            "servicenow_url": servicenow_change_url,
                            "wave": rem.wave_name,
                            "stage": rem.stage,
                            "hosts": sorted(rem.fix_plan.keys()),
                            "status": "failed",
                        }
                    )

        for pipeline_id, pipe in list(pipelines.items()):
            if pipe.status in {"done", "failed_terminal"}:
                continue

            if not pipe.active_hosts:
                pipe.status = "done"
                notify_event(
                    use_teams,
                    webhook,
                    "wave_completed",
                    {
                        "change_id": meta["change_id"],
                        "servicenow_url": servicenow_change_url,
                        "wave": pipe.wave_name,
                        "completed_stages": pipe.completed_stages,
                        "failed_hosts_count": len(pipe.failed_hosts),
                        "successful_hosts_count": len(pipe.active_hosts),
                        "total_hosts": len(pipe.hosts),
                        "status": "completed",
                    }
                )
                continue

            if pipe.current_stage_idx >= len(STAGES):
                pipe.status = "done"
                notify_event(
                    use_teams,
                    webhook,
                    "wave_completed",
                    {
                        "change_id": meta["change_id"],
                        "servicenow_url": servicenow_change_url,
                        "wave": pipe.wave_name,
                        "completed_stages": pipe.completed_stages,
                        "failed_hosts_count": len(pipe.failed_hosts),
                        "successful_hosts_count": len(pipe.active_hosts),
                        "total_hosts": len(pipe.hosts),
                        "status": "completed",
                    }
                )
                continue

            stage = STAGES[pipe.current_stage_idx]
            if pipe.stage_started_at is None:
                pipe.stage_started_at = now

            completion = get_stage_completion(pg_cfg, table_name, pipe.active_hosts, stage)

            if completion["completed"]:
                results = split_stage_results(completion["rows"])
                success_hosts = results["success_hosts"]
                failed_hosts = results["failed_hosts"]

                notify_event(
                    use_teams,
                    webhook,
                    "stage_update",
                    {
                        "change_id": meta["change_id"],
                        "servicenow_url": servicenow_change_url,
                        "wave": pipe.wave_name,
                        "stage": stage,
                        "success_count": len(success_hosts),
                        "failed_count": len(failed_hosts),
                        "failed_hosts_details": [
                            f"{h} ({results['failed_reasons'].get(h, 'unknown')})"
                            for h in failed_hosts
                        ] if failed_hosts else [],
                        "status": "in_progress",
                    }
                )

                if failed_hosts:
                    failed_rows = [r for r in completion["rows"] if r.get("server") in failed_hosts]
                    fix_plan = ai_remediation_plan(plan, stage, failed_rows)

                    actionable: Dict[str, str] = {}
                    for host, fix_type in fix_plan.items():
                        retry_key = (host, stage)
                        if fix_type not in {"boot_cleanup", "rpm_db_rebuild", "kernel_reinstall", "root_issue"}:
                            failed_terminal[host] = {
                                "stage": stage,
                                "reason": results["failed_reasons"].get(host, f"no_safe_fix:{fix_type}"),
                            }
                            continue

                        if retry_key in remediated_once:
                            failed_terminal[host] = {
                                "stage": stage,
                                "reason": results["failed_reasons"].get(host, f"retry_exhausted:{fix_type}"),
                            }
                            continue

                        actionable[host] = fix_type
                        remediated_once.add(retry_key)

                    if actionable:
                        notify_event(
                            use_teams,
                            webhook,
                            "remediation_triggered",
                            {
                                "change_id": meta["change_id"],
                                "servicenow_url": servicenow_change_url,
                                "wave": pipe.wave_name,
                                "stage": stage,
                                "fix_plan": [
                                    f"{h} → {fix}"
                                    for h, fix in actionable.items()
                                ],
                                "status": "triggered",
                            }
                        )
                        rem_job_id = launch_remediation_job(
                            client,
                            remediation_job_template_id,
                            meta,
                            pg_cfg,
                            table_name,
                            pipe.wave_name,
                            stage,
                            actionable,
                        )
                        remediation_jobs[f"{pipe.pipeline_id}:{stage}:{rem_job_id}"] = RemediationJob(
                            remediation_id=f"{pipe.pipeline_id}:{stage}:{rem_job_id}",
                            wave_name=pipe.wave_name,
                            parent_pipeline_id=pipe.pipeline_id,
                            stage=stage,
                            job_id=rem_job_id,
                            fix_plan=actionable,
                            created_at=now,
                        )

                pipe.failed_hosts.update({h: results["failed_reasons"].get(h, "failed") for h in failed_hosts})
                pipe.active_hosts = success_hosts
                pipe.completed_stages.append(stage)
                pipe.current_stage_idx += 1
                pipe.stage_started_at = now
                continue

            if pipe.last_heartbeat_at is None or (now - pipe.last_heartbeat_at).total_seconds() >= heartbeat_seconds:
                notify_event(
                    use_teams,
                    webhook,
                    "stage_progress",
                    {
                        "change_id": meta["change_id"],
                        "servicenow_url": servicenow_change_url,
                        "wave": pipe.wave_name,
                        "stage": stage,
                        "completed_count": completion["completed_count"],
                        "expected_count": completion["expected_count"],
                        "status": "in_progress",
                    }
                )
                pipe.last_heartbeat_at = now

            job = get_job_health(client, pipe.job_id)
            status = str(job.get("status") or "unknown")
            started = job.get("started")
            if started is None and status not in {"new", "pending", "waiting", "running", "successful"}:
                status = "failed"

            elapsed = (now - pipe.stage_started_at).total_seconds() if pipe.stage_started_at else 0
            if elapsed > timeout_seconds:
                for host in completion["pending_hosts"]:
                    failed_terminal[host] = {"stage": stage, "reason": f"stage_timeout:{stage}"}
                pipe.status = "failed_terminal"
                notify_event(
                    use_teams,
                    webhook,
                    "stage_timeout",
                    {
                        "change_id": meta["change_id"],
                        "servicenow_url": servicenow_change_url,
                        "wave": pipe.wave_name,
                        "stage": stage,
                        "hosts": completion["pending_hosts"],
                        "status": "timeout",
                    }
                )
                continue

            if status in {"failed", "error", "canceled"} and completion["pending_hosts"]:
                explanation = str(job.get("job_explanation") or status)

                aap_not_started = job.get("started") is None
                retryable_start_failure = aap_not_started and not pipe.aap_retry_done

                if retryable_start_failure:
                    notify_event(
                        use_teams,
                        webhook,
                        "aap_job_retry",
                        {
                            "change_id": meta["change_id"],
                            "servicenow_url": servicenow_change_url,
                            "wave": pipe.wave_name,
                            "stage": stage,
                            "job_status": status,
                            "reason": explanation,
                            "hosts": completion["pending_hosts"],
                        }
                    )

                    retry_pipeline = launch_patch_pipeline(
                        client,
                        patch_job_template_id,
                        meta,
                        pg_cfg,
                        table_name,
                        pipe.wave_name,
                        pipe.active_hosts,
                        stage,
                        pipe.kind == "rerun",
                        f"{pipe.wave_name}:{pipe.kind}:{stage}:retry:{int(time.time())}",
                    )
                    retry_pipeline.completed_stages = list(pipe.completed_stages)
                    retry_pipeline.failed_hosts = dict(pipe.failed_hosts)
                    retry_pipeline.aap_retry_done = True

                    pipe.status = "done"
                    pipelines[retry_pipeline.pipeline_id] = retry_pipeline
                    continue

                for host in completion["pending_hosts"]:
                    failed_terminal[host] = {"stage": stage, "reason": f"aap_job_{status}:{explanation}"}
                pipe.status = "failed_terminal"
                notify_event(
                    use_teams,
                    webhook,
                    "aap_job_failed",
                    {
                        "change_id": meta["change_id"],
                        "servicenow_url": servicenow_change_url,
                        "wave": pipe.wave_name,
                        "stage": stage,
                        "job_status": status,
                        "hosts": completion["pending_hosts"],
                        "status": "failed",
                    }
                )

        launched_all = len(launched_waves) == len(selected_groups)
        all_pipelines_done = all(p.status in {"done", "failed_terminal"} for p in pipelines.values()) if pipelines else True
        all_rems_done = all(r.status == "done" for r in remediation_jobs.values()) if remediation_jobs else True
        if launched_all and all_pipelines_done and all_rems_done:
            actual_end_time = now
            break

        time.sleep(max(5, poll_interval))

    all_hosts = sorted(host_first_wave.keys())
    fetch_monitoring(nr_client, pg_cfg, table_name, all_hosts, traffic_minutes)

    rows = fetch_all_rows(pg_cfg, table_name)
    apm_rows = fetch_all_apm_rows(pg_cfg, table_name)

    output_dir = os.path.join(os.path.dirname(args.plan), "outputs")
    email_delivery_dir = email_cfg.get("attachment_delivery_dir")
    attachments = write_reports(
        output_dir,
        meta["change_id"],
        rows,
        apm_rows,
        delivery_dir=email_delivery_dir,
    )
    
    report_links = {}
    
    if sp_enabled and sp_client:
        try:
            change_id = meta["change_id"]
            base_folder = sp_cfg.get("base_folder", "General/patching-reports")
            folder_path = f"{base_folder}/{change_id}"
    
            patch_file = attachments.get("patch_report")
            apm_file = attachments.get("apm_report")
    
            if patch_file:
                report_links["patch_report"] = sp_client.upload_file(
                    folder_path,
                    "patch_report.xlsx",
                    patch_file,
                )
    
            if apm_file:
                report_links["apm_report"] = sp_client.upload_file(
                    folder_path,
                    "apm_report.xlsx",
                    apm_file,
                )
    
            print(f"[INFO] SharePoint upload successful: {report_links}")
    
        except Exception as e:
            print(f"[WARN] SharePoint upload failed: {e}")
    summary = summarize_rows(rows, apm_rows)
    has_waves_run = bool(selected_groups)

    # Github Run ID
    github_run_id = os.getenv("GITHUB_RUN_ID", "")
    github_repo = os.getenv("GITHUB_REPOSITORY", "")
    github_url = ""

    # AAP Template url
    aap_url = aap_cfg.get("controller_url", "")
    job_template_id = aap_cfg.get("job_template_id", "")
    aap_template_url = f"{aap_url}/#/templates/job_templates/{job_template_id}/jobs"

    if github_run_id and github_repo:
        github_url = f"https://github.com/{github_repo}/actions/runs/{github_run_id}"

    if not has_waves_run:
        notify_event(
            use_teams,
            webhook,
            "patching_skipped",
            {
                "change_id": meta["change_id"],
                "servicenow_url": servicenow_change_url,
                "reason": "no_eligible_waves",
                "message": "No patching operations were executed as no eligible waves were found in the current change window or matching inventory.",
                "link": github_url,
                "status": "skipped",
            }
        )
    else:
        notify_event(
            use_teams,
            webhook,
            "patching_completed",
            {
                "change_id": meta["change_id"],
                "servicenow_url": servicenow_change_url,
                "summary_counts": {
                    "total": summary["counts"].get("TOTAL", 0),
                    "success": summary["counts"].get("SUCCESS", 0),
                    "failed": summary["counts"].get("FAILED", 0),
                    "warning": summary["counts"].get("WARNING", 0),
                    "degraded": summary["counts"].get("DEGRADED", 0),
                },
                "reports": report_links,
                "reports_generated": ["patch_report", "apm_report"],
                "action_required": "Review failed and degraded servers from generated reports and refer email attachments.",
                "links": {
                    "pipeline": github_url,
                    "aap_job": aap_template_url
                },
                "status": "completed",
            }
        )

    has_waves_run = bool(selected_groups)
    generate_and_print_email_vars(
        plan,
        summary,
        apm_rows,
        output_dir,
        actual_start_time,
        actual_end_time,
        has_waves_run,
        report_links=report_links
    )

    print("Orchestrator script finished.")

if __name__ == "__main__":
    main()