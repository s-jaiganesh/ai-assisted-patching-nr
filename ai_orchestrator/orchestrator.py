import subprocess
import argparse
import os
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from .plan_loader import load_plan
from .scheduler import parse_group_name_for_schedule, tz_now
from .aap_client import AAPClient
from .db_client import (
    seed_hosts,
    record_duplicate_host,
    fetch_rows_for_hosts,
    fetch_apm_rows_for_hosts,
    classify_failures,
    update_monitoring,
    ensure_tables,
    connect
)
from .decision_gate import ai_select_waves, ai_decide, ai_wave_failure_decision
from .teams_notifier import post_teams
from .newrelic_client import NewRelicClient

def build_limit(hosts: list[str]) -> str:
    return ",".join(hosts)

def main():
    print("Orchestrator script started.")
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", required=True)
    args = ap.parse_args()

    print(f"Loading plan from {args.plan}...")
    plan = load_plan(args.plan)
    DEBUG = plan.get("debug", False)
    meta = plan["metadata"]

    tz_name = meta.get("timezone", "America/New_York")
    tz = ZoneInfo(tz_name)
    current_time = tz_now(tz)

    cw_start = datetime.fromisoformat(meta["change_window"]["start"])
    cw_end = datetime.fromisoformat(meta["change_window"]["end"])
    print(f"Current time: {current_time}")
    print(f"Change window: Start={cw_start}, End={cw_end}")
    aap_cfg = plan["integrations"]["aap"]
    aap_token = aap_cfg.get("token") or os.getenv("AAP_TOKEN", "")
    pg_cfg = plan["integrations"]["postgres"]
    teams_cfg = plan.get("integrations", {}).get("communications", {}).get("teams", {})
    use_teams = bool(teams_cfg.get("enabled", False))
    webhook = teams_cfg.get("webhook_url", "")
    nr_cfg = plan["integrations"].get("newrelic", {})
    nr_enabled = bool(nr_cfg.get("enabled", False))
    nr_key = nr_cfg.get("api_key", "")

    table_timestamp = current_time.strftime("%Y%m%d_%H%M")
    table_name = f"patch_status_{meta['change_id']}_{table_timestamp}"
    pg_cfg["table"] = table_name
    print(f"Using dynamic database table: {table_name}")
    print("Connecting to database and ensuring tables...")
    with connect(pg_cfg) as conn:
        ensure_tables(conn, table_name)
    print("Database connection established and tables ensured.")

    client = AAPClient(
        aap_cfg["controller_url"],
        aap_token,
        verify_ssl=bool(aap_cfg.get("verify_ssl", True)),
    )
    print("AAPClient initialized.")

    nr_client = None
    if nr_enabled:
        nr_client = NewRelicClient(
            api_key=nr_key,
            graphql_url=nr_cfg.get("graphql_url", "https://api.newrelic.com/graphql"),
            debug=DEBUG,
        )
        print("NewRelicClient initialized.")
    traffic_minutes = int(nr_cfg.get("traffic_minutes", 5))

    inventory_id = int(aap_cfg["inventory_id"])
    job_template_id = int(aap_cfg["job_template_id"])

    print("Discovering all available patching groups from AAP...")
    all_groups = client.get_all_group_names(inventory_id)
    print(f"Discovered {len(all_groups)} groups: {all_groups}")  # Modified log to show groups

    # New logging to show parsed schedule for candidate groups based on change window and tolerance
    candidate_groups = []
    print("Evaluating all discovered groups against schedule and change window...")
    for group_name in all_groups:
        scheduled_dt = parse_group_name_for_schedule(group_name, cw_start, tz)
        print(f"Parsed schedule for group '{group_name}': {scheduled_dt}")

        if scheduled_dt is None:
            print(f"Skipping '{group_name}': Could not parse schedule from group name.")
            continue

        if not (cw_start <= scheduled_dt <= cw_end):
            print(f"Skipping '{group_name}': Scheduled time {scheduled_dt} is outside change window ({cw_start} - {cw_end}).")
            continue

        delay_tolerance = timedelta(minutes=meta.get("wave_delay_tolerance_minutes", 15))
        if not (current_time >= (scheduled_dt - delay_tolerance) and current_time <= (scheduled_dt + delay_tolerance)):
            print(f"Skipping '{group_name}': Scheduled time {scheduled_dt} is not within delay tolerance of current time {current_time} (tolerance: {delay_tolerance}).")
            continue

        candidate_groups.append(group_name)
        print(f"'{group_name}' added to candidate_groups.")

    wave_ctx = {
        "timezone": tz_name,
        "current_time": current_time.isoformat(),
        "delay_tolerance_minutes": meta.get("wave_delay_tolerance_minutes", 15),
        "change_window": {
            "start": meta["change_window"]["start"],
            "end": meta["change_window"]["end"],
        },
        "inventory_groups": all_groups,
        "candidate_groups": candidate_groups,
    }

    print("Calling AI to select waves...")
    wave_sel = ai_select_waves(plan, wave_ctx)
    selected_groups = wave_sel.get("waves_to_run", [])
    print(f"AI selected {len(selected_groups)} waves: {selected_groups}")
    def _sched_dt(g: str):
        dt = parse_group_name_for_schedule(g, cw_start, tz)
        print(f"Group: {g}, Parsed schedule datetime: {dt}")
        return dt or datetime.max.replace(tzinfo=tz)
    selected_groups = sorted(selected_groups, key=_sched_dt)

    wave_hosts = {}
    print("Fetching hosts for selected waves...")
    for inv_group in selected_groups:
        hosts = client.get_hosts_in_group(inventory_id, inv_group)
        wave_hosts[inv_group] = hosts
        print(f"Found {len(hosts)} hosts in wave {inv_group}")

    host_first_wave = {}
    wave_hosts_dedup = {}
    print("Deduplicating hosts across waves...")
    for inv_group in selected_groups:
        hosts = wave_hosts.get(inv_group, [])
        deduped = []
        for h in hosts:
            if h in host_first_wave:
                patched_wave = host_first_wave[h]
                dup_key = f"{h}_dup_{inv_group}"
                record_duplicate_host(
                    pg_cfg,
                    table_name,
                    original_host=h,
                    dup_server_key=dup_key,
                    current_wave=inv_group,
                    patched_wave=patched_wave,
                )
                print(f"Duplicate host {h} found in {inv_group}, already in {patched_wave}")
                continue
            host_first_wave[h] = inv_group
            deduped.append(h)
        wave_hosts_dedup[inv_group] = deduped
        print(f"Wave {inv_group} has {len(deduped)} unique hosts after deduplication.")

    jobs = {}
    job_status = {}
    retry_tracker = {}
    wave_limits = {}
    wave_extra_vars = {}

    print("Entering main loop to check for waves to launch...")
    launched_waves = set()
    while len(launched_waves) < len(selected_groups):
        current_time_for_loop = tz_now(tz)
        print(f"Looping, current time: {current_time_for_loop.isoformat()}")

        waves_to_launch_this_iteration = []
        for inv_group in selected_groups:
            if inv_group in launched_waves:
                continue

            scheduled_dt = parse_group_name_for_schedule(inv_group, cw_start, tz)
            if scheduled_dt and current_time_for_loop >= scheduled_dt:
                waves_to_launch_this_iteration.append(inv_group)

        if waves_to_launch_this_iteration:
            print(f"Waves to launch this iteration: {waves_to_launch_this_iteration}")
            for inv_group in waves_to_launch_this_iteration:
                if inv_group in launched_waves:
                    continue 

                hosts_in_wave = wave_hosts_dedup.get(inv_group, [])
                if not hosts_in_wave:
                    print(f"No unique hosts for wave {inv_group}, skipping job launch.")
                    launched_waves.add(inv_group)
                    job_status[inv_group] = "skipped_no_hosts"
                    continue

                extra_vars = {
                    "change_id": meta["change_id"],
                    "wave_id": inv_group,
                    "inventory_group": inv_group,
                    "stage": "full",
                    "patch_table": table_name,
                    "pg_host": pg_cfg["host"],
                    "pg_port": pg_cfg.get("port", 5432),
                    "pg_dbname": pg_cfg["dbname"],
                }
                limit = build_limit(hosts_in_wave)
                wave_limits[inv_group] = limit
                wave_extra_vars[inv_group] = extra_vars

                print(f"Launching job for wave {inv_group} with limit: {limit}")
                job_id = client.launch_job_template(
                    job_template_id,
                    limit=limit,
                    extra_vars=extra_vars,
                )
                jobs[inv_group] = job_id
                job_status[inv_group] = "running"
                launched_waves.add(inv_group)
                print(f"{inv_group} launched job {job_id}")

        if len(launched_waves) == len(selected_groups):
            print("All waves have been launched. Moving to monitoring.")
            break

        print("Waiting for 20 seconds before checking for next wave schedule...")
        time.sleep(20)


    if not jobs:
        print("No jobs were launched. Exiting orchestrator.")
        return

    print("Entering job monitoring loop...")
    while True:
        all_jobs_completed = True
        print(f"Checking job statuses at {tz_now(tz).isoformat()}...")
        for inv_group, job_id in jobs.items():
            if job_status.get(inv_group) not in ["successful", "skipped", "failed_terminal", "skipped_no_hosts"]:
                all_jobs_completed = False
                print(f"Fetching status for job {job_id} (Wave: {inv_group})...")
                status = client.get_job(job_id).get("status")
                print(f"Job {job_id} (Wave: {inv_group}) current status: {status}")

                if status == "successful":
                    job_status[inv_group] = "successful"
                    print(f"Job {job_id} (Wave: {inv_group}) completed successfully.")
                elif status == "failed":
                    project_update_id = client.get_job(job_id).get("project_update")
                    project_update_status = None
                    if project_update_id:
                        try:
                            pu = client.get_project_update(project_update_id)
                            project_update_status = pu.get("status")
                        except Exception as e:
                            project_update_status = f"unknown (error: {e})"
                            print(f"Error fetching project update {project_update_id}: {e}")

                    failure_ctx = {
                        "change_id": meta["change_id"],
                        "wave": inv_group,
                        "job_id": job_id,
                        "job_status": status,
                        "job_explanation": client.get_job(job_id).get("job_explanation"),
                        "project_update_status": project_update_status,
                        "retry_count": retry_tracker.get(inv_group, 0),
                    }
                    print(f"Job {job_id} failed. Calling AI for failure decision for {inv_group}...")
                    decision_obj = ai_wave_failure_decision(plan, failure_ctx)
                    decision = decision_obj.get("decision")
                    note = decision_obj.get("note")
                    print(f"AI failure decision for {inv_group}: {decision}. Note: {note}")

                    if decision == "RETRY_WAVE":
                        retry_tracker[inv_group] = retry_tracker.get(inv_group, 0) + 1
                        print(f"Retrying wave {inv_group}, retry count: {retry_tracker[inv_group]}")
                        new_job_id = client.launch_job_template(
                            job_template_id,
                            limit=wave_limits.get(inv_group, ""),
                            extra_vars=wave_extra_vars.get(inv_group, {}),
                        )
                        jobs[inv_group] = new_job_id
                        job_status[inv_group] = "running"
                        print(f"{inv_group} retried with new job {new_job_id}")
                        if use_teams:
                            post_teams(
                                webhook,
                                f"{meta['change_id']} | {inv_group} | AI decided RETRY_WAVE",
                            )
                    elif decision == "SKIP_WAVE":
                        job_status[inv_group] = "skipped"
                        print(f"AI decided to SKIP_WAVE for {inv_group}.")
                        if use_teams:
                            post_teams(
                                webhook,
                                f"{meta['change_id']} | {inv_group} | AI decided SKIP_WAVE",
                            )
                    elif decision == "FAIL_CHANGE":
                        job_status[inv_group] = "failed_terminal"
                        print(f"AI decided to FAIL_CHANGE for {inv_group}. Reason: {note}")
                        raise RuntimeError(f"AI decided to stop change: {note}")
            else: 
                print(f"Wave {inv_group} is already in a terminal state: {job_status.get(inv_group)}")

        if all(status in ["successful", "skipped", "failed_terminal", "skipped_no_hosts"] for status in job_status.values()):
            print("All jobs completed. Exiting monitoring loop.")
            break

        print("Waiting 20 seconds before next status check...")
        time.sleep(20)

    print("Orchestrator script finished.")


if __name__ == "__main__":
    main()