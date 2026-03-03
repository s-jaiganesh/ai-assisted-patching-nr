import subprocess
import argparse
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from .plan_loader import load_plan
from .scheduler import parse_group_name_for_schedule, tz_now
from .aap_client import AAPClient
from .db_client import seed_hosts, fetch_rows_for_hosts, fetch_apm_rows_for_hosts, classify_failures, update_monitoring, ensure_tables, connect
from .decision_gate import ai_decide
from .teams_notifier import post_teams
from .newrelic_client import NewRelicClient

def build_limit(hosts: list[str]) -> str:
    return ",".join(hosts)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", required=True)
    ap.add_argument("--run-now", action="store_true", help="Run the first discovered wave immediately, ignoring schedules.")
    args = ap.parse_args()

    plan = load_plan(args.plan)
    DEBUG = plan.get("debug", False)
    meta = plan["metadata"]

    # Timezone and Change Window setup
    tz_name = meta.get("timezone", "America/New_York")
    tz = ZoneInfo(tz_name)
    current_time = tz_now(tz)
    cw_start = datetime.fromisoformat(meta["change_window"]["start"])

    # --- Integrations Setup ---
    aap_cfg = plan["integrations"]["aap"]
    aap_token = aap_cfg.get("token") or os.getenv("AAP_TOKEN", "")
    if not aap_token or "REPLACE_ME" in aap_token:
        raise RuntimeError("AAP token missing. Put in plan.json or set env AAP_TOKEN.")

    pg_cfg = plan["integrations"]["postgres"]
    if "REPLACE_ME" in str(pg_cfg.get("password", "")):
        raise RuntimeError("Postgres password missing. Put in plan.json (POC).")

    teams_cfg = plan.get("integrations", {}).get("communications", {}).get("teams", {})
    use_teams = bool(teams_cfg.get("enabled", False)) and "REPLACE_ME" not in str(teams_cfg.get("webhook_url", ""))
    webhook = teams_cfg.get("webhook_url", "")

    llm_cfg = plan.get("integrations", {}).get("llm", {})
    if llm_cfg.get("enabled") and "REPLACE_ME" in str(llm_cfg.get("token", "")):
        raise RuntimeError("LLM token missing. Put in plan.json (POC).")

    nr_cfg = plan["integrations"].get("newrelic", {})
    nr_enabled = bool(nr_cfg.get("enabled", False))
    nr_key = nr_cfg.get("api_key", "")
    if nr_key.startswith("${") and nr_key.endswith("}"):
        nr_key = os.getenv(nr_key[2:-1], "")
    if nr_enabled and ("REPLACE_ME" in nr_key or not nr_key):
        raise RuntimeError("New Relic api_key missing. Put in plan.json (POC).")

    # --- DYNAMIC TABLE NAME GENERATION ---
    table_timestamp = current_time.strftime('%Y%m%d_%H%M')
    table_name = f"patch_status_{meta['change_id']}_{table_timestamp}"
    pg_cfg["table"] = table_name
    print(f"Using dynamic database table: {table_name}")

    # --- DATABASE SETUP ---
    with connect(pg_cfg) as conn:
        ensure_tables(conn, table_name)

    # --- Clients Setup ---
    client = AAPClient(
        aap_cfg["controller_url"],
        aap_token,
        verify_ssl=bool(aap_cfg.get("verify_ssl", True))
    )
    nr_client = NewRelicClient(
        api_key=nr_key,
        graphql_url=nr_cfg.get("graphql_url", "https://api.newrelic.com/graphql"),
        debug=DEBUG
    ) if nr_enabled else None
    traffic_minutes = int(nr_cfg.get("traffic_minutes", 5))
    max_retry_rounds = int(plan.get("policies", {}).get("max_retry_rounds", 1))
    hb_seconds = int(aap_cfg.get("teams_update_every_seconds", 600))
    inventory_id = int(aap_cfg["inventory_id"])
    job_template_id = int(aap_cfg["job_template_id"])

    # --- DYNAMIC WAVE DISCOVERY AND SCHEDULING ---
    print("Discovering all available patching groups from AAP...")
    all_groups = client.get_all_group_names(inventory_id)

    potential_waves = []
    for group_name in all_groups:
        scheduled_dt = parse_group_name_for_schedule(group_name, cw_start, tz)
        if scheduled_dt:
            potential_waves.append({
                "wave_id": group_name,
                "inventory_group": group_name,
                "scheduled_dt": scheduled_dt
            })

    # Sort waves by schedule to ensure --run-now is predictable
    potential_waves.sort(key=lambda w: w["scheduled_dt"])

    wave_to_run = None
    if args.run_now and potential_waves:
        wave_to_run = potential_waves[0]
        print(f"INFO: --run-now flag is set. Immediately running the first discovered wave: {wave_to_run['wave_id']}")
    else:
        for wave in potential_waves:
            # Run if the wave is scheduled in the last hour
            if current_time >= wave["scheduled_dt"] and current_time < (wave["scheduled_dt"] + timedelta(hours=1)):
                wave_to_run = wave
                break

    if not wave_to_run:
        if args.run_now:
            print("INFO: --run-now flag set, but no waves were discovered in the inventory. Exiting.")
        else:
            print("No patching wave scheduled to run at the current time. Exiting.")
        if use_teams:
            post_teams(webhook, f"{meta['change_id']} | No wave scheduled to run now. Orchestrator finished.")
        return

    # --- WAVE EXECUTION ---
    inv_group = wave_to_run["inventory_group"]
    wave_id = wave_to_run["wave_id"]
    print(f"Proceeding with wave: {wave_id} scheduled for {wave_to_run['scheduled_dt'].isoformat()}")

    try:
        hosts_in_wave = client.get_hosts_by_group_name(inventory_id, inv_group)
    except RuntimeError as e:
        print(f"[ERROR] Could not fetch hosts for group '{inv_group}': {e}. Aborting wave.")
        return

    if not hosts_in_wave:
        print(f"[WARNING] No hosts found in group '{inv_group}'. Skipping wave.")
        return

    seed_hosts(pg_cfg, table_name, hosts_in_wave)

    if nr_enabled and hosts_in_wave:
        for h in hosts_in_wave:
            short_hostname = h.split('.')[0]
            snap = nr_client.snapshot_for_host(short_hostname, traffic_minutes=traffic_minutes)
            update_monitoring(pg_cfg, table_name, h, phase="pre", snap=snap)

    if use_teams:
        post_teams(webhook, f"{meta['change_id']} | {wave_id} | {inv_group} | STARTING patch wave for {len(hosts_in_wave)} hosts. NR pre-check captured.")

    extra_vars = {
        "change_id": meta["change_id"],
        "wave_id": wave_id,
        "inventory_group": inv_group,
        "stage": "full",
        "patch_table": table_name,
        "pg_host": pg_cfg["host"],
        "pg_port": pg_cfg.get("port", 5432),
        "pg_dbname": pg_cfg["dbname"],
    }

    execution_mode = plan.get("execution_mode", "production")
    if execution_mode == "test":
        job_id, status = "LOCAL-HELLO-TEST", {"status": "successful"}
    elif execution_mode == "nr-test":
        job_id, status = "LOCAL-NR-TEST", {"status": "successful"}
    else:
        job_id = client.launch_job_template(job_template_id, limit=inv_group, extra_vars=extra_vars)

        def on_heartbeat(job_json: dict):
            if not use_teams:
                return
            st = job_json.get("status", "unknown")
            post_teams(webhook, f"{meta['change_id']} | {wave_id} | {inv_group} | AAP job {job_id} status={st}")

        status = client.wait_for_job(
            job_id,
            poll_seconds=int(aap_cfg.get("poll_interval_seconds", 15)),
            timeout_seconds=int(aap_cfg.get("job_timeout_seconds", 14400)),
            heartbeat_seconds=hb_seconds,
            on_heartbeat=on_heartbeat,
        )

    if nr_enabled and hosts_in_wave:
        for h in hosts_in_wave:
            short_hostname = h.split('.')[0]
            snap = nr_client.snapshot_for_host(short_hostname, traffic_minutes=traffic_minutes)
            update_monitoring(pg_cfg, table_name, h, phase="post", snap=snap)

    rows = fetch_rows_for_hosts(pg_cfg, table_name, hosts_in_wave)
    cls = classify_failures(rows)

    apm_rows = fetch_apm_rows_for_hosts(pg_cfg, table_name, hosts_in_wave)
    apm_failures = []
    for r in apm_rows:
        server = r.get("server")
        apm_name = r.get("nr_apm_name")
        pre_traffic = r.get("apm_pre_traffic") or 0.0
        post_traffic = r.get("apm_post_traffic") or 0.0
        post_alert = (r.get("apm_post_alert") or "").upper()

        if pre_traffic > 0 and post_traffic == 0:
            apm_failures.append({"server": server, "apm_name": apm_name, "reason": "APM_TRAFFIC_DROPPED_TO_ZERO"})
            continue
        if post_alert and post_alert not in ("GREEN", "NOT_ALERTING", "APM_NOT_FOUND"):
            apm_failures.append({"server": server, "apm_name": apm_name, "reason": f"APM_ALERT_{post_alert}"})

    context = {
        "change_id": meta["change_id"],
        "wave_id": wave_id,
        "inventory_group": inv_group,
        "aap_status": status,
        "failed_hosts": cls["failed"],
        "unreachable_hosts": cls["unreachable"],
        "nr_flags": apm_failures[:25],
        "constraint": "No out-of-band remediation APIs. Only retry via AAP limit."
    }

    decision_obj = ai_decide(plan, context)
    decision = decision_obj.get("decision")
    note = (decision_obj.get("note", "") or "").strip()
    retry_stage = decision_obj.get("retry_stage") or "apply_patch"

    if use_teams:
        status_str = status.get('status', 'unknown')
        teams_msg = f"{meta['change_id']} | {wave_id} | {inv_group} | AAP={status_str} | failed={len(cls['failed'])} | unreachable={len(cls['unreachable'])} | AI={decision}."
        if apm_failures:
            teams_msg += f" | APM Failures: {len(apm_failures)}"
            for fail in apm_failures[:5]:
                teams_msg += f"\n- {fail['server']} | {fail['apm_name']}: {fail['reason']}"
        if note:
            teams_msg += f" | AI Note: {note}"
        post_teams(webhook, teams_msg)

    if decision == "RETRY_FAILED_HOSTS" and cls["failed"]:
        retry_hosts = cls["failed"]
        for round_no in range(1, max_retry_rounds + 1):
            if not retry_hosts:
                break
            retry_limit = build_limit(retry_hosts)
            extra_vars2 = dict(extra_vars)
            extra_vars2["stage"] = retry_stage
            extra_vars2["retry_round"] = round_no

            job2 = client.launch_job_template(job_template_id, limit=retry_limit, extra_vars=extra_vars2)
            st2 = client.wait_for_job(
                job2,
                poll_seconds=int(aap_cfg.get("poll_interval_seconds", 15)),
                timeout_seconds=int(aap_cfg.get("job_timeout_seconds", 14400)),
                heartbeat_seconds=hb_seconds,
                on_heartbeat=(lambda j: post_teams(webhook, f"{meta['change_id']} | {wave_id} retry#{round_no} | stage={retry_stage} | job={job2} | status={j.get('status','unknown')}") if use_teams else None),
            )

            rows2 = fetch_rows_for_hosts(pg_cfg, table_name, retry_hosts)
            cls2 = classify_failures(rows2)
            retry_hosts = cls2["failed"]

            if use_teams:
                post_teams(webhook, f"{meta['change_id']} | {wave_id} retry#{round_no} DONE | AAP={st2} | remaining_failed={len(retry_hosts)}")

            if not retry_hosts:
                break

    if use_teams:
        post_teams(webhook, f"{meta['change_id']} | Orchestrator finished.")

if __name__ == "__main__":
    main()