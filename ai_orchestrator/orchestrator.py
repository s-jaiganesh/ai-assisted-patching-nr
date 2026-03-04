import subprocess
import argparse
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from .plan_loader import load_plan
from .scheduler import parse_group_name_for_schedule, tz_now
from .aap_client import AAPClient
from .db_client import seed_hosts, record_duplicate_host, fetch_rows_for_hosts, fetch_apm_rows_for_hosts, classify_failures, update_monitoring, ensure_tables, connect
from .decision_gate import ai_select_waves, ai_decide
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
    cw_end = datetime.fromisoformat(meta["change_window"]["end"])

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
    hb_seconds = int(aap_cfg.get("teams_update_every_seconds", 600))
    inventory_id = int(aap_cfg["inventory_id"])
    job_template_id = int(aap_cfg["job_template_id"])

    # --- DYNAMIC WAVE DISCOVERY (AI selects which groups to run) ---
    print("Discovering all available patching groups from AAP...")
    all_groups = client.get_all_group_names(inventory_id)

    if not all_groups:
        msg = f"{meta['change_id']} | No inventory groups found in AAP inventory_id={inventory_id}. Failing automation."
        print(msg)
        if use_teams:
            post_teams(webhook, msg)
        raise RuntimeError("No AAP inventory groups found.")

    if args.run_now:
        # Debug override: run the first discovered group only
        selected_groups = [sorted(all_groups)[0]]
        ai_wave_note = "--run-now override"
    else:
        wave_ctx = {
            "timezone": tz_name,
            "current_time": current_time.isoformat(),
            "change_window": {"start": meta["change_window"]["start"], "end": meta["change_window"]["end"]},
            "inventory_groups": all_groups,
        }
        wave_sel = ai_select_waves(plan, wave_ctx)
        selected_groups = wave_sel.get("waves_to_run", []) or []
        ai_wave_note = wave_sel.get("note", "")

    if not selected_groups:
        msg = f"{meta['change_id']} | No patching waves selected within change window. AI_note={ai_wave_note or 'n/a'}. Failing automation."
        print(msg)
        if use_teams:
            post_teams(webhook, msg)
        # Fail pipeline immediately (no email per requirement)
        raise RuntimeError("No waves selected for this change window.")

    # Sort groups by schedule for predictable ordering (jobs can still run in parallel in AAP)
    def _sched_dt(g: str):
        dt = parse_group_name_for_schedule(g, cw_start, tz)
        return dt or datetime.max.replace(tzinfo=tz)

    selected_groups = sorted(selected_groups, key=_sched_dt)

# --- WAVE EXECUTION ---
    # Snapshot hosts per selected wave/group (inventory can be dynamic)
    wave_hosts: dict[str, list[str]] = {}
    for inv_group in selected_groups:
        try:
            hosts = client.get_hosts_by_group_name(inventory_id, inv_group)
        except RuntimeError as e:
            msg = f"[ERROR] Could not fetch hosts for group '{inv_group}': {e}. Skipping this wave."
            print(msg)
            if use_teams:
                post_teams(webhook, f"{meta['change_id']} | {inv_group} | {msg}")
            wave_hosts[inv_group] = []
            continue
        wave_hosts[inv_group] = hosts or []

    # De-duplicate hosts across waves: if a hostname appears again, skip it and record a dup row.
    host_first_wave: dict[str, str] = {}
    wave_hosts_dedup: dict[str, list[str]] = {}
    dup_notes: list[str] = []

    for inv_group in selected_groups:
        hosts = wave_hosts.get(inv_group, []) or []
        deduped: list[str] = []
        for h in hosts:
            if h in host_first_wave:
                patched_wave = host_first_wave[h]
                dup_key = f"{h}_dup_{inv_group}"
                record_duplicate_host(pg_cfg, table_name, original_host=h, dup_server_key=dup_key, current_wave=inv_group, patched_wave=patched_wave)
                dup_notes.append(f"{h} already patched in {patched_wave}; skipping in {inv_group}")
                continue
            host_first_wave[h] = inv_group
            deduped.append(h)
        wave_hosts_dedup[inv_group] = deduped

    if use_teams and dup_notes:
        # Keep Teams message short
        sample = "\n".join(dup_notes[:10])
        more = "" if len(dup_notes) <= 10 else f"\n(+{len(dup_notes)-10} more)"
        post_teams(webhook, f"{meta['change_id']} | DUPLICATE HOSTS DETECTED (skipped)\n{sample}{more}")

    # Seed DB and capture NR pre-check snapshots per wave
    for inv_group in selected_groups:
        hosts_in_wave = wave_hosts_dedup.get(inv_group, []) or []
        if not hosts_in_wave:
            msg = f"{meta['change_id']} | {inv_group} | No hosts to patch in this wave (empty or all duplicates)."
            print(msg)
            if use_teams:
                post_teams(webhook, msg)
            continue

        seed_hosts(pg_cfg, table_name, hosts_in_wave, wave_name=inv_group)

        if nr_enabled:
            for h in hosts_in_wave:
                short_hostname = h.split('.')[0]
                snap = nr_client.snapshot_for_host(short_hostname, traffic_minutes=traffic_minutes)
                update_monitoring(pg_cfg, table_name, h, phase="pre", snap=snap)

        if use_teams:
            post_teams(webhook, f"{meta['change_id']} | {inv_group} | STARTING patch wave for {len(hosts_in_wave)} hosts. NR pre-check captured.")

    # Launch all wave jobs first (runs in parallel in AAP)
    jobs: dict[str, str] = {}
    job_status: dict[str, dict] = {}
    for inv_group in selected_groups:
        hosts_in_wave = wave_hosts_dedup.get(inv_group, []) or []
        if not hosts_in_wave:
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

        execution_mode = plan.get("execution_mode", "production")
        if execution_mode in ("test", "nr-test"):
            jobs[inv_group] = f"LOCAL-{execution_mode.upper()}-{inv_group}"
            job_status[inv_group] = {"status": "successful"}
            continue

        job_id = client.launch_job_template(job_template_id, limit=inv_group, extra_vars=extra_vars)
        jobs[inv_group] = job_id

        if use_teams:
            post_teams(webhook, f"{meta['change_id']} | {inv_group} | AAP job launched: {job_id}")

    # Wait for jobs (monitoring sequentially is ok; jobs run concurrently in AAP)
    for inv_group, job_id in jobs.items():
        if str(job_id).startswith("LOCAL-"):
            continue

        def on_heartbeat(job_json: dict, _inv=inv_group, _job=job_id):
            if not use_teams:
                return
            st = job_json.get("status", "unknown")
            post_teams(webhook, f"{meta['change_id']} | {_inv} | AAP job {_job} status={st}")

        status = client.wait_for_job(
            job_id,
            poll_seconds=int(aap_cfg.get("poll_interval_seconds", 15)),
            timeout_seconds=int(aap_cfg.get("job_timeout_seconds", 14400)),
            heartbeat_seconds=int(aap_cfg.get("teams_update_every_seconds", 600)),
            on_heartbeat=on_heartbeat,
        )
        job_status[inv_group] = status

    # Post snapshots + DB evaluation per wave
    for inv_group in selected_groups:
        hosts_in_wave = wave_hosts_dedup.get(inv_group, []) or []
        if not hosts_in_wave:
            continue

        # NR reporting can lag after reboot; wait 5 minutes before post snapshot
        if nr_enabled:
            from time import sleep
            sleep(300)
            for h in hosts_in_wave:
                short_hostname = h.split('.')[0]
                snap = nr_client.snapshot_for_host(short_hostname, traffic_minutes=traffic_minutes)
                update_monitoring(pg_cfg, table_name, h, phase="post", snap=snap)

        rows = fetch_rows_for_hosts(pg_cfg, table_name, hosts_in_wave)
        cls = classify_failures(rows)

        apm_rows = fetch_apm_rows_for_hosts(pg_cfg, table_name, hosts_in_wave)
        apm_flags = []
        for r in apm_rows:
            server = r.get("server")
            apm_name = r.get("nr_apm_name")
            pre_traffic = r.get("apm_pre_traffic") or 0.0
            post_traffic = r.get("apm_post_traffic") or 0.0
            post_alert = (r.get("apm_post_alert") or "").upper()

            if pre_traffic > 0 and post_traffic == 0:
                apm_flags.append({"server": server, "apm_name": apm_name, "reason": "APM_TRAFFIC_DROPPED_TO_ZERO"})
                continue
            if post_alert and post_alert not in ("GREEN", "NOT_ALERTING", "APM_NOT_FOUND"):
                apm_flags.append({"server": server, "apm_name": apm_name, "reason": f"APM_ALERT_{post_alert}"})

        ctx = {
            "change_id": meta["change_id"],
            "wave_id": inv_group,
            "inventory_group": inv_group,
            "aap_status": job_status.get(inv_group, {}),
            "failed_hosts": cls["failed"],
            "unreachable_hosts": cls["unreachable"],
            "nr_flags": apm_flags[:50],
            "source_of_truth": "postgres",
        }
        decision_obj = ai_decide(plan, ctx)
        decision = decision_obj.get("decision")
        note = (decision_obj.get("note", "") or "").strip()

        if use_teams:
            status_str = job_status.get(inv_group, "unknown")
            msg = f"{meta['change_id']} | {inv_group} | AAP={status_str} | failed={len(cls['failed'])} | unreachable={len(cls['unreachable'])} | AI={decision}."
            if note:
                msg += f" Note: {note}"
            post_teams(webhook, msg)

    if use_teams:
        post_teams(webhook, f"{meta['change_id']} | Orchestrator completed all selected waves: {', '.join(selected_groups)}")


if __name__ == "__main__":
    main()
