import subprocess
import argparse
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from time import sleep
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
    args = ap.parse_args()

    plan = load_plan(args.plan)
    DEBUG = plan.get("debug", False)
    meta = plan["metadata"]

    tz_name = meta.get("timezone", "America/New_York")
    tz = ZoneInfo(tz_name)
    current_time = tz_now(tz)
    cw_start = datetime.fromisoformat(meta["change_window"]["start"])
    cw_end = datetime.fromisoformat(meta["change_window"]["end"])

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

    table_timestamp = current_time.strftime('%Y%m%d_%H%M')
    table_name = f"patch_status_{meta['change_id']}_{table_timestamp}"
    pg_cfg["table"] = table_name
    print(f"Using dynamic database table: {table_name}")

    with connect(pg_cfg) as conn:
        ensure_tables(conn, table_name)

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
    inventory_id = int(aap_cfg["inventory_id"])
    job_template_id = int(aap_cfg["job_template_id"])

    print("Discovering all available patching groups from AAP...")
    all_groups = client.get_all_group_names(inventory_id)

    if not all_groups:
        msg = f"{meta['change_id']} | No inventory groups found in AAP inventory_id={inventory_id}. Failing automation."
        print(msg)
        if use_teams:
            post_teams(webhook, msg)
        raise RuntimeError("No AAP inventory groups found.")

    wave_ctx = {
        "timezone": tz_name,
        "current_time": current_time.isoformat(),
        "delay_tolerance_minutes": meta.get("wave_delay_tolerance_minutes", 15),
        "change_window": {"start": meta["change_window"]["start"], "end": meta["change_window"]["end"]},
        "inventory_groups": all_groups,
    }


    # --- PYTHON GROUP FILTER (deterministic) ---
    candidate_groups = []
    for g in all_groups:
        sched = parse_group_name_for_schedule(g, cw_start, tz)
        if not sched:
            continue
        if sched < cw_start or sched > cw_end:
            continue
        candidate_groups.append(g)
    
    print("candidate_groups", candidate_groups)
    # --- AI VALIDATION ---
    
    wave_ctx = {
        "timezone": tz_name,
        "current_time": current_time.isoformat(),
        "change_window": {
            "start": meta["change_window"]["start"],
            "end": meta["change_window"]["end"]
        },
        "candidate_groups": candidate_groups
    
    }
    wave_sel = ai_select_waves(plan, wave_ctx)
    selected_groups = wave_sel.get("waves_to_run", []) or []    
    print("selected groups", selected_groups)

    ai_wave_note = wave_sel.get("note", "")    
    
    if not selected_groups:
        msg = f"{meta['change_id']} | No patching waves selected within change window. AI_note={ai_wave_note or 'n/a'}. Failing automation."
        print(msg)
        if use_teams:
            post_teams(webhook, msg)
        raise RuntimeError("No waves selected for this change window.")

    def _sched_dt(g: str):
        dt = parse_group_name_for_schedule(g, cw_start, tz)
        return dt or datetime.max.replace(tzinfo=tz)

    selected_groups = sorted(selected_groups, key=_sched_dt)

    # --- HOST SNAPSHOT ---
    wave_hosts: dict[str, list[str]] = {}

    for inv_group in selected_groups:
        try:
            hosts = client.get_hosts_in_group(inventory_id, inv_group)
            print(f"DEBUG hosts discovered for {inv_group}: {hosts}")
        except RuntimeError as e:
            msg = f"[ERROR] Could not fetch hosts for group '{inv_group}': {e}. Skipping this wave."
            print(msg)
            if use_teams:
                post_teams(webhook, f"{meta['change_id']} | {inv_group} | {msg}")
            wave_hosts[inv_group] = []
            continue

        wave_hosts[inv_group] = hosts or []

    host_first_wave: dict[str, str] = {}
    wave_hosts_dedup: dict[str, list[str]] = {}

    for inv_group in selected_groups:
        hosts = wave_hosts.get(inv_group, []) or []
        deduped: list[str] = []

        for h in hosts:
            if h in host_first_wave:
                patched_wave = host_first_wave[h]
                dup_key = f"{h}_dup_{inv_group}"
                record_duplicate_host(pg_cfg, table_name, original_host=h, dup_server_key=dup_key, current_wave=inv_group, patched_wave=patched_wave)
                continue

            host_first_wave[h] = inv_group
            deduped.append(h)

        wave_hosts_dedup[inv_group] = deduped

    # -------- WAVE SCHEDULER --------

    wave_schedule = {}
    for g in selected_groups:
        wave_schedule[g] = _sched_dt(g)

    started_waves = set()

    while True:

        now = tz_now(tz)

        if now > cw_end:
            break

        for inv_group, sched_time in wave_schedule.items():

            if inv_group in started_waves:
                continue

            if sched_time <= now:

                hosts_in_wave = wave_hosts_dedup.get(inv_group, []) or []
                if not hosts_in_wave:
                    started_waves.add(inv_group)
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
                    started_waves.add(inv_group)
                    continue

                limit = build_limit(hosts_in_wave)

                print(f"DEBUG wave: {inv_group}")
                print(f"DEBUG hosts: {hosts_in_wave}")
                print(f"DEBUG limit string: {limit}")

#                import sys
#                sys.exit("DEBUG STOP BEFORE AAP JOB LAUNCH")

                job_id = client.launch_job_template(
                    job_template_id,
                    limit=limit,
                    extra_vars=extra_vars
                )

                if use_teams:
                    post_teams(webhook, f"{meta['change_id']} | {inv_group} | AAP job launched: {job_id}")

                started_waves.add(inv_group)

        if len(started_waves) == len(wave_schedule):
            break

        sleep(15)


if __name__ == "__main__":
    main()
