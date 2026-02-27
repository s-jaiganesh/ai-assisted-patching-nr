import subprocess
import argparse
import os
from datetime import datetime
from .plan_loader import load_plan
from .scheduler import wave_start_datetime, sleep_until
from .aap_client import AAPClient
from .db_client import seed_hosts, fetch_rows_for_hosts, fetch_apm_rows_for_hosts, classify_failures, update_monitoring
from .decision_gate import ai_decide
from .teams_notifier import post_teams
from .newrelic_client import NewRelicClient

def build_limit(hosts: list[str]) -> str:
    return ",".join(hosts)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", required=True)
    ap.add_argument("--run-now", action="store_true")
    ap.add_argument("--wave")
    args = ap.parse_args()

    plan = load_plan(args.plan)
    DEBUG = plan.get("debug", False)
    meta = plan["metadata"]
    tz = meta.get("timezone", "America/New_York")
    cw_start = datetime.fromisoformat(meta["change_window"]["start"])
    cw_end = datetime.fromisoformat(meta["change_window"]["end"])

    aap_cfg = plan["integrations"]["aap"]
    aap_token = aap_cfg.get("token") or os.getenv("AAP_TOKEN","")
    if not aap_token or "REPLACE_ME" in aap_token:
        raise RuntimeError("AAP token missing. Put in plan.json or set env AAP_TOKEN.")

    pg_cfg = plan["integrations"]["postgres"]
    if "REPLACE_ME" in str(pg_cfg.get("password","")):
        raise RuntimeError("Postgres password missing. Put in plan.json (POC).")

    teams_cfg = plan.get("integrations",{}).get("communications",{}).get("teams",{})
    use_teams = bool(teams_cfg.get("enabled", False)) and "REPLACE_ME" not in str(teams_cfg.get("webhook_url",""))
    webhook = teams_cfg.get("webhook_url","")

    llm_cfg = plan["integrations"]["llm"]
    if llm_cfg.get("enabled") and "REPLACE_ME" in str(llm_cfg.get("token","")):
        raise RuntimeError("LLM token missing. Put in plan.json (POC).")

    nr_cfg = plan["integrations"].get("newrelic", {})
    nr_enabled = bool(nr_cfg.get("enabled", False))
    nr_key = nr_cfg.get("api_key","")
    if nr_key.startswith("${") and nr_key.endswith("}"):
        nr_key = os.getenv(nr_key[2:-1], "")
    if nr_enabled and ("REPLACE_ME" in nr_key or not nr_key):
        raise RuntimeError("New Relic api_key missing. Put in plan.json (POC).")
    nr_client = NewRelicClient(
        api_key=nr_key,
        graphql_url=nr_cfg.get("graphql_url","https://api.newrelic.com/graphql"),
        debug=DEBUG
    ) if nr_enabled else None
    traffic_minutes = int(nr_cfg.get("traffic_minutes", 5))

    client = AAPClient(
        aap_cfg["controller_url"],
        aap_token,
        verify_ssl=bool(aap_cfg.get("verify_ssl", True))
    )

    waves = plan["waves"]
    if args.wave:
        waves = [w for w in waves if w.get("wave_id") == args.wave]

    max_retry_rounds = int(plan.get("policies",{}).get("max_retry_rounds", 1))
    hb_seconds = int(aap_cfg.get("teams_update_every_seconds", 600))

    for wave in waves:
        wave_id = wave["wave_id"]
        start_dt = wave_start_datetime(cw_start, wave.get("scheduled_start","00:00"))
        if not args.run_now and start_dt > cw_end:
            continue

        sleep_until(start_dt, tz, run_now=args.run_now)

        inv_group = wave["inventory_group"]
        limit = inv_group  # wave execution: inventory group only (AAP limit)

        limit_hosts = wave.get("limit_hosts", [])
        seed_hosts(pg_cfg, pg_cfg["table"], limit_hosts)

        # New Relic PRE snapshot (infra + apm)
        if nr_enabled and limit_hosts:
            for h in limit_hosts:
                short_hostname = h.split('.')[0]
                snap = nr_client.snapshot_for_host(short_hostname, traffic_minutes=traffic_minutes)
                update_monitoring(pg_cfg, pg_cfg["table"], h, phase="pre", snap=snap)

        if use_teams:
            post_teams(webhook, f"{meta['change_id']} | {wave_id} | {inv_group} | STARTING patch wave. NR pre-check captured for {len(limit_hosts)} hosts.")

        extra_vars = {
            "change_id": meta["change_id"],
            "wave_id": wave_id,
            "inventory_group": inv_group,
            "stage": "full",
            "patch_table": pg_cfg["table"],
            "pg_host": pg_cfg["host"],
            "pg_port": pg_cfg.get("port", 5432),
            "pg_dbname": pg_cfg["dbname"],
        }

        execution_mode = plan.get("execution_mode", "production")
        if execution_mode == "test":
           print("[TEST MODE] Running hello.yml locally")
           subprocess.run(
                ["ansible-playbook", "ansible/playbooks/hello.yml"],
                check=True
            )
           job_id = "LOCAL-HELLO-TEST"
        elif execution_mode == "nr-test":
            print("[NR-TEST MODE] Skipping Ansible job run.")
            job_id = "LOCAL-NR-TEST"
        else:
           job_id = client.launch_job_template(
               int(aap_cfg["job_template_id"]),
               limit=limit,
               extra_vars=extra_vars
           )
        def heartbeat(job_json: dict):
            if not use_teams:
                return
            st = job_json.get("status","unknown")
            post_teams(webhook, f"{meta['change_id']} | {wave_id} | {inv_group} | AAP job {job_id} status={st}")
        if execution_mode in ('test', 'nr-test'):
            status = {"status": "successful"}
        else:
            status = client.wait_for_job(
                job_id,
                poll_seconds=int(aap_cfg.get("poll_interval_seconds",15)),
                timeout_seconds=int(aap_cfg.get("job_timeout_seconds",14400)),
                heartbeat_seconds=hb_seconds,
                on_heartbeat=heartbeat,
            )

        # New Relic POST snapshot (infra + apm)
        if nr_enabled and limit_hosts:
            for h in limit_hosts:
                short_hostname = h.split('.')[0]
                snap = nr_client.snapshot_for_host(short_hostname, traffic_minutes=traffic_minutes)
                update_monitoring(pg_cfg, pg_cfg["table"], h, phase="post", snap=snap)

        # Classify failures from Ansible and Infra
        rows = fetch_rows_for_hosts(pg_cfg, pg_cfg["table"], limit_hosts)
        cls = classify_failures(rows)  # Identifies Ansible failures

        # NEW: Classify failures from New Relic APM data
        apm_rows = fetch_apm_rows_for_hosts(pg_cfg, pg_cfg["table"], limit_hosts)
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
        print("\n============ AI input context ===============")
        print(context)
        print("\n============ END AI input ===============")
        decision_obj = ai_decide(plan, context)
        print("\n============ AI output decision ===============")
        print(decision_obj)
        print("\n============ END AI output ===============")
        decision = decision_obj.get("decision")
        note = (decision_obj.get("note","") or "").strip()
        retry_stage = decision_obj.get("retry_stage") or "apply_patch"

        if use_teams:
            status_str = status.get('status','unknown')
            teams_msg = f"{meta['change_id']} | {wave_id} | {inv_group} | AAP={status_str} | failed={len(cls['failed'])} | unreachable={len(cls['unreachable'])} | AI={decision}."
            if apm_failures:
                teams_msg += f" | **APM Failures: {len(apm_failures)}**"
                for fail in apm_failures[:5]:
                    teams_msg += f"\n- {fail['server']} | {fail['apm_name']}: {fail['reason']}"
            if note:
                teams_msg += f" | AI Note: {note}"
            post_teams(webhook, teams_msg)

        if decision == "RETRY_FAILED_HOSTS" and cls["failed"]:
            retry_hosts = cls["failed"]
            for round_no in range(1, max_retry_rounds+1):
                retry_limit = build_limit(retry_hosts)
                extra_vars2 = dict(extra_vars)
                extra_vars2["stage"] = retry_stage
                extra_vars2["retry_round"] = round_no

                job2 = client.launch_job_template(int(aap_cfg["job_template_id"]), limit=retry_limit, extra_vars=extra_vars2)
                st2 = client.wait_for_job(
                    job2,
                    poll_seconds=int(aap_cfg.get("poll_interval_seconds",15)),
                    timeout_seconds=int(aap_cfg.get("job_timeout_seconds",14400)),
                    heartbeat_seconds=hb_seconds,
                    on_heartbeat=(lambda j: post_teams(webhook, f"{meta['change_id']} | {wave_id} retry#{round_no} | stage={retry_stage} | job={job2} | status={j.get('status','unknown')}") if use_teams else None),
                )

                rows2 = fetch_rows_for_hosts(pg_cfg, pg_cfg["table"], retry_hosts)
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

