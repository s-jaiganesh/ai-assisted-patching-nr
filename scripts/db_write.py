#!/usr/bin/env python3
import argparse
import psycopg2

def ensure_table(cur, table: str):
    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {table} (
      server TEXT PRIMARY KEY,
      reachable BOOLEAN,

      pre_check TEXT, pre_check_reason TEXT,
      apply_patch TEXT, apply_patch_reason TEXT,
      post_reboot TEXT, post_reboot_reason TEXT,
      post_check TEXT, post_check_reason TEXT,
      kernel_check TEXT, kernel_check_reason TEXT,

      infra_pre_reporting BOOLEAN,
      infra_pre_alert TEXT,
      infra_pre_alert_label TEXT,
      infra_pre_alert_openedAt TIMESTAMPTZ,

      infra_post_reporting BOOLEAN,
      infra_post_alert TEXT,
      infra_post_alert_label TEXT,
      infra_post_alert_openedAt TIMESTAMPTZ,

      apm_pre_alert TEXT,
      apm_pre_traffic DOUBLE PRECISION,
      apm_post_alert TEXT,
      apm_post_traffic DOUBLE PRECISION,

      nr_host_guid TEXT,
      nr_host_account_id BIGINT,
      nr_apm_guid TEXT,
      nr_apm_account_id BIGINT,
      nr_apm_name TEXT,

      updated_at TIMESTAMPTZ DEFAULT now()
    );
    """)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", type=int, default=5432)
    ap.add_argument("--dbname", required=True)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", required=True)
    ap.add_argument("--table", required=True)
    ap.add_argument("--server", required=True)
    ap.add_argument("--stage", required=True, choices=["pre_check","apply_patch","post_reboot","post_check","kernel_check"])
    ap.add_argument("--status", required=True)
    ap.add_argument("--reason", default="")
    args = ap.parse_args()

    conn = psycopg2.connect(host=args.host, port=args.port, dbname=args.dbname, user=args.user, password=args.password)
    conn.autocommit = True
    cur = conn.cursor()
    ensure_table(cur, args.table)

    cur.execute(f"INSERT INTO {args.table}(server) VALUES (%s) ON CONFLICT (server) DO NOTHING", (args.server,))

    if args.stage == "pre_check":
        reachable = (args.status.lower() == "success")
        cur.execute(
            f"UPDATE {args.table} SET reachable=%s, pre_check=%s, pre_check_reason=%s, updated_at=now() WHERE server=%s",
            (reachable, args.status, args.reason, args.server)
        )
    else:
        cur.execute(
            f"UPDATE {args.table} SET {args.stage}=%s, {args.stage}_reason=%s, updated_at=now() WHERE server=%s",
            (args.status, args.reason, args.server)
        )

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
