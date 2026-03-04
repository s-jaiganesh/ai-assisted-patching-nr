import psycopg2
import psycopg2.extras
from typing import List, Dict, Any
from datetime import datetime, timezone
import json

def epoch_ms_to_timestamp(epoch_ms):
    if not epoch_ms:
        return None
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)

def connect(pg: dict):
    return psycopg2.connect(
        host=pg["host"],
        port=int(pg.get("port", 5432)),
        dbname=pg.get("dbname"),
        user=pg["user"],
        password=pg["password"],
        connect_timeout=10,
    )

def ensure_tables(conn, table: str):
    """
    Ensure patch status tables exist for this run.

    Notes:
    - Do NOT drop existing tables (historical runs must remain).
    - Create tables if missing.
    - Apply additive schema updates via ALTER TABLE ... ADD COLUMN IF NOT EXISTS.
    """
    with conn.cursor() as cur:
        # Main patch status table (additive schema; keep compatible with older runs)
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
              server TEXT PRIMARY KEY,
              wave_name TEXT,
              reachable BOOLEAN,

              pre_check TEXT, pre_check_reason TEXT,
              pre_reboot TEXT, pre_reboot_reason TEXT,
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

              nr_host_guid TEXT,
              nr_host_account_id BIGINT,

              updated_at TIMESTAMPTZ DEFAULT now()
            );
            """
        )

        # APM table
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table}_apm (
              server TEXT,
              nr_apm_guid TEXT,
              nr_apm_name TEXT,
              nr_apm_account_id BIGINT,

              apm_pre_alert TEXT,
              apm_pre_traffic DOUBLE PRECISION,
              apm_post_alert TEXT,
              apm_post_traffic DOUBLE PRECISION,

              updated_at TIMESTAMPTZ DEFAULT now(),

              PRIMARY KEY (server, nr_apm_guid)
            );
            """
        )

        # Additive migrations (for older tables created before these columns existed)
        for coldef in [
            "wave_name TEXT",
            "pre_reboot TEXT",
            "pre_reboot_reason TEXT",
        ]:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {coldef};")

        conn.commit()

def seed_hosts(pg: dict, table: str, hosts: List[str], wave_name: str | None = None):
    """Insert seed rows for hosts (idempotent).

    - server is unique key
    - wave_name is stored for reporting/dup-detection context
    """
    if not hosts:
        return
    with connect(pg) as conn:
        with conn.cursor() as cur:
            if wave_name:
                host_data = [(h, wave_name) for h in hosts]
                cur.executemany(
                    f"INSERT INTO {table}(server, wave_name) VALUES (%s, %s) "
                    f"ON CONFLICT (server) DO UPDATE SET wave_name = EXCLUDED.wave_name, updated_at = now()",
                    host_data,
                )
            else:
                host_data = [(h,) for h in hosts]
                cur.executemany(
                    f"INSERT INTO {table}(server) VALUES (%s) ON CONFLICT (server) DO NOTHING",
                    host_data,
                )
        conn.commit()


def record_duplicate_host(pg: dict, table: str, original_host: str, dup_server_key: str, current_wave: str, patched_wave: str | None):
    """Record a duplicate host occurrence as its own row (server key must be unique)."""
    reason = f"hostname_dup; already_patched_in={patched_wave or 'unknown'}; skipping_in={current_wave}"
    with connect(pg) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {table}(server, wave_name, reachable, pre_check, pre_check_reason, updated_at)
                VALUES (%s, %s, NULL, 'skipped', %s, now())
                ON CONFLICT (server) DO UPDATE SET
                    wave_name = EXCLUDED.wave_name,
                    pre_check = EXCLUDED.pre_check,
                    pre_check_reason = EXCLUDED.pre_check_reason,
                    updated_at = now();
                """,
                (dup_server_key, current_wave, reason),
            )
        conn.commit()


def fetch_rows_for_hosts(pg: dict, table: str, hosts: List[str]) -> List[Dict[str, Any]]:
    if not hosts:
        return []
    with connect(pg) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"SELECT * FROM {table} WHERE server = ANY(%s) ORDER BY server", (hosts,))
            return [dict(r) for r in cur.fetchall()]

def fetch_apm_rows_for_hosts(pg: dict, table: str, hosts: List[str]) -> List[Dict[str, Any]]:
    if not hosts:
        return []
    apm_table = f"{table}_apm"
    with connect(pg) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"SELECT * FROM {apm_table} WHERE server = ANY(%s) ORDER BY server, nr_apm_name", (hosts,))
            return [dict(r) for r in cur.fetchall()]

def update_monitoring(pg: dict, table: str, hostname: str, phase: str, snap: Dict[str, Any]):
    with connect(pg) as conn:
        apm_table = f"{table}_apm"
        with conn.cursor() as cur:
            cur.execute(f"INSERT INTO {table}(server) VALUES (%s) ON CONFLICT (server) DO NOTHING", (hostname,))

            if not snap.get("nr_found"):
                reason = snap.get("reason", "HOST_NOT_FOUND")
                cur.execute(
                    f"UPDATE {table} SET infra_{phase}_reporting=false, infra_{phase}_alert=%s, updated_at=now() WHERE server=%s",
                    (json.dumps([{"reason": reason}]), hostname)
                )
                conn.commit()
                return
            cur.execute(
                f"""
                UPDATE {table}
                SET nr_host_guid=%s,
                    nr_host_account_id=%s,
                    infra_{phase}_reporting=%s,
                    infra_{phase}_alert=%s,
                    infra_{phase}_alert_label=%s,
                    infra_{phase}_alert_openedAt=%s,
                    updated_at=now()
                WHERE server=%s
                """,
                (
                    snap.get("host_guid"),
                    snap.get("host_account_id"),
                    snap.get("infra_reporting"),
                    json.dumps([{"severity": snap.get("infra_alert"), "label": snap.get("infra_alert_label"), "openedAt": snap.get("infra_alert_openedAt")}]),
                    snap.get("infra_alert_label"),
                    epoch_ms_to_timestamp(snap.get("infra_alert_openedAt")),
                    hostname,
                )
            )

            apm_services = snap.get("apm_services", [])
            for service in apm_services:
                if phase == "pre":
                    cur.execute(
                        f"""
                        INSERT INTO {apm_table} (server, nr_apm_guid, nr_apm_name, nr_apm_account_id, apm_pre_alert, apm_pre_traffic, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, now())
                        ON CONFLICT (server, nr_apm_guid)
                        DO UPDATE SET
                          apm_pre_alert = EXCLUDED.apm_pre_alert,
                          apm_pre_traffic = EXCLUDED.apm_pre_traffic,
                          updated_at = now()
                        """,
                        (hostname, service.get("guid"), service.get("name"), service.get("account_id"), service.get("alert"), service.get("traffic"))
                    )
                else:
                    cur.execute(
                        f"""
                        UPDATE {apm_table}
                        SET apm_post_alert=%s,
                            apm_post_traffic=%s,
                            updated_at=now()
                        WHERE server=%s AND nr_apm_guid=%s
                        """,
                        (service.get("alert"), service.get("traffic"), hostname, service.get("guid"))
                    )
        conn.commit()

def classify_failures(rows: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    """
    Classifies failures based on Ansible patching results ONLY.
    APM failure classification will be handled separately in the orchestrator.
    """
    failed = []
    unreachable = []
    for r in rows:
        srv = r.get("server")
        reachable = str(r.get("reachable", "true")).lower() in ("true", "1", "yes", "y")
        if not reachable:
            unreachable.append(srv)
            continue
        for stage in ("pre_check", "pre_reboot", "apply_patch", "post_reboot", "post_check", "kernel_check"):
            if str(r.get(stage, "")).lower() == "failed":
                failed.append(srv)
                break
    return {"failed": sorted(set(failed)), "unreachable": sorted(set(unreachable))} 