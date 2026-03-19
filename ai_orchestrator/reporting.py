import json
import os
import smtplib
from email.message import EmailMessage
from typing import Any, Dict, List

from openpyxl import Workbook

from .db_client import STAGES

ALERTING_VALUES = {"critical", "warning", "alert", "red"}
HEALTHY_VALUES = {"", "none", "not_configured", "healthy", "green", "ok"}


def _normalize_alert(value: Any) -> str:
    return str(value or "").strip().lower()


def _final_patch_status(row: Dict[str, Any]) -> str:
    for stage in STAGES:
        if str(row.get(stage) or "").strip().lower() == "failed":
            return "FAILED"
    return "SUCCESS"


def _apm_bucket(apm_rows: List[Dict[str, Any]]) -> str:
    if not apm_rows:
        return "N/A"
    alerts = [_normalize_alert(r.get("apm_post_alert")) for r in apm_rows]
    bad = [a for a in alerts if a in ALERTING_VALUES]
    good = [a for a in alerts if a in HEALTHY_VALUES]
    if bad and good:
        return "DEGRADED"
    if bad:
        return "DEGRADED"
    return "GREEN"


def summarize_rows(rows: List[Dict[str, Any]], apm_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    apm_by_server: Dict[str, List[Dict[str, Any]]] = {}
    for row in apm_rows:
        apm_by_server.setdefault(row.get("server"), []).append(row)

    detail_rows = []
    counts = {"SUCCESS": 0, "FAILED": 0, "WARNING": 0, "DEGRADED": 0}

    for row in rows:
        patch = _final_patch_status(row)
        nr_reporting = row.get("infra_post_reporting")
        apm_state = _apm_bucket(apm_by_server.get(row.get("server"), []))
        final_status = patch
        if patch != "FAILED":
            if nr_reporting is False:
                final_status = "WARNING"
            elif apm_state == "DEGRADED":
                final_status = "DEGRADED"
            else:
                final_status = "SUCCESS"
        counts[final_status] = counts.get(final_status, 0) + 1
        detail_rows.append(
            {
                "server": row.get("server"),
                "wave_name": row.get("wave_name"),
                "patch_status": patch,
                "nr_reporting": "Yes" if nr_reporting is True else ("No" if nr_reporting is False else "N/A"),
                "apm_status": apm_state,
                "final_status": final_status,
                "pre_check_reason": row.get("pre_check_reason"),
                "apply_patch_reason": row.get("apply_patch_reason"),
                "post_reboot_reason": row.get("post_reboot_reason"),
                "post_check_reason": row.get("post_check_reason"),
                "kernel_check_reason": row.get("kernel_check_reason"),
            }
        )
    return {"counts": counts, "details": detail_rows, "apm_by_server": apm_by_server}


def _save_workbook(path: str, headers: List[str], rows: List[List[Any]]):
    wb = Workbook()
    ws = wb.active
    ws.append(headers)
    for row in rows:
        ws.append(row)
    wb.save(path)


def write_reports(output_dir: str, change_id: str, rows: List[Dict[str, Any]], apm_rows: List[Dict[str, Any]]) -> Dict[str, str]:
    os.makedirs(output_dir, exist_ok=True)
    summary = summarize_rows(rows, apm_rows)
    patch_path = os.path.join(output_dir, f"{change_id}_patch_status.xlsx")
    apm_path = os.path.join(output_dir, f"{change_id}_apm_status.xlsx")
    summary_path = os.path.join(output_dir, f"{change_id}_summary.json")

    _save_workbook(
        patch_path,
        ["server", "wave_name", "patch_status", "nr_reporting", "apm_status", "final_status", "pre_check_reason", "apply_patch_reason", "post_reboot_reason", "post_check_reason", "kernel_check_reason"],
        [
            [
                d["server"], d["wave_name"], d["patch_status"], d["nr_reporting"], d["apm_status"], d["final_status"],
                d["pre_check_reason"], d["apply_patch_reason"], d["post_reboot_reason"], d["post_check_reason"], d["kernel_check_reason"],
            ]
            for d in summary["details"]
        ],
    )

    _save_workbook(
        apm_path,
        ["server", "nr_apm_name", "apm_post_alert", "apm_post_traffic", "nr_apm_guid", "nr_apm_account_id"],
        [
            [r.get("server"), r.get("nr_apm_name"), r.get("apm_post_alert"), r.get("apm_post_traffic"), r.get("nr_apm_guid"), r.get("nr_apm_account_id")]
            for r in apm_rows
        ],
    )

    with open(summary_path, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, default=str)

    return {"patch_report": patch_path, "apm_report": apm_path, "summary_json": summary_path}


def send_email_if_configured(email_cfg: Dict[str, Any], change_id: str, summary: Dict[str, Any], attachments: Dict[str, str]) -> None:
    if not email_cfg.get("enabled"):
        return
    recipients = email_cfg.get("to") or []
    if isinstance(recipients, str):
        recipients = [x.strip() for x in recipients.split(",") if x.strip()]
    if not recipients:
        return

    msg = EmailMessage()
    msg["Subject"] = f"{change_id} patching summary"
    msg["From"] = email_cfg["from"]
    msg["To"] = ", ".join(recipients)
    counts = summary.get("counts", {})
    msg.set_content(
        f"Change {change_id} completed. Success={counts.get('SUCCESS',0)}, Failed={counts.get('FAILED',0)}, "
        f"Warning={counts.get('WARNING',0)}, Degraded={counts.get('DEGRADED',0)}. See attachments."
    )
    for path in attachments.values():
        with open(path, "rb") as fh:
            data = fh.read()
        msg.add_attachment(data, maintype="application", subtype="octet-stream", filename=os.path.basename(path))

    host = email_cfg.get("smtp_host")
    port = int(email_cfg.get("smtp_port", 25))
    with smtplib.SMTP(host, port, timeout=30) as smtp:
        if email_cfg.get("starttls"):
            smtp.starttls()
        if email_cfg.get("username"):
            smtp.login(email_cfg["username"], email_cfg.get("password", ""))
        smtp.send_message(msg)
