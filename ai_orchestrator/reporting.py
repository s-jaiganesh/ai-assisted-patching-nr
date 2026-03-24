import json
import os
import shutil
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


def _patch_remark(row: Dict[str, Any]) -> str:
    for stage in STAGES:
        if str(row.get(stage) or "").strip().lower() == "failed":
            return str(row.get(f"{stage}_reason") or f"{stage} failed")
    return "Patched successfully"


def _group_apm_rows(apm_rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in apm_rows:
        grouped.setdefault(str(row.get("server") or ""), []).append(row)
    return grouped


def _apm_summary_for_server(server: str, row: Dict[str, Any], apm_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    total = len(apm_rows)
    green = 0
    red = 0
    for apm_row in apm_rows:
        alert = _normalize_alert(apm_row.get("apm_post_alert"))
        if alert in ALERTING_VALUES:
            red += 1
        else:
            green += 1
    reporting = row.get("infra_post_reporting")
    reporting_value = "Yes" if reporting is True else ("No" if reporting is False else "N/A")
    if total == 0:
        apm_status = "0 Green, 0 Red"
    else:
        apm_status = f"{green} Green, {red} Red"
    return {
        "Host": server,
        "Reporting": reporting_value,
        "Total APMs": total,
        "APM Status": apm_status,
    }


def summarize_rows(rows: List[Dict[str, Any]], apm_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    apm_by_server = _group_apm_rows(apm_rows)
    patch_report_rows: List[Dict[str, Any]] = []
    apm_report_rows: List[Dict[str, Any]] = []
    success_count = 0
    failed_count = 0

    for row in rows:
        server = str(row.get("server") or "")
        patch_status = _final_patch_status(row)
        remarks = _patch_remark(row)
        if patch_status == "SUCCESS":
            success_count += 1
        else:
            failed_count += 1
        patch_report_rows.append(
            {
                "Host": server,
                "Patching Status": patch_status,
                "Remarks": remarks,
            }
        )
        apm_report_rows.append(_apm_summary_for_server(server, row, apm_by_server.get(server, [])))

    return {
        "counts": {
            "TOTAL": len(rows),
            "SUCCESS": success_count,
            "FAILED": failed_count,
            "WARNING": 0,
            "DEGRADED": 0,
        },
        "patch_report_rows": patch_report_rows,
        "apm_report_rows": apm_report_rows,
    }


def _save_workbook(path: str, headers: List[str], rows: List[List[Any]]) -> None:
    wb = Workbook()
    ws = wb.active
    ws.append(headers)
    for row in rows:
        ws.append(row)
    wb.save(path)


def _prepare_delivery_path(local_path: str, delivery_dir: str | None) -> str:
    if not delivery_dir:
        return local_path
    os.makedirs(delivery_dir, exist_ok=True)
    dest_path = os.path.join(delivery_dir, os.path.basename(local_path))
    shutil.copy2(local_path, dest_path)
    return dest_path


def write_reports(
    output_dir: str,
    change_id: str,
    rows: List[Dict[str, Any]],
    apm_rows: List[Dict[str, Any]],
    delivery_dir: str | None = None,
) -> Dict[str, str]:
    os.makedirs(output_dir, exist_ok=True)
    summary = summarize_rows(rows, apm_rows)
    patch_local_path = os.path.join(output_dir, f"{change_id}_patch_report.xlsx")
    apm_local_path = os.path.join(output_dir, f"{change_id}_apm_report.xlsx")
    summary_local_path = os.path.join(output_dir, f"{change_id}_summary.json")

    _save_workbook(
        patch_local_path,
        ["Host", "Patching Status", "Remarks"],
        [[r["Host"], r["Patching Status"], r["Remarks"]] for r in summary["patch_report_rows"]],
    )

    _save_workbook(
        apm_local_path,
        ["Host", "Reporting", "Total APMs", "APM Status"],
        [[r["Host"], r["Reporting"], r["Total APMs"], r["APM Status"]] for r in summary["apm_report_rows"]],
    )

    with open(summary_local_path, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, default=str)

    patch_delivery_path = _prepare_delivery_path(patch_local_path, delivery_dir)
    apm_delivery_path = _prepare_delivery_path(apm_local_path, delivery_dir)
    summary_delivery_path = _prepare_delivery_path(summary_local_path, delivery_dir)

    return {
        "patch_report": patch_delivery_path,
        "apm_report": apm_delivery_path,
        "summary_json": summary_delivery_path,
        "patch_report_local": patch_local_path,
        "apm_report_local": apm_local_path,
        "summary_json_local": summary_local_path,
    }
