import json
import urllib.request

# ✅ Use existing LLM client (GAS API)
from .llm_client import call_llm


# -----------------------------
# CONFIG
# -----------------------------
TIMEOUT = 20


# -----------------------------
# LIGHTWEIGHT PROMPT
# -----------------------------
def build_llm_prompt(payload: dict) -> str:
    return f"""
You are assisting in generating short additional context for a Teams notification.

Rules:
- Max 3 lines
- No headers
- No emojis
- No markdown
- Use <br> for line breaks if needed
- Do NOT repeat the main message
- Add only useful insight (reason, suggestion, summary)

Input:
{json.dumps(payload, indent=2)}

Return only the additional lines.
"""


# -----------------------------
# STRUCTURED MESSAGE BUILDER
# -----------------------------
def build_structured_message(payload: dict) -> str:
    change_id = payload.get("change_id", "UNKNOWN")
    event = payload.get("event_type", "")
    url = payload.get("servicenow_url", "#")

    header = f"<a href='{url}'>{change_id}</a> | "

    # -------------------------
    # ORCHESTRATOR STARTED
    # -------------------------
    if event == "orchestrator_started":
        waiting = payload.get("waiting_waves", [])
        no_waves = payload.get("no_waves", False)
    
        if no_waves:
            return (
                f"{header}Orchestrator Started<br><br>"
                f"Status:<br>No eligible waves found"
            )

        return (
            f"{header}Orchestrator Started<br><br>"
            f"Waiting Waves:<br>"
            f"{'<br>'.join(waiting) if waiting else 'None'}"
        )

    # -------------------------
    # ORCHESTRATOR WAITING
    # -------------------------
    elif event == "orchestrator_waiting":
        return (
            f"{header}Waiting for Next Wave<br><br>"
            f"Wave:<br>{payload.get('next_wave', 'N/A')}<br><br>"
            f"Scheduled Time:<br>{payload.get('scheduled_time', 'N/A')}"
        )

    # -------------------------
    # WAVE STARTED
    # -------------------------
    elif event == "wave_started":
        return (
            f"{header}Wave Started<br><br>"
            f"Wave:<br>{payload.get('wave', 'N/A')}<br><br>"
            f"Hosts:<br>{payload.get('host_count', 0)}<br><br>"
            f"Stage:<br>{payload.get('stage', 'pre_check')}"
        )

    # -------------------------
    # WAVE SKIPPED
    # -------------------------
    elif event == "wave_skipped":
        return (
            f"{header}Wave Skipped<br><br>"
            f"Wave:<br>{payload.get('wave', 'N/A')}<br><br>"
            f"Reason:<br>{payload.get('reason', 'No valid hosts')}"
        )

    # -------------------------
    # STAGE UPDATE
    # -------------------------
    elif event == "stage_update":
        success = payload.get("success") or payload.get("success_count", 0)
        failed = payload.get("failed") or payload.get("failed_count", 0)
        failed_details = payload.get("failed_hosts_details", [])
    
        msg = (
            f"{header}Stage Update<br><br>"
            f"Stage:<br>{payload.get('stage', 'N/A')}<br><br>"
            f"Success:<br>{success}<br>"
            f"Failed:<br>{failed}"
        )
    
        if failed_details:
            msg += "<br><br>Failed Hosts:<br>" + "<br>".join(failed_details)
            msg += "<br>".join(failed_details[:10])  # limit to avoid clutter
            if len(failed_details) > 10:
                msg += f"<br>...and {len(failed_details) - 10} more"
    
        return msg

    # -------------------------
    # REMEDIATION TRIGGERED
    # -------------------------
    elif event == "remediation_triggered":
        fixes = payload.get("fix_plan", [])
    
        msg = (
            f"{header}Remediation Triggered<br><br>"
            f"Stage:<br>{payload.get('stage', 'N/A')}"
        )
    
        if fixes:
            msg += "<br><br>Fix Plan:<br>" + "<br>".join(fixes)
    
        return msg

    # -------------------------
    # REMEDIATION SUCCESS
    # -------------------------
    elif event == "remediation_success":
        hosts = payload.get("fixed_hosts") or payload.get("hosts", [])
    
        return (
            f"{header}Remediation Successful<br><br>"
            f"Fixed Hosts:<br>{'<br>'.join(hosts) if hosts else 'None'}<br><br>"
            f"Rerun Triggered"
        )

    # -------------------------
    # REMEDIATION FAILED
    # -------------------------
    elif event == "remediation_failed":
        hosts = payload.get("failed_hosts", [])
        return (
            f"{header}Remediation Failed<br><br>"
            f"Hosts:<br>{'<br>'.join(hosts)}"
        )

    # -------------------------
    # AAP JOB FAILED
    # -------------------------
    elif event == "aap_job_failed":
        return (
            f"{header}AAP Job Failed<br><br>"
            f"Stage:<br>{payload.get('stage', 'N/A')}<br><br>"
            f"Reason:<br>{payload.get('job_status', 'failed')}"
        )

    # -------------------------
    # WAVE COMPLETED
    # -------------------------
    elif event == "wave_completed":
        return (
            f"{header}Wave Completed<br><br>"
            f"Wave:<br>{payload.get('wave', 'N/A')}"
        )

    # -------------------------
    # PATCHING COMPLETED
    # -------------------------
    elif event == "patching_completed":
        summary = payload.get("summary") or payload.get("summary_counts", {})
        links = payload.get("links", {})
    
        msg = (
            f"{header}Patching Completed<br><br>"
            f"Summary:<br>"
            f"Total: {summary.get('TOTAL') or summary.get('total', 0)}<br>"
            f"Success: {summary.get('SUCCESS') or summary.get('success', 0)}<br>"
            f"Failed: {summary.get('FAILED') or summary.get('failed', 0)}<br>"
            f"Warning: {summary.get('WARNING') or summary.get('warning', 0)}<br>"
            f"Degraded: {summary.get('DEGRADED') or summary.get('degraded', 0)}"
        )
    
        if links:
            msg += "<br><br>Links:<br>"
            if links.get("pipeline"):
                msg += f"<a href='{links['pipeline']}'>Pipeline</a><br>"
            if links.get("aap_job"):
                msg += f"<a href='{links['aap_job']}'>AAP Jobs</a><br>"
    
        msg += (
            "<br>Reports:<br>"
            "- Patch Report<br>"
            "- APM Report<br><br>"
            "Action Required:<br>"
            "Review failed and degraded systems"
        )
    
        return msg

    # -------------------------
    # PATCHING SKIPPED
    # -------------------------
    elif event == "patching_skipped":
        return (
            f"{header}Patching Skipped<br><br>"
            f"Reason:<br>{payload.get('reason', 'No eligible waves found')}"
        )

    return f"{change_id} | {event.replace('_', ' ').title()}"


# -----------------------------
# TEAMS POST
# -----------------------------
def post(webhook: str, message: str):
    try:
        req = urllib.request.Request(
            webhook,
            data=json.dumps({"body": message}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        urllib.request.urlopen(req, timeout=TIMEOUT)

    except Exception as e:
        print(f"[TEAMS ERROR] {e}")


# -----------------------------
# MAIN ENTRY POINT
# -----------------------------
def notify_event(use_teams: bool, webhook: str, event_type: str, data: dict):
    if not use_teams or not webhook:
        return

    payload = {
        "event_type": event_type,
        **data
    }

    # ✅ Step 1: Structured message (PRIMARY)
    message = build_structured_message(payload)

    # ✅ Step 2: LLM enhancement (via GAS API)
    try:
        prompt = build_llm_prompt(payload)
        ai_extra = call_llm(prompt)

        if ai_extra:
            message += f"<br><br>{ai_extra.strip()}"

    except Exception as e:
        print(f"[LLM ENHANCEMENT ERROR] {e}")

    # ✅ Step 3: Send to Teams
    post(webhook, message)
