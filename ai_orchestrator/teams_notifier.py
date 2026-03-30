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
        return (
            f"{header}Orchestrator Started<br><br>"
            f"Status:<br>"
            f"Waiting for eligible waves"
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
        return (
            f"{header}Stage Update<br><br>"
            f"Stage:<br>{payload.get('stage', 'N/A')}<br><br>"
            f"Success:<br>{payload.get('success', 0)}<br>"
            f"Failed:<br>{payload.get('failed', 0)}"
        )

    # -------------------------
    # REMEDIATION TRIGGERED
    # -------------------------
    elif event == "remediation_triggered":
        return (
            f"{header}Remediation Triggered<br><br>"
            f"Stage:<br>{payload.get('stage', 'N/A')}"
        )

    # -------------------------
    # REMEDIATION SUCCESS
    # -------------------------
    elif event == "remediation_success":
        hosts = payload.get("fixed_hosts", [])
        return (
            f"{header}Remediation Successful<br><br>"
            f"Fixed Hosts:<br>{'<br>'.join(hosts)}<br><br>"
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
            f"Reason:<br>{payload.get('reason', 'Job execution failed')}"
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
        summary = payload.get("summary", {})

        return (
            f"{header}Patching Completed<br><br>"
            f"Summary:<br>"
            f"Total: {summary.get('TOTAL', 0)}<br>"
            f"Success: {summary.get('SUCCESS', 0)}<br>"
            f"Failed: {summary.get('FAILED', 0)}<br>"
            f"Warning: {summary.get('WARNING', 0)}<br>"
            f"Degraded: {summary.get('DEGRADED', 0)}<br><br>"
            f"Reports:<br>"
            f"- Patch Report<br>"
            f"- APM Report<br><br>"
            f"Action Required:<br>"
            f"Review failed and degraded systems"
        )

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
