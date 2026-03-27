import json
import os
import urllib.request

# -----------------------------
# CONFIG
# -----------------------------
LLM_ENABLED = os.getenv("LLM_ENABLED", "true").lower() == "true"
LLM_API_URL = os.getenv("LLM_API_URL")
LLM_API_KEY = os.getenv("LLM_API_KEY")
TIMEOUT = 20

# -----------------------------
# MASTER PROMPT
# -----------------------------
MASTER_PROMPT = """
You are an enterprise DevOps assistant generating Microsoft Teams notifications for automated Linux patching.

GOAL:
Generate clean, professional, structured Teams messages based on the event type and input data.

STRICT RULES:
- Output MUST be plain text with HTML formatting
- Use ONLY <br> and <a href="">
- NO markdown
- NO emojis
- NO null/None values
- Do NOT print JSON
- Keep messages concise and readable
- Always include change_id as clickable ServiceNow link if provided

FORMATTING STANDARD:
- First line: <a href="URL">CHANGE_ID</a> | EVENT TITLE
- Use section headers with labels like:
  Summary:
  Wave:
  Stage:
  Hosts:
  Status:
  Action Required:
- Use <br> for line breaks
- Keep spacing clean (avoid clutter)

EVENT TYPES AND EXPECTED FORMAT:

1. orchestrator_started
- Show waiting waves and schedule times

2. orchestrator_waiting
- Show next wave and scheduled time

3. wave_started
- Show wave name, host count, starting stage

4. wave_skipped
- Show wave name and reason

5. stage_progress
- Show completed vs expected hosts

6. stage_update
- Show success count, failed count
- If failed hosts exist, list them with reason

7. remediation_triggered
- Show stage and remediation plan (host → fix)

8. remediation_success
- Show fixed hosts and rerun triggered

9. remediation_failed
- Show failed hosts after remediation

10. remediation_job_failed
- Show failure with reason

11. stage_timeout
- Show affected hosts

12. aap_job_retry
- Show retry reason and affected hosts

13. aap_job_failed
- Show job failure and affected hosts

14. wave_completed
- Show completed stages, success/failed counts

15. patching_completed
- Show summary:
  Total, Success, Failed, Warning, Degraded
- Include:
  Reports Generated
  Action Required
  Links (pipeline, AAP job)

16. patching_skipped
- Show reason clearly

INPUT:
{input_json}

INSTRUCTIONS:
- Identify event_type
- Format message accordingly
- Ignore missing fields gracefully
- Do NOT hallucinate data
- Keep output under 15 lines unless necessary

Return ONLY the formatted message.
"""

# -----------------------------
# LLM CALL
# -----------------------------
def call_llm(payload: dict) -> str:
    try:
        prompt = MASTER_PROMPT.replace("{input_json}", json.dumps(payload, indent=2))

        body = {
            "prompt": prompt,
            "max_tokens": 800,
            "temperature": 0.2
        }

        req = urllib.request.Request(
            LLM_API_URL,
            data=json.dumps(body).encode(),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {LLM_API_KEY}"
            },
            method="POST"
        )

        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            res = json.loads(resp.read().decode())

        return res.get("text", "").strip()

    except Exception as e:
        print(f"[LLM ERROR] {e}")
        return ""


# -----------------------------
# FALLBACK (MINIMAL)
# -----------------------------
def fallback(payload: dict) -> str:
    change_id = payload.get("change_id", "UNKNOWN")
    event = payload.get("event_type", "update")

    return f"{change_id} | {event.replace('_', ' ').title()}"


# -----------------------------
# TEAMS POST
# -----------------------------
def post(webhook: str, message: str):
    try:
        req = urllib.request.Request(
            webhook,
            data=json.dumps({"text": message}).encode(),
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

    message = ""

    if LLM_ENABLED:
        message = call_llm(payload)

    if not message:
        print("[INFO] Using fallback")
        message = fallback(payload)

    post(webhook, message)