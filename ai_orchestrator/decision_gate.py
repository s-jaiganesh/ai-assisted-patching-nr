import json
import re
from typing import Dict, List

from .llm_client import call_llm

DECISIONS = {"PROCEED_NEXT_WAVE", "SKIP_FAILED_AND_PROCEED"}
FIX_TYPES = {"boot_cleanup", "rpm_db_rebuild", "kernel_reinstall", "root_issue", "no_fix"}


def _safe_json_load(raw: str) -> dict | None:
    try:
        return json.loads(raw)
    except Exception:
        return None


def ai_select_waves(plan: dict, context: dict) -> dict:
    llm = plan["integrations"]["llm"]
    system = (
        "You are a Linux infrastructure patching administrator. "
        "Validate patch waves provided by the automation. "
        "Output ONLY strict JSON."
    )
    user = f"""
Output STRICT JSON only:
{{
  "waves_to_run": [string, ...],
  "note": "optional short note"
}}

Context contains candidate groups already filtered by scheduler.

Rules:
- Only choose groups from context.candidate_groups.
- Do NOT invent group names.
- Remove groups that appear intentionally disabled
  (do_not_patch, skip, ignore, backup, dummy, temp etc).
- Maintain safe ordering based on operational reasoning.

Context:
{json.dumps(context, indent=2)}
"""
    raw = call_llm(llm, system, user)
    obj = _safe_json_load(raw)
    if not obj or not isinstance(obj.get("waves_to_run"), list):
        return {"waves_to_run": context.get("candidate_groups", []), "note": "AI fallback used"}
    inv = set(context.get("candidate_groups", []))
    waves = [w for w in obj["waves_to_run"] if w in inv]
    seen = set()
    waves = [w for w in waves if not (w in seen or seen.add(w))]
    return {"waves_to_run": waves, "note": (obj.get("note") or "").strip()}


def ai_decide(plan: dict, context: dict) -> dict:
    llm = plan["integrations"]["llm"]
    system = "You are a Linux patching operations lead. Output ONLY strict JSON."
    user = f"""
Output STRICT JSON only:
{{
  "decision": one of {sorted(list(DECISIONS))},
  "note": "short sentence for ops team"
}}

Rules:
- DB is the source of truth.
- If any hosts failed any stage, prefer SKIP_FAILED_AND_PROCEED.
- If hosts unreachable, prefer SKIP_FAILED_AND_PROCEED.

Context:
{json.dumps(context, indent=2)}
"""
    raw = call_llm(llm, system, user)
    obj = _safe_json_load(raw)
    if not obj:
        return {"decision": "SKIP_FAILED_AND_PROCEED", "note": "AI output not parseable"}
    dec = obj.get("decision")
    if dec not in DECISIONS:
        return {"decision": "SKIP_FAILED_AND_PROCEED", "note": "Invalid AI decision"}
    return {"decision": dec, "note": (obj.get("note") or "").strip()}


def ai_wave_failure_decision(plan: dict, context: dict) -> dict:
    llm = plan["integrations"]["llm"]
    system = (
        "You are a Linux infrastructure patching operations lead. "
        "Be conservative and safe. Output ONLY strict JSON."
    )

    user = f"""
Output STRICT JSON only:
{{
  "decision": "RETRY_WAVE | SKIP_WAVE | FAIL_CHANGE",
  "note": "short explanation"
}}
Failure Context:
{json.dumps(context, indent=2)}
Rules:
- If the failure reason indicates project sync or project update failure, return RETRY_WAVE.
- If retry_count > 1, do NOT retry again.
- If the patch playbook started and failed during execution, prefer SKIP_WAVE.
- If the failure indicates infrastructure or systemic issue, return FAIL_CHANGE.
- Keep note short.
"""
    raw = call_llm(llm, system, user)
    obj = _safe_json_load(raw)
    if not obj:
        return {
            "decision": "SKIP_WAVE",
            "note": "AI output not parseable; skipping wave."
        }
    decision = obj.get("decision")
    if decision not in {"RETRY_WAVE", "SKIP_WAVE", "FAIL_CHANGE"}:
        return {
            "decision": "SKIP_WAVE",
            "note": "AI returned invalid decision."
        }
    return {
        "decision": decision,
        "note": (obj.get("note") or "").strip()
    }


def classify_fix_type(stage: str, reason: str) -> str:
    text = f"{stage} {reason}".lower()
    if "/boot" in text or "boot free" in text or "boot full" in text:
        return "boot_cleanup"
    if "/root" in text or re.search(r"\broot free\b", text):
        return "root_issue"
    if "rpmdb" in text or "rpm db" in text or "database disk image is malformed" in text:
        return "rpm_db_rebuild"
    if "running=" in text and "latest=" in text:
        return "kernel_reinstall"
    return "no_fix"


def ai_remediation_plan(plan: dict, stage: str, failed_rows: List[Dict[str, str]]) -> Dict[str, str]:
    llm = plan["integrations"]["llm"]
    system = (
        "You are a Linux patch remediation planner. "
        "Choose only safe remediations from the allowed list. Output ONLY strict JSON."
    )
    payload_rows = [
        {"server": r.get("server"), "reason": r.get("stage_reason", ""), "stage": stage}
        for r in failed_rows
    ]
    user = f"""
Output STRICT JSON only:
{{
  "remediations": [
    {{"server": "name", "fix_type": "boot_cleanup|rpm_db_rebuild|kernel_reinstall|root_issue|no_fix"}}
  ]
}}

Rules:
- boot_cleanup only for /boot full or boot free space issues.
- root_issue when root filesystem issue exists. Do not invent an auto-fix.
- rpm_db_rebuild only for rpm db corruption.
- kernel_reinstall only for kernel mismatch issues.
- If none clearly apply, use no_fix.

Failed rows:
{json.dumps(payload_rows, indent=2)}
"""
    try:
        raw = call_llm(llm, system, user)
        obj = _safe_json_load(raw)
    except Exception:
        obj = None
    plan_map: Dict[str, str] = {}
    if obj and isinstance(obj.get("remediations"), list):
        for item in obj["remediations"]:
            server = item.get("server")
            fix_type = item.get("fix_type")
            if server and fix_type in FIX_TYPES:
                plan_map[server] = fix_type
    for row in failed_rows:
        server = row.get("server")
        if server and server not in plan_map:
            plan_map[server] = classify_fix_type(stage, row.get("stage_reason", ""))
    return plan_map
