import json
from .llm_client import call_llm

DECISIONS = {"PROCEED_NEXT_WAVE", "SKIP_FAILED_AND_PROCEED"}


def _safe_json_load(raw: str) -> dict | None:
    try:
        return json.loads(raw)
    except Exception:
        return None


def ai_select_waves(plan: dict, context: dict) -> dict:
    """AI selects inventory group names (waves) to run within a change window.

    Expected STRICT JSON:
    {
      "waves_to_run": ["week3_sat_2300", ...],
      "note": "short optional note"
    }
    """
    llm = plan["integrations"]["llm"]
    system = "You are a Linux patching operations lead. Output ONLY strict JSON. No prose."
    user = f"""
Output STRICT JSON only:
{{
  "waves_to_run": [string, ...],
  "note": "optional short note"
}}

Rules:
- Use ONLY group names from context.inventory_groups.
- Include any group whose START time (from its name weekX_day_HHMM) falls inside the change window [start,end].
- Do NOT require exact match to current time; the workflow may start a few minutes late.
- Group names do not include end time; only compare start times to the change window.
- If none match, return an empty list.

Context:
{json.dumps(context, indent=2)}
"""
    raw = call_llm(llm, system, user)
    obj = _safe_json_load(raw)
    if not obj or not isinstance(obj.get("waves_to_run", None), list):
        return {"waves_to_run": [], "note": "AI output not parseable for wave selection."}

    inv = set(context.get("inventory_groups", []))
    waves = [w for w in obj.get("waves_to_run", []) if isinstance(w, str) and w in inv]
    # De-duplicate while preserving order
    seen = set()
    waves = [w for w in waves if not (w in seen or seen.add(w))]
    return {"waves_to_run": waves, "note": (obj.get("note") or "").strip()}


def ai_decide(plan: dict, context: dict) -> dict:
    """AI decides whether to proceed to next wave based on DB-sourced outcomes."""
    llm = plan["integrations"]["llm"]
    system = "You are a Linux patching operations lead. Be conservative. Output ONLY strict JSON."
    user = f"""
Output STRICT JSON only:
{{
  "decision": one of {sorted(list(DECISIONS))},
  "note": "short sentence for ops team"
}}

Rules:
- DB is the source of truth.
- If any hosts failed any stage, prefer SKIP_FAILED_AND_PROCEED (continue remaining hosts/waves).
- If any hosts are unreachable, prefer SKIP_FAILED_AND_PROCEED.
- Keep note short. Do not suggest external remediation actions.

Context:
{json.dumps(context, indent=2)}
"""
    raw = call_llm(llm, system, user)
    obj = _safe_json_load(raw)
    if not obj:
        return {"decision": "SKIP_FAILED_AND_PROCEED", "note": "AI output not parseable; skipping failed hosts and proceeding."}

    dec = obj.get("decision")
    if dec not in DECISIONS:
        return {"decision": "SKIP_FAILED_AND_PROCEED", "note": "AI returned invalid decision; skipping failed hosts and proceeding."}

    return {"decision": dec, "note": (obj.get("note") or "").strip()}
