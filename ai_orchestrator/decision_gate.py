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

    # read delay tolerance from plan.json
    delay_tolerance = plan.get("wave_delay_tolerance_minutes", 15)

    system = (
        "You are a Linux infrastructure patching administrator. "
        "You must be conservative and safe. Output ONLY strict JSON. No prose."
    )

    user = f"""
Output STRICT JSON only:
{{
  "waves_to_run": [string, ...],
  "note": "optional short note"
}}

Follow these steps carefully before producing the final answer.

Step 1
Identify valid patch wave group names from context.inventory_groups.
A valid wave name format is:
week<week_number>_<day>_<HHMM>

Examples:
week1_thu_1645
week3_sat_2300

Step 2
Ignore inventory groups that appear intentionally disabled or not meant for patching.

These may contain words such as:
do_not_patch, dont_touch, do_not_touch, dont_touch_me, no_patch, no-patch,
ignore, skip, backup, bkp, temp, test, hold, old, dummy.

Note: naming can vary; use your judgement to exclude groups that clearly indicate "do not patch".

Step 3
From the remaining valid groups, determine which waves fall inside the change window [start,end].

Important:
Group names do not include end time; only compare START times (HHMM in the name) to the change window.

Step 4
Select waves that are due to run now based on current time.

Operational note:
Automation pipelines (ServiceNow → GitHub Actions → orchestrator) may start slightly late.

Therefore apply a **{delay_tolerance} minute tolerance window**.

Rules:
- A wave is eligible if its start time is within the change window.
- Include waves whose start time is up to {delay_tolerance} minutes before current time (to tolerate automation delays).
- Return ALL valid wave group names that meet these conditions.
- One or multiple waves may be returned.

Step 5 (Self Verification)
Before returning the final answer, verify that:

- Every selected wave exists in context.inventory_groups (do not invent names).
- No selected wave appears intentionally disabled or not for patching.
- The selected wave matches the valid wave format week<week_number>_<day>_<HHMM>.
- The wave start time falls within the change window.

If any selected wave violates these rules, remove it from the final list.

If none match, return an empty list.

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