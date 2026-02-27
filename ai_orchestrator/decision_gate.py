import json
from .llm_client import call_llm

ALLOWED = {"PROCEED_NEXT_WAVE","RETRY_FAILED_HOSTS","SKIP_FAILED_AND_PROCEED"}

def ai_decide(plan: dict, context: dict) -> dict:
    llm = plan["integrations"]["llm"]
    system = "You are an operations patching lead. Be conservative and avoid risky actions."
    user = f"""
Output STRICT JSON only:
{{
  "decision": one of {sorted(list(ALLOWED))},
  "retry_stage": optional string only when decision is RETRY_FAILED_HOSTS,
  "note": "short sentence for ops team"
}}

Rules:
- If any hosts are unreachable or infra_post_reporting is false, prefer SKIP_FAILED_AND_PROCEED.
- If patch succeeded but apm_post_traffic dropped to 0 (and apm_pre_traffic > 0), prefer SKIP_FAILED_AND_PROCEED and escalate.
- Do not suggest calling any external remediation APIs.
- Keep note short.

Context:
{json.dumps(context, indent=2)}
"""
    raw = call_llm(llm, system, user)
    try:
        obj = json.loads(raw)
    except Exception:
        return {"decision":"SKIP_FAILED_AND_PROCEED","note":"AI output not parseable; skipping failed hosts and proceeding."}

    dec = obj.get("decision")
    if dec not in ALLOWED:
        return {"decision":"SKIP_FAILED_AND_PROCEED","note":"AI returned invalid decision; skipping failed hosts and proceeding."}
    return obj
