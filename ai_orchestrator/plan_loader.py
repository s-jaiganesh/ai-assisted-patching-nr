import json
import os
from datetime import datetime

def _expand_env(obj):
    if isinstance(obj, dict):
        return {k: _expand_env(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_expand_env(v) for v in obj]
    elif isinstance(obj, str):
        return os.path.expandvars(obj)
    else:
        return obj
def load_plan(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        plan = json.load(f)
    # FIX: expand ${ENV_VAR} values from GitHub Actions / shell
    plan = _expand_env(plan)
    meta = plan.get("metadata", {})
    cw = meta.get("change_window", {})
    if not meta.get("change_id"):
        raise ValueError("plan.metadata.change_id missing")
    if not cw.get("start") or not cw.get("end"):
        raise ValueError("plan.metadata.change_window.start/end missing")
    start = datetime.fromisoformat(cw["start"])
    end = datetime.fromisoformat(cw["end"])
    if end <= start:
        raise ValueError("change_window.end must be after start")
    waves = plan.get("waves")
    if not isinstance(waves, list) or not waves:
        raise ValueError("plan.waves must be non-empty list")
    return plan