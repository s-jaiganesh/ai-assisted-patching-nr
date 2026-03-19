#!/bin/bash
set -e
python3 -m ai_orchestrator.orchestrator --plan plan.json "$@"
