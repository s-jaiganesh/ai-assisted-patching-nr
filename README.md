# AI-assisted patching with New Relic (Infra + APM) validation

## What this project does
- Runs patching via **AAP Job Template** (single template, stage-controlled)
- Writes patch stage status into **Postgres**
- Runs **New Relic pre + post validation** and stores results into the same Postgres table:
  - Infra: reporting, alertSeverity, recent violation label/openedAt
  - APM: alertSeverity, traffic rate
- Uses **AI** for limited decisions only: proceed / retry failed hosts / skip-and-proceed
- Sends **Teams webhook** updates every 10 minutes while AAP job is running + end-of-wave summary

## Entry point
From repo root:
    python3 -m ai_orchestrator.orchestrator --plan plan.json --run-now

## AAP template
Point AAP Job Template to:
    ansible/playbooks/run_patching.yml
