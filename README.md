# AI-assisted patching with New Relic (Infra + APM) validation

## What this project does
- Runs patching via **AAP Job Template** (single template, stage-controlled)
- Writes patch stage status into **Postgres**
- Runs **New Relic post validation** and stores results into the same Postgres table
- Uses **AI** for limited decisions only: wave selection and safe remediation selection
- Sends **Teams** updates during orchestration
- Generates **Patch** and **APM** Excel reports
- Triggers a dedicated **AAP email job template** to send the reports as attachments

## Entry point
From repo root:

```bash
python3 -m ai_orchestrator.orchestrator --plan plan.json
```

## AAP templates
- Main patching template: `ansible/playbooks/run_patching.yml`
- Remediation template: `ansible/playbooks/run_remediation.yml`
- Email template: `ansible/playbooks/send_report_email.yml`

## Email delivery note
For the email job to attach the Excel files successfully, the file paths passed by the orchestrator must be reachable from the AAP execution environment. Use a shared path in `communications.email.attachment_delivery_dir` when required.
