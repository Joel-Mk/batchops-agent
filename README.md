BatchOps Agent AI Agent Orchestrator for a Config-Driven Batch ML
Pipeline

============================================================

OVERVIEW

BatchOps Agent is a config-driven AI orchestration system that executes
an end-to-end batch ML pipeline for multiple data providers.

The system: - Accepts CLI triggers - Generates a structured execution
plan - Executes a custom state-machine workflow - Enforces guardrails -
Persists state for resume - Produces structured run artifacts 

ARCHITECTURE

CLI (agent.py) ↓ Orchestrator (Custom State Machine) ↓ Ingestion ↓
Validation (DQ Gate) ↓ Import (DuckDB - Idempotent) ↓ Feature ETL ↓
Train ↓ Inference ↓ Artifacts (runs//)

PROJECT STRUCTURE

configs/ → Provider configs (YAML) pipelines/ → Pipeline specification
(YAML) src/ → Core orchestration + pipeline modules scripts/ → Synthetic
generator + OCR script data/ → OCR input/output runs/example_run/ →
Example successful run artifacts agent.py → CLI entry point

INSTALLATION

1)  Install dependencies

pip install -r requirements.txt

2)  Install OCR Dependencies (Windows)

-   Install Tesseract OCR
-   Install Poppler
-   Update paths inside scripts/pdf_to_csv.py

RUNNING THE PIPELINE

CLI Trigger:

python agent.py run –provider provider_a –pipeline propensity_model
–snapshot-date 2026-02-01

Resume Failed Run:

python agent.py resume –run-id

State is persisted in: runs//state.json

CONFIG-DRIVEN MULTI-PROVIDER DESIGN

Providers: configs/provider_a.yaml configs/provider_b.yaml

Pipeline: pipelines/propensity_model.yaml

Adding a new provider requires only a new YAML config.

PIPELINE STAGES

1)  Ingestion

-   Reads CSV files
-   Supports multiple entities (customers, transactions)

2)  Validation (DQ Gate) Checks:

-   Required columns
-   Uniqueness
-   Missingness thresholds


Outputs: dq_report.json dq_report.md

Training is blocked if validation fails.

3)  Import (Raw to Curated)

-   Loads data into DuckDB
-   Idempotent (reruns do not duplicate)

4)  Feature ETL Creates features keyed by: customer_id + snapshot_date


5)  Train Model:

-   Logistic Regression

Outputs: - model.pkl - metrics.json (ROC-AUC, Accuracy)

6)  Inference Outputs: customer_id, score, snapshot_date

Guardrails: - Cannot infer without model artifact - Basic sanity checks
applied

RUN ARTIFACTS

Each run produces:

runs// plan.json logs.jsonl dq_report.json dq_report.md metrics.json
model/ predictions/ run_summary.md

Example run included: runs/example_run/

OCR: PDF TO CSV EXTRACTION

Implemented using: - Tesseract OCR - Poppler - pdf2image

Run:

python scripts/pdf_to_csv.py

Converts: data/ocr_input/sample_input.pdf to
data/ocr_input/ocr_output.csv

SYNTHETIC DATA GENERATOR

python scripts/generate_data.py

Creates: - customers.csv - transactions.csv

GUARDRAILS IMPLEMENTED

-   Block training if DQ fails
-   Block inference without model artifact
-   Persist state for resume
-   Idempotent import
-   Structured JSON logging

DESIGN TRADEOFFS

-   Custom state machine instead of LangGraph (allowed per
    specification)
-   DuckDB instead of Postgres (lightweight, reproducible)
-   CLI interface instead of Streamlit (explicitly allowed)
