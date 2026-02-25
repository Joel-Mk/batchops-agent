import os
import json
from datetime import datetime

from src.ingestion import run_ingestion
from src.validation import run_validation


STAGES = [
    "ingestion",
    "validation",
    "import",
    "feature_etl",
    "train",
    "inference"
]


def create_run_folder():
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_path = os.path.join("runs", run_id)
    os.makedirs(run_path, exist_ok=True)
    return run_id, run_path


def write_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def run_pipeline(provider, pipeline, snapshot_date):

    run_id, run_path = create_run_folder()

    # Create execution plan
    plan = {
        "provider": provider,
        "pipeline": pipeline,
        "snapshot_date": snapshot_date,
        "stages": STAGES
    }

    write_json(os.path.join(run_path, "plan.json"), plan)

    state = {
        "current_stage": None,
        "status": "running"
    }

    write_json(os.path.join(run_path, "state.json"), state)

    data = None  # will hold ingestion output

    for stage in STAGES:

        print(f"\nRunning stage: {stage}")

        state["current_stage"] = stage
        write_json(os.path.join(run_path, "state.json"), state)

        # ---- Stage Execution ----

        if stage == "ingestion":
            data = run_ingestion(provider)
            print("Ingestion completed.")

        elif stage == "validation":
            validation_status = run_validation(data, run_path)

            if validation_status == "FAIL":
                print("Validation failed. Stopping pipeline.")
                state["status"] = "failed"
                write_json(os.path.join(run_path, "state.json"), state)
                return

            print("Validation passed.")

        elif stage == "import":
            print("Import completed (placeholder).")

        elif stage == "feature_etl":
            print("Feature ETL completed (placeholder).")

        elif stage == "train":
            print("Training completed (placeholder).")

        elif stage == "inference":
            print("Inference completed (placeholder).")

        else:
            raise ValueError(f"Unknown stage: {stage}")

    state["status"] = "completed"
    write_json(os.path.join(run_path, "state.json"), state)

    print(f"\nRun {run_id} completed successfully.")


def resume_pipeline(run_id):

    run_path = os.path.join("runs", run_id)

    if not os.path.exists(run_path):
        print("Run ID not found.")
        return

    with open(os.path.join(run_path, "state.json")) as f:
        state = json.load(f)

    last_stage = state["current_stage"]

    if last_stage not in STAGES:
        print("Invalid stage in state file.")
        return

    start_index = STAGES.index(last_stage) + 1

    print(f"Resuming from stage after: {last_stage}")

    for stage in STAGES[start_index:]:
        print(f"Running stage: {stage}")