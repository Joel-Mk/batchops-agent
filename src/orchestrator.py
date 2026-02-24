import os
import json
from datetime import datetime

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

    for stage in STAGES:
        print(f"Running stage: {stage}")

        state["current_stage"] = stage
        write_json(os.path.join(run_path, "state.json"), state)

        # Placeholder execution
        print(f"{stage} completed.")

    state["status"] = "completed"
    write_json(os.path.join(run_path, "state.json"), state)

    print(f"Run {run_id} completed successfully.")


def resume_pipeline(run_id):
    run_path = os.path.join("runs", run_id)

    if not os.path.exists(run_path):
        print("Run ID not found.")
        return

    with open(os.path.join(run_path, "state.json")) as f:
        state = json.load(f)

    last_stage = state["current_stage"]
    start_index = STAGES.index(last_stage) + 1

    print(f"Resuming from stage after: {last_stage}")

    for stage in STAGES[start_index:]:
        print(f"Running stage: {stage}")