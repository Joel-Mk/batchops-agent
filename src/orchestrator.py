import os
import json
from datetime import datetime

from src.ingestion import run_ingestion
from src.validation import run_validation
from src.import_layer import run_import
from src.feature_engineering import run_feature_etl
from src.train import run_train
from src.inference import run_inference


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


def append_log(run_path, stage, status, message=""):
    log_entry = {
        "timestamp": datetime.now().isoformat(),
        "stage": stage,
        "status": status,
        "message": message
    }

    log_path = os.path.join(run_path, "logs.jsonl")
    with open(log_path, "a") as f:
        f.write(json.dumps(log_entry) + "\n")


def run_pipeline(provider, pipeline, snapshot_date):

    run_id, run_path = create_run_folder()

    # ---- Execution Plan ----
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

    data = None
    train_output = None

    # ---- Stage Loop ----
    for stage in STAGES:

        print(f"\nRunning stage: {stage}")
        append_log(run_path, stage, "started")

        state["current_stage"] = stage
        write_json(os.path.join(run_path, "state.json"), state)

        if stage == "ingestion":
            data = run_ingestion(provider)
            print("Ingestion completed.")
            append_log(run_path, stage, "completed")

        elif stage == "validation":
            validation_status = run_validation(data, run_path)

            if validation_status == "FAIL":
                print("Validation failed. Stopping pipeline.")
                append_log(run_path, stage, "failed")

                state["status"] = "failed"
                write_json(os.path.join(run_path, "state.json"), state)
                return

            print("Validation passed.")
            append_log(run_path, stage, "completed")

        elif stage == "import":
            run_import(data, run_path)
            print("Import completed.")
            append_log(run_path, stage, "completed")

        elif stage == "feature_etl":
            run_feature_etl(run_path, snapshot_date)
            print("Feature ETL completed.")
            append_log(run_path, stage, "completed")

        elif stage == "train":
            train_output = run_train(run_path)
            print("Training completed.")
            append_log(run_path, stage, "completed")

        elif stage == "inference":
            run_inference(
                model=train_output["model"],
                X_test=train_output["X_test"],
                customer_ids=train_output["customer_ids_test"],
                run_path=run_path
            )
            print("Inference completed.")
            append_log(run_path, stage, "completed")

        else:
            raise ValueError(f"Unknown stage: {stage}")

    # ---- Mark Completed ----
    state["status"] = "completed"
    write_json(os.path.join(run_path, "state.json"), state)

    # ---- Generate Run Summary ----
    summary_path = os.path.join(run_path, "run_summary.md")

    with open(summary_path, "w") as f:
        f.write("# Run Summary\n\n")
        f.write(f"**Provider:** {provider}\n\n")
        f.write(f"**Pipeline:** {pipeline}\n\n")
        f.write(f"**Snapshot Date:** {snapshot_date}\n\n")
        f.write(f"**Status:** completed\n\n")

        metrics_path = os.path.join(run_path, "metrics.json")
        if os.path.exists(metrics_path):
            with open(metrics_path) as mf:
                metrics_data = json.load(mf)

            f.write("## Metrics\n")
            for k, v in metrics_data.items():
                f.write(f"- {k}: {v}\n")

    print(f"\nRun {run_id} completed successfully.")


def resume_pipeline(run_id):

    run_path = os.path.join("runs", run_id)

    if not os.path.exists(run_path):
        print("Run ID not found.")
        return

    # ---- Load state and plan ----
    with open(os.path.join(run_path, "state.json")) as f:
        state = json.load(f)

    with open(os.path.join(run_path, "plan.json")) as f:
        plan = json.load(f)

    last_stage = state["current_stage"]

    if last_stage not in STAGES:
        print("Invalid stage in state file.")
        return

    start_index = STAGES.index(last_stage) + 1

    print(f"Resuming run {run_id} from stage after: {last_stage}")

    provider = plan["provider"]
    pipeline = plan["pipeline"]
    snapshot_date = plan["snapshot_date"]

    # ---- Dependency-aware ingestion rebuild ----
    # Re-run ingestion only if failure happened before or at import
    if last_stage in ["ingestion", "validation", "import"]:
        print("Rebuilding ingestion data...")
        data = run_ingestion(provider)
    else:
        data = None

    train_output = None

    # ---- Resume remaining stages ----
    for stage in STAGES[start_index:]:

        print(f"\nRunning stage: {stage}")
        append_log(run_path, stage, "started")

        state["current_stage"] = stage
        write_json(os.path.join(run_path, "state.json"), state)

        if stage == "validation":
            validation_status = run_validation(data, run_path)

            if validation_status == "FAIL":
                append_log(run_path, stage, "failed")
                state["status"] = "failed"
                write_json(os.path.join(run_path, "state.json"), state)
                return

            append_log(run_path, stage, "completed")

        elif stage == "import":
            run_import(data, run_path)
            append_log(run_path, stage, "completed")

        elif stage == "feature_etl":
            run_feature_etl(run_path, snapshot_date)
            append_log(run_path, stage, "completed")

        elif stage == "train":
            train_output = run_train(run_path)
            append_log(run_path, stage, "completed")

        elif stage == "inference":

            # If resume started at inference directly,
            # reload model + test data safely
            if train_output is None:
                print("Reloading model for inference...")
                train_output = run_train(run_path)

            run_inference(
                model=train_output["model"],
                X_test=train_output["X_test"],
                customer_ids=train_output["customer_ids_test"],
                run_path=run_path
            )

            append_log(run_path, stage, "completed")

    state["status"] = "completed"
    write_json(os.path.join(run_path, "state.json"), state)

    print(f"\nRun {run_id} resumed and completed successfully.")