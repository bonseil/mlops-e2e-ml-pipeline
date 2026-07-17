"""Log a completed evaluation run to MLflow. Called by the DAG via `uv run`."""
import argparse
import json
from pathlib import Path
import mlflow

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True)
    args = ap.parse_args()
    run_dir = Path(args.run_dir)

    cfg = json.loads((run_dir / "config.json").read_text())
    metrics = json.loads((run_dir / "metrics.json").read_text())

    mlflow.set_experiment("agent-evals")
    with mlflow.start_run(run_name=cfg["run_id"]):
        mlflow.log_params({k: cfg[k] for k in
            ["run_id", "model", "subset", "split", "task_slice", "workers", "cost_limit"]})
        mlflow.log_metrics({k: metrics[k] for k in
            ["submitted_instances", "resolved_instances", "resolution_rate"]})
        mlflow.set_tag("artifact_path", str(run_dir))
        for f in ["config.json", "metrics.json", "manifest.json"]:
            mlflow.log_artifact(str(run_dir / f))

if __name__ == "__main__":
    main()