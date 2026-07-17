import json
import subprocess
from datetime import datetime
from pathlib import Path
import os
import shutil
#import mlflow

from airflow.sdk import dag, task, Param

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNS_ROOT = PROJECT_ROOT / "runs"


@dag(
    dag_id="evaluate_agent",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "split": Param("test", type="string"),
        "subset": Param("verified", type="string"),
        "workers": Param(4, type="integer"),
        "model": Param("nebius/moonshotai/Kimi-K2.6", type="string"),
        "task_slice": Param("0:2", type="string"),
        "run_id": Param("", type="string"),   # empty -> auto-generate
        "cost_limit": Param(3, type="number"),
    },
)
def evaluate_agent():

    @task
    def prepare_run(**context) -> dict:
        """Resolve params into a run config, create runs/<run-id>/, write config.json."""
        p = dict(context["params"])
        if not p["run_id"]:
            p["run_id"] = datetime.now().strftime("run-%Y%m%d-%H%M%S")
        run_dir = RUNS_ROOT / p["run_id"]
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "config.json").write_text(json.dumps(p, indent=2))
        return p  # goes to XCom; downstream tasks receive it

    @task
    def run_agent(cfg: dict) -> str:
        agent_dir = RUNS_ROOT / cfg["run_id"] / "run-agent"
        agent_dir.mkdir(parents=True, exist_ok=True)

        subprocess.run(
            [  # 1. argv: the command itself
                "uv", "run", "mini-extra", "swebench",
                "--subset", cfg["subset"],
                "--split", cfg["split"],
                "--model", cfg["model"],
                "--slice", cfg["task_slice"],
                "--workers", str(cfg["workers"]),
                "-o", str(agent_dir),
            ],
            cwd=PROJECT_ROOT,  # 2. directory to run it from
            env={  # 3. environment variables for the child process
                **os.environ,
                "MSWEA_COST_TRACKING": "ignore_errors",
                "MSWEA_GLOBAL_COST_LIMIT": str(cfg["cost_limit"]),
            },
            check=True,  # 4. raise if exit code != 0
        )
        return str(agent_dir / "preds.json")

    @task
    def run_eval(cfg: dict, preds_path: str) -> str:
        """TODO: call swebench harness on preds_path,
        logs/reports under runs/<run-id>/run-eval/. Return eval dir."""
        eval_dir = RUNS_ROOT / cfg["run_id"] / "run-eval"
        eval_dir.mkdir(parents=True, exist_ok=True)

        dataset={
            "verified": "princeton-nlp/SWE-bench_Verified",
            "lite": "princeton-nlp/SWE-bench_Lite",
            "full": "princeton-nlp/SWE-bench",
        }
        subprocess.run(
            [  # 1. argv: the command itself
                "uv",
                "run",
                "python", "-m",
                "swebench.harness.run_evaluation",
                "--dataset_name", dataset[str(cfg["subset"])],
                "--predictions_path", preds_path,
                "--split", cfg["split"],
#                "--model", cfg["model"],
                "--max_workers", str(cfg["workers"]),
                "--run_id", cfg["run_id"],
                "--report_dir", str(eval_dir),
            ],
            cwd=PROJECT_ROOT,  # 2. directory to run it from
            check=True,  # 4. raise if exit code != 0
        )
        # The harness writes relative to cwd — relocate outputs into the run folder.
        report_name = f"{cfg['model'].replace('/', '__')}.{cfg['run_id']}.json"

        src_report = PROJECT_ROOT / report_name
        if src_report.exists():
            shutil.move(str(src_report), str(eval_dir / report_name))

        src_logs = PROJECT_ROOT / "logs" / "run_evaluation" / cfg["run_id"]
        if src_logs.exists():
            shutil.move(str(src_logs), str(eval_dir / "logs"))
        return str(eval_dir)

    def _aggregate_from_instance_reports(eval_dir: Path) -> dict:
        """Fallback: scan per-instance report.json files"""
        total = 0
        resolved = 0
        applied = 0
        resolved_ids = []

        for report_file in eval_dir.rglob("report.json"):
            try:
                data = json.loads(report_file.read_text())
            except Exception as e:
                print(f"WARNING: skipping unreadable {report_file}: {e}")
                continue
            # Shape: {instance_id: {resolved, patch_successfully_applied, tests_status, ...}}
            for inst_id, inst_data in data.items():
                total += 1
                if inst_data.get("resolved") is True:
                    resolved += 1
                    resolved_ids.append(inst_id)
                if inst_data.get("patch_successfully_applied") is True:
                    applied += 1

        return {
            "total_instances": total,
            "submitted_instances": total,
            "resolved_instances": resolved,
            "applied_instances": applied,
            "resolved_ids": resolved_ids,
        }

    @task
    def summarize_and_log(cfg: dict, eval_dir: str) -> str:
        """Parse SWE-bench evaluation reports and write runs/<run-id>/metrics.json"""
        # mlflow.log_metrics({
        #     "resolved": metrics["resolved"],
        #     "resolution_rate": metrics["resolution_rate"],
        # })
        # mlflow.log_artifact(str(metrics_path))
        run_id = cfg["run_id"]
        run_dir = RUNS_ROOT / run_id
        metrics_path = run_dir / "metrics.json"

        eval_dir = Path(eval_dir)

        # Primary: the aggregate report the harness writes to --report_dir.
        # Deterministic name: slashes in the model name become "__".
        report_name = f"{cfg['model'].replace('/', '__')}.{run_id}.json"
        report_path = eval_dir / report_name


        # Look for the main summary report (most common locations)
        report_candidates = [
            eval_dir / "results.json",  # common top-level
            eval_dir / f"{cfg['model'].replace('/', '__')}.{run_id}.json",
            list(eval_dir.glob("*.json"))[0] if list(eval_dir.glob("*.json")) else None,
        ]

        if report_path.exists():
            report = json.loads(report_path.read_text())
            print(f"Parsed aggregate report: {report_path}")
        else:
            print(f"Aggregate report {report_path} not found; "
                  f"falling back to per-instance reports")
            report = _aggregate_from_instance_reports(eval_dir)
            report_path = None

        submitted = report.get("submitted_instances", report.get("total_instances", 0))
        resolved = report.get("resolved_instances", 0)

        # Build clean metrics
        metrics = {
            # provenance
            "run_id": run_id,
            "model": cfg["model"],
            "subset": cfg["subset"],
            "split": cfg["split"],
            "task_slice": cfg["task_slice"],
            "timestamp": datetime.now().isoformat(),
            "raw_report_path": str(report_path) if report_path else None,
            # headline metric
            "resolution_rate": round(resolved / submitted * 100, 2) if submitted else 0.0,
            # all scalar counts from the report (ID lists stay in the report artifact)
            **{k: v for k, v in report.items() if isinstance(v, (int, float))},
        }

        metrics_path.write_text(json.dumps(metrics, indent=2))
        print(f"Metrics written to {metrics_path}")
        print(f"Resolved: {resolved}/{submitted} ({metrics['resolution_rate']}%)")
        return str(metrics_path)

    cfg = prepare_run()
    preds = run_agent(cfg)
    ev = run_eval(cfg, preds)
    summarize_and_log(cfg, ev)

evaluate_agent()
