import json
from datetime import datetime, timedelta
from pathlib import Path
import os
from airflow.providers.docker.operators.docker import DockerOperator
from botocore.exceptions import ClientError
from docker.types import Mount
from airflow.sdk import dag, task, Param
import mlflow

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNS_ROOT = PROJECT_ROOT / "runs"                 # host world
CONTAINER_RUNS = "/mlops-assignment/runs"         # container world
HOST_PROJECT_DIR = os.environ.get("HOST_PROJECT_DIR", str(PROJECT_ROOT))
HOST_RUNS = HOST_PROJECT_DIR + "/runs"

def _read_env_key(name: str) -> str:
    env_file = PROJECT_ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line.startswith("export "):
                line = line[len("export "):]
            if "=" in line:
                k, v = line.split("=", 1)
                if k.strip() == name:
                    return v.strip().strip("'\"")
    return os.environ.get(name, "")

NEBIUS_API_KEY = _read_env_key("NEBIUS_API_KEY")
HF_TOKEN = _read_env_key("HF_TOKEN")
if not NEBIUS_API_KEY:
    raise ValueError("NEBIUS_API_KEY missing: set it in .env or the environment")

class TemplatedDockerOperator(DockerOperator):
    template_fields = (*DockerOperator.template_fields, "working_dir")

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
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
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
        (run_dir / "run-eval").mkdir(exist_ok=True)
        DATASETS = {
            "verified": "princeton-nlp/SWE-bench_Verified",
            "lite": "princeton-nlp/SWE-bench_Lite",
            "full": "princeton-nlp/SWE-bench",
        }
        p["dataset_name"] = DATASETS[p["subset"]]  # KeyError = loud, good

        (run_dir / "config.json").write_text(json.dumps(p, indent=2))
        return p  # goes to XCom; downstream tasks receive it

    run_agent = DockerOperator(
        task_id="run_agent",
        image="mlops-eval:latest",
        command=[
            "mini-extra","swebench",
            "--subset","{{ ti.xcom_pull(task_ids='prepare_run')['subset'] }}",
            "--split","{{ ti.xcom_pull(task_ids='prepare_run')['split'] }}",
            "--model","{{ ti.xcom_pull(task_ids='prepare_run')['model'] }}",
            "--slice","{{ ti.xcom_pull(task_ids='prepare_run')['task_slice'] }}",
            "--workers","{{ ti.xcom_pull(task_ids='prepare_run')['workers'] }}",
            "-o",CONTAINER_RUNS +"/{{ ti.xcom_pull(task_ids='prepare_run')['run_id'] }}/run-agent",
        ],
        environment={
            "MSWEA_COST_TRACKING": "ignore_errors",
            "MSWEA_GLOBAL_COST_LIMIT": "{{ ti.xcom_pull(task_ids='prepare_run')['cost_limit'] }}",
        },
        private_environment={
            "NEBIUS_API_KEY": NEBIUS_API_KEY,
            "HF_TOKEN": HF_TOKEN
        },
        mounts=[
            Mount(source=str(HOST_RUNS), target="/mlops-assignment/runs", type="bind"),
            Mount(
                source='/var/run/docker.sock',
                target="/var/run/docker.sock",
                type="bind",
            ),
        ],
        mount_tmp_dir=False,
        auto_remove="success",
        retries=0,
        execution_timeout=timedelta(hours=1),
    )

    XC = "{{ ti.xcom_pull(task_ids='prepare_run')"  # readability helper

    run_eval = TemplatedDockerOperator(
        task_id="run_eval",
        image="mlops-eval:latest",
        command=[
            "python",
            "-m",
            "swebench.harness.run_evaluation",
            "--dataset_name",
            XC + "['dataset_name'] }}",
            "--predictions_path",
            CONTAINER_RUNS + "/" + XC + "['run_id'] }}/run-agent/preds.json",
            "--split",
            XC + "['split'] }}",
            "--max_workers",
            XC + "['workers'] }}",
            "--run_id",
            XC + "['run_id'] }}",
        ],
        working_dir=CONTAINER_RUNS + "/" + XC + "['run_id'] }}/run-eval",
        mounts=[
            Mount(source=str(HOST_RUNS), target=CONTAINER_RUNS, type="bind"),
            Mount(
                source="/var/run/docker.sock",
                target="/var/run/docker.sock",
                type="bind",
            ),
        ],
        private_environment={"HF_TOKEN": HF_TOKEN},
        mount_tmp_dir=False,
        auto_remove="success",
        execution_timeout=timedelta(hours=1),
    )

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

        if total == 0:
            raise FileNotFoundError(
                f"No per-instance report.json files found under {eval_dir}"
            )

        return {
            "total_instances": total,
            "submitted_instances": total,
            "resolved_instances": resolved,
            "applied_instances": applied,
            "resolved_ids": resolved_ids,
        }

    @task
    def summarize(cfg: dict) -> str:
        run_id = cfg["run_id"]
        run_dir = RUNS_ROOT / run_id
        metrics_path = run_dir / "metrics.json"

        eval_dir = RUNS_ROOT / run_id / "run-eval"

        # Primary: the aggregate report the harness writes to --report_dir.
        # Deterministic name: slashes in the model name become "__".
        report_name = f"{cfg['model'].replace('/', '__')}.{run_id}.json"
        report_path = eval_dir / report_name

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

        def rel(p):  # store paths relative to run_dir -> folder stays portable
            return str(Path(p).relative_to(run_dir))

        manifest = {
            "run_id": run_id,
            "created_at": datetime.now().isoformat(),
            "config": cfg,  # full resolved params, inline
            "files": {
                "config": "config.json",
                "predictions": rel(run_dir / "run-agent" / "preds.json"),
                "trajectories_dir": "run-agent",
                "eval_report": rel(report_path) if report_path else None,
                "eval_logs_dir": "run-eval/logs/run_evaluation/"+run_id,
                "metrics": "metrics.json",
            },
            "instances": {
                "submitted": submitted,
                "resolved_ids": report.get("resolved_ids", []),
                "unresolved_ids": report.get("unresolved_ids", []),
            },
            "storage": {
                "local_path": str(run_dir),
                "remote_uri": None,  # S3 task fills this later
            },
        }
        (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
        return str(metrics_path)


    @task
    def upload_to_s3(cfg: dict) -> str:
        import boto3

        run_id = cfg["run_id"]
        run_dir = RUNS_ROOT / run_id
        bucket = os.environ["S3_BUCKET"]
        uri = f"s3://{bucket}/runs/{run_id}/"

        # we record destiny in the manifest BEFORE uploading, so the
        #    uploaded manifest already points at its remote home
        manifest_path = run_dir / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        manifest["storage"]["remote_uri"] = uri
        manifest_path.write_text(json.dumps(manifest, indent=2))

        # we walk run_dir, upload each file under runs/<run_id>/<relpath>
        s3 = boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT_URL"])
        try:
            s3.head_bucket(Bucket=bucket)  # exists and accessible?
        except ClientError:
            s3.create_bucket(Bucket=bucket)  # no -> create it
            print(f"Created bucket: {bucket}")
        for path in run_dir.rglob("*"):
            if path.is_file():
                key = f"runs/{run_id}/{path.relative_to(run_dir)}"
                s3.upload_file(str(path), bucket, key)
        return uri

    @task
    def log_to_mlflow(cfg: dict, remote_uri:str) -> None:
        run_id = cfg["run_id"]
        run_dir = RUNS_ROOT / run_id
        metrics = json.loads((run_dir / "metrics.json").read_text())

        mlflow.set_experiment("agent-evals")
        with mlflow.start_run(run_name=run_id):
            mlflow.log_params({k: cfg[k] for k in
                ["run_id", "model", "subset", "split", "task_slice", "workers", "cost_limit"]})
            mlflow.log_metrics({k: metrics[k] for k in
                ["submitted_instances", "resolved_instances", "resolution_rate"]})
            mlflow.set_tag("artifact_path", str(run_dir))
            mlflow.set_tag("remote_uri", remote_uri)
            for f in ["config.json", "metrics.json", "manifest.json"]:
                mlflow.log_artifact(str(run_dir / f))

    cfg = prepare_run()
    summary = summarize(cfg)
    uri = upload_to_s3(cfg)
    logged = log_to_mlflow(cfg, uri)
    cfg >> run_agent >> run_eval >> summary >> uri >> logged
evaluate_agent()
