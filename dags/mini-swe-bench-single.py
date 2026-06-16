import os
from datetime import datetime
from pathlib import Path
import subprocess

from airflow.decorators import dag, task

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dag(
    dag_id="mini-swe-bench-single",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
)
def my_dag():
    @task
    def run_script():
        subprocess.run(
            [
                "uv",
                "run",
                "mini-extra",
                "swebench-single",
                "--subset",
                "verified",
                "--split",
                "test",
                "--model",
                "nebius/moonshotai/Kimi-K2.6",
                "--yolo",
                "--cost-limit",
                "0",
                "-i",
                "sympy__sympy-15599",
                "-o",
                "trajectory.json",
            ],
            cwd=PROJECT_ROOT,
            env={
                **os.environ,
                "MSWEA_COST_TRACKING": "ignore_errors",
            },
        )

    run_script()


my_dag()
