# Home assignment: Evaluation pipeline for coding-agent experiments

**What**: Home assignment.

**Where**: Nebius Academy course [AI Performance Engineering](https://academy.nebius.com/ai-engineering-il), MLOps module, lecture #6, "End-to-end ML pipeline".

**Author**: Simon Karasik.

**Learning objective**: Get hands-on experience turning an ad-hoc coding-agent evaluation script into an automated, observable, versioned, and durable Airflow pipeline with a structured data footprint: datasets, artifacts, metadata, metrics, logs, and trajectories.

**Inspired by**: https://github.com/GlebBerjoskin/mlops-assignment

---

## Legend

Imagine you are an MLOps engineer on a team that builds better coding agents. Think Claude Code, Codex, Cursor, OpenCode, mini-swe-agent, and similar systems.

Agent quality depends on two broad things:

1. **Harness**: the agent loop, prompts, tools, skills, retries, subagents, context management, and execution environment.
2. **Model**: the LLM that powers the harness, including decoding parameters and fine-tuned variants.

Your researchers want to experiment with both. Typical research loops look like this:

1. tweak a prompt or harness setting -> run the agent -> evaluate generated patches
2. fine-tune a model -> deploy it -> run the agent -> evaluate generated patches

Quality is measured on [SWE-bench](https://www.swebench.com/)-like tasks: the agent receives a real GitHub issue inside an isolated environment, tries to solve it, produces a patch, and the patch is judged by real unit tests.

Right now the researchers have several scripts on one VM. Someone SSHes in, runs them by hand, waits, copies logs, and pastes numbers into a doc. One experiment at a time. No queue. No durable run history. No reliable way to answer "which config produced this result?" or "why did this run fail?"

So, the team needs your help to turn these ad-hoc scripts into reliable, multi-user pipelines.

## Task

You are provided with ad-hoc scripts in `scripts/` to run [mini-swe-agent](https://github.com/SWE-agent/mini-swe-agent) and evaluate the results using [SWE-bench](https://github.com/swe-bench/SWE-bench).

Sample outputs of `scripts/mini-swe-bench-batch.sh` and `scripts/swe-bench-eval.sh` are available in `sample/`.

Your goal is to turn these ad-hoc scripts from `scripts/` into a proper, configurable Airflow pipeline that implements the basic  `run-agent -> run-evaluation` workflow: run `mini-swe-agent` on a subset of SWE-bench instances and evaluate the results.

As a starting point with Airflow, you are provided with `run-airflow-standalone.sh` and a dag in `dags/` that re-implements `scripts/mini-swe-bench-single.sh`.

**Airflow pipeline requirements**:
- Configurable from Airflow parameters. Required params: `split`, `subset`, `workers`. Useful optional params: `model`, `task_slice`, `run_id`, and `cost_limit`. No hard-code for experiment values.
- All run artifacts are properly structured. E.g.,
```
runs/
  <<run-id>>/
    config.json
    run-agent/
      astropy__astropy-12907/
      preds.json
    run-eval/
    metrics.json
    manifest.json
```
- It's possible to re-construct the run based on the produced `<<run-id>>` folder: input SWE-bench tasks, configuration, output trajectories, predictions, evaluation logs, metrics, etc. Basically, you can just send a directory to someone -- and they will be able to grab the whole picture.
- Each run metrics and parameters are logged to `MLflow`, one can easily compare different runs.
- The easy-mode solution may call the scripts from Airflow with Python/Bash tasks. For a production-style solution, use `DockerOperator` to run the scripts in isolated environments. `Dockerfile` for the project is provided. In large-scale production, `DockerOperator` can be replaced with `KubernetesPodOperator`.
- For the full solution, run artifacts are saved to remote long-term storage, such as Object Storage (S3). If you skip remote storage in the first iteration, still write a clear local `runs/<run-id>/` folder and document how it would be uploaded.

**Deployment**
1. Easy mode: run Airflow with `run-airflow-standalone.sh` and focus on making the DAG configurable and reproducible.
2. Production-style mode: deploy Airflow and MLflow locally on the VM using `docker compose`: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#running-airflow-in-docker
3. MLflow should be reachable from the VM and used by the DAG to log parameters, metrics, and artifact references.

Ultimately, the pipeline may look like: `run-mini-swe-agent` -> `swe-bench-eval` -> `log-artifacts-to-s3` -> `log-metrics-to-mlflow`.

## Suggested Implementation Path

You do not need to become a mini-swe-agent or SWE-bench expert to finish the first useful iteration. The cloned upstream repositories are there for debugging and deeper understanding. The fastest path is to finish the orchestration skeleton first, then improve isolation and deployment.

### Phase 1: Speedrun Working DAG

Goal: one Airflow button starts a small SWE-bench batch, evaluates it, and writes a reproducible run directory.

Start from `dags/mini-swe-bench-single.py` and create `dags/evaluate_agent.py`, or extend the existing DAG. Keep the first version simple and explicit:

1. `prepare_run`: read Airflow params and create `runs/<run-id>/config.json`.
2. `run_agent`: run `scripts/mini-swe-bench-batch.sh` or an equivalent helper with the selected params, and write trajectories plus `preds.json` to `runs/<run-id>/run-agent/`.
3. `run_eval`: run `scripts/swe-bench-eval.sh` using that `preds.json`, and write SWE-bench logs and reports to `runs/<run-id>/run-eval/`.
4. `summarize_and_log`: parse evaluation reports, write `runs/<run-id>/metrics.json`, and log params, metrics, and the artifact path to MLflow.

Recommended helper functions for easy mode:

```python
build_run_config(params) -> dict
prepare_run_dir(run_config) -> Path
run_agent_batch(run_config, run_dir) -> Path
run_swebench_eval(run_config, preds_path, run_dir) -> Path
collect_metrics(eval_dir) -> dict
log_mlflow_run(run_config, metrics, artifact_uri) -> None
```

You may implement these helpers directly inside the DAG at first. If the code grows, move them into `src/` or `pipeline/`.

Minimum Airflow params: `split`, `subset`, `workers`, `model`, `task_slice`, `run_id`, `cost_limit`.

### Phase 2: Make The Run Durable

Goal: a teammate can understand the whole run from one folder or one artifact URI.

Make sure every run produces this shape:

```text
runs/<run-id>/
  config.json
  run-agent/
    preds.json
    trajectories/
  run-eval/
    logs/
    reports/
  metrics.json
  manifest.json
```

`manifest.json` should point to the important files and record where full artifacts live. For full credit, upload the run folder or a compressed copy to Object Storage (S3) and log that URI to MLflow.

### Phase 3: Production Polish

After the speedrun works, improve the engineering around it:

- Replace direct subprocess calls with `DockerOperator` tasks that use the provided `Dockerfile`.
- Run Airflow and MLflow through `docker-compose.yaml`.
- Add sensible retries and timeouts around agent, evaluation, upload, and MLflow logging steps.
- Keep inspecting the cloned mini-swe-agent and SWE-bench repositories only when you need to understand trajectory format, prediction format, or evaluation output.

---

## Why This Matters

By the end of the assignment you should be able to:

- Model an ML experiment as a pipeline with explicit inputs, outputs, retries, and dependencies.
- Use Airflow for orchestration instead of manual shell ordering.
- Track experiment configs, datasets, model IDs, metrics, artifacts, and logs in MLflow.
- Run coding-agent evaluations in reproducible execution environments, with Docker images as the production-style path.
- Inspect mini-swe-agent trajectories to understand what happened inside an agent run.
- Compare multiple experiments without losing track of which code, prompt, dataset, and model produced each result.

If done carefully, this assignment teaches the practical MLOps discipline that research code usually lacks: durability, repeatability, provenance, and operational visibility.

---

## Prerequisites

- A CPU VM with 8 CPU, 32 GB RAM, public IP. Can be created in Nebius.
- `NEBIUS_API_KEY` for Nebius Token Factory

You do not need a GPU VM for the orchestration parts. The inference part is handled by managed APIs.

Create a VM with 8 CPU, 32 GB RAM, public IP. Add your public SSH key.

For simplicity, add this VM to your `~/.ssh/config`, for instance:

```
Host sbkarasik-academy-playground
  HostName 89.169.100.8
  User sbkarasik
  ForwardAgent yes
```

Connect to the VM. 

Install the basic tools:
```bash
# uv 
curl -LsSf https://astral.sh/uv/install.sh | sh

# Docker
# Add Docker's official GPG key:
sudo apt update
sudo apt install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# Add the repository to Apt sources:
sudo tee /etc/apt/sources.list.d/docker.sources <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}")
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc
EOF

sudo apt update

sudo apt install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Let your user use `docker` without `sudo`
sudo usermod -aG docker "$USER"
sudo newgrp docker
```

Set up the starter repo:

```bash
git clone <repo-url>
cd <repo-folder>
cp .env.example .env
```

Clone the upstream reference repositories as well:

```bash
cd ..
git clone https://github.com/SWE-agent/mini-swe-agent.git
git clone https://github.com/swe-bench/SWE-bench.git
cd <repo-folder>
```

These repositories are not meant to be used as-is in your final pipeline. They are reference material: read them to understand how `mini-swe-agent` writes trajectories, how SWE-bench expects predictions, and how the evaluation harness produces reports and logs.

Install the dependencies:
```bash
uv sync
```

Activate the venv: `source .venv/bin/activate`.

Add your `NEBIUS_API_KEY` to `.env`.

**Check your setup**:
- Run the script: `bash scripts/mini-swe-bench-single.sh`
- Via Airflow:
  - Run the Airflow: `bash run-airflow-standalone`
  - Forward port `8080` -- this is where Airflow is running.
    - VSCode/Cursor may do it automatically for you.
    - Plain SSH: `ssh -L 8080:localhost:8080 <user>@<vm-host>`.
  - Open it: http://localhost:8080
  - Try running the example DAG `mini-swe-bench-single`.


Congratulations! You are all set.

## Final Deliverables

By the end of the mandatory assignment, your repo should contain enough code and evidence for someone else to run a small evaluation and understand the result.

### Minimum Working Submission

| File or directory | What to add or finish |
|---|---|
| `dags/evaluate_agent.py` or an updated DAG in `dags/` | A configurable Airflow DAG with `prepare_run`, `run_agent`, `run_eval`, and `summarize_and_log` tasks |
| Airflow params | At minimum: `split`, `subset`, and `workers`; optional but useful: `model`, `task_slice`, `run_id`, `cost_limit` |
| `scripts/mini-swe-bench-batch.sh` or wrapper code | A way for the DAG to run mini-swe-agent with DAG-provided params and write outputs into `runs/<run-id>/run-agent/` |
| `scripts/swe-bench-eval.sh` or wrapper code | A way for the DAG to evaluate the produced `preds.json` and write logs/reports into `runs/<run-id>/run-eval/` |
| `runs/<run-id>/` sample or manifest | A reproducible run folder containing `config.json`, predictions, trajectories or trajectory references, evaluation logs/reports, `metrics.json`, and `manifest.json` |
| MLflow run | Logged params, metrics, `run_id`, and artifact path or artifact URI for at least one completed evaluation |
| `REPORT.md` | Short writeup with architecture, how to trigger the DAG, artifact layout, MLflow link/screenshot, one completed run, and rerun instructions |

This is the speedrun path: complete the existing scaffold, make the DAG configurable, save the outputs in one place, and log enough to MLflow to compare runs.

### Production-Style Additions

| File or directory | What it adds |
|---|---|
| `Dockerfile` | Repeatable execution environment for agent and evaluation steps |
| `DockerOperator` usage in the DAG | Isolated execution instead of local subprocess calls |
| `docker-compose.yaml` | Docker Compose deployment for Airflow and MLflow on the VM |
| `.env.example` | Non-secret environment template for Airflow, MLflow, Object Storage, and inference credentials |
| S3/Object Storage upload | Long-term storage for full `runs/<run-id>/` artifacts |
| `screenshots/airflow_dag.png` | Airflow UI showing the completed evaluation pipeline |
| `screenshots/mlflow_runs.png` | MLflow UI showing logged evaluation runs and metrics |
| `screenshots/object_storage_artifacts.png` | Object Storage UI, CLI output, or equivalent evidence showing uploaded run artifacts |

If full artifacts are too large to commit, commit a small manifest or example folder and include the remote artifact URI in `REPORT.md`.

---

## Grading

We care more about engineering judgment and traceability than about one lucky metric. A weak result with excellent provenance and analysis is better than a pasted number nobody can reproduce.

| Area | Weight | What a strong submission shows |
|---|---:|---|
| **Configurable Airflow DAG** | 35% | The DAG implements the `run-agent -> run-evaluation` workflow, exposes `split`, `subset`, and `workers` as parameters, avoids hard-coded experiment values, and can be triggered reliably from the Airflow UI. A strong standalone Airflow solution is acceptable here. |
| **Artifact structure and reproducibility** | 20% | Each run writes a structured `runs/<run-id>/` tree and includes enough inputs, outputs, trajectories, predictions, logs, and reports to reconstruct the run. Extra credit within this area for uploading artifacts to S3/Object Storage. |
| **MLflow tracking** | 15% | Runs log parameters, metrics, run IDs, and artifact references so multiple evaluations can be compared in the MLflow UI. |
| **Execution isolation** | 10% | Agent and evaluation work run in a documented, repeatable environment. `DockerOperator` with the project `Dockerfile` is the preferred production-style solution, but a clear standalone Airflow implementation without `DockerOperator` can still receive most of the credit if it is reproducible. |
| **Docker Compose deployment** | 10% | Airflow and MLflow can run from `docker-compose.yaml` with documented setup and required environment variables. The Compose deployment should support the pipeline rather than become the main point of the assignment. |
| **Report and reproducibility** | 10% | `REPORT.md` explains the architecture, how to trigger a run, where artifacts live, how to rerun by `run-id`, and what happened in at least one completed evaluation. |

