# REPORT — Evaluation Pipeline for Coding-Agent Experiments

Nebius Academy, AI Performance Engineering — MLOps module, lecture \#6 home assignment. Author: Binyamin Singer.

## What this is

An Airflow pipeline that takes an ad-hoc "SSH in and run some scripts" workflow and turns it into something a team can actually share: press one button (with parameters), and [mini-swe-agent](https://github.com/SWE-agent/mini-swe-agent) runs on a slice of SWE-bench, the [SWE-bench harness](https://github.com/swe-bench/SWE-bench) grades the patches, every run leaves behind a self-describing artifact folder, a copy goes to S3-compatible object storage, and the whole thing lands in MLflow where runs can be compared side by side.

It works. The last section of this report is an honest tour of everything that broke along the way and what each breakage taught me.

## Architecture

One DAG, `dags/evaluate_agent.py`, six tasks:

prepare\_run \-\> run\_agent \-\> run\_eval \-\> summarize \-\> upload\_to\_s3 \-\> log\_to\_mlflow

| Task | Runs where | What it does |
| :---- | :---- | :---- |
| `prepare_run` | Airflow worker (Python `@task`) | Resolves Airflow params into a run config, auto-generates `run_id` if empty, maps `subset` to the HF dataset name, creates `runs/<run-id>/`, writes `config.json` |
| `run_agent` | `DockerOperator`, `mlops-eval` image | Runs `mini-extra swebench` with the configured model/slice/workers; writes trajectories and `preds.json` to `runs/<run-id>/run-agent/` |
| `run_eval` | `DockerOperator`, same image | Runs the SWE-bench harness on `preds.json`; `working_dir` is pointed inside the run folder so reports and logs land in `runs/<run-id>/run-eval/` by construction |
| `summarize` | Python `@task` | Parses the aggregate report (with a loud-failing fallback to per-instance reports), writes `metrics.json` and `manifest.json` |
| `upload_to_s3` | Python `@task` | Stamps the remote URI into the manifest, then uploads the whole run folder to object storage (creating the bucket if needed) |
| `log_to_mlflow` | Python `@task` | Logs params, metrics, artifact references, and the remote URI to MLflow — last in the chain, so the MLflow record is complete |

Design rules that emerged (each one has a scar attached — see the last section):

- **XCom carries identity, not paths.** Only the resolved config (with `run_id`) travels between tasks; every task derives its own paths from `run_id` plus the layout convention. Absolute paths are meaningless across the three worlds (host, Airflow container, task containers), so they never travel.  
- **`run_id` is resolved exactly once**, in `prepare_run`. Retries of any downstream task reuse the same identity; a rerun with the same `run_id` resumes (both mini-swe-agent and the harness skip completed work), and a fresh experiment means a fresh (auto-generated) `run_id`.  
- **Task boundaries are retry boundaries.** A failed eval or a hiccuping MLflow server can be cleared and re-run without re-paying for the expensive agent step.  
- **Everything fails loudly.** Container exit codes fail tasks natively; a missing report raises instead of producing zero-filled metrics; a missing API key fails the DAG *import*, not the third task of an expensive run.

### The three-worlds path problem

There are three filesystems in play: the host, the Airflow containers, and the task containers that DockerOperator launches. The same `runs/` directory has a different name in each:

| World | Path | Who uses it |
| :---- | :---- | :---- |
| Host | `$HOST_PROJECT_DIR/runs` | Docker daemon (bind-mount sources) |
| Airflow container | `/opt/airflow/runs` | `@task` Python code (`RUNS_ROOT`) |
| Task containers | `/mlops-assignment/runs` | Agent/eval commands (`CONTAINER_RUNS`) |

Bind-mount sources are resolved by the **host's** Docker daemon — the task containers are siblings of the Airflow containers, not children — so mount sources must be host paths, delivered via the `HOST_PROJECT_DIR` env var. Each constant is used only in the world whose daemon or filesystem interprets it.

## Configuration (Airflow params)

Everything experiment-specific is a parameter; nothing is hard-coded:

| Param | Default | Meaning |
| :---- | :---- | :---- |
| `split` | `test` | SWE-bench dataset split |
| `subset` | `verified` | Dataset subset; mapped to the HF dataset name in `prepare_run` |
| `workers` | 4 | Parallel workers for both agent and evaluation |
| `model` | `nebius/moonshotai/Kimi-K2.6` | Model used by mini-swe-agent |
| `task_slice` | `0:2` | Slice of the subset to run |
| `run_id` | `""` | Run identity; auto-generated timestamp when empty (recommended) |
| `cost_limit` | 3 | Whole-run budget in $, enforced via `MSWEA_GLOBAL_COST_LIMIT` |

Note on `cost_limit`: batch mode has no per-instance cost flag, so this is a *global* budget for the run. If it trips mid-batch the run fails, even though partial predictions exist; evaluating partial results is possible future work. The budget exists because of an early lesson: an uncapped single-instance run once spent 230+ steps rewriting the same file before I killed it by hand.

## Artifact layout

Every run produces one folder that is meant to survive being sent to a stranger:

runs/\<run-id\>/

  config.json                  \# full resolved input params

  manifest.json                \# table of contents \+ provenance \+ storage pointers

  metrics.json                 \# parsed evaluation metrics (scalars only)

  run-agent/

    preds.json                 \# predictions, the contract between agent and eval

    \<instance\_id\>/             \# per-instance trajectory

    minisweagent.log

  run-eval/

    \<model\>.\<run-id\>.json      \# aggregate harness report

    logs/run\_evaluation/\<run-id\>/   \# per-instance harness logs and reports

`manifest.json` inlines the config, points to every key file with *relative* paths (so the folder stays valid after being copied anywhere), lists resolved/unresolved instance IDs, and records both `local_path` and `remote_uri`. The acceptance test I used: open the folder cold and reconstruct the run — what was asked, what the agent did, what the tests said — with no other context. The manifest is stamped with the remote URI *before* upload, so the copy in object storage already knows its own address.

The separation of the three JSON files is deliberate: `config.json` answers "what was asked", `metrics.json` answers "what happened" in comparable scalars, `manifest.json` answers "where is everything". ID lists live in the manifest, not the metrics — metrics stay loggable and comparable.

### Rerunning by run\_id

- Reproduce a config: take `runs/<run-id>/config.json`, trigger the DAG with the same values. (Agent runs are nondeterministic — the same config once resolved an instance and once didn't — which is exactly why configs are recorded.)  
- Resume/repair: re-trigger with the *same* `run_id`; completed instances are skipped. Clear an individual task in the UI to re-run just that stage.  
- Inspect: start from `manifest.json`, follow the pointers.

## Deployment (docker-compose)

`docker-compose.yaml` runs the whole platform: Postgres (Airflow metadata), the Airflow services (api-server, scheduler, dag-processor, triggerer — Celery/Redis removed in favor of `LocalExecutor`, one VM doesn't need distributed workers), MLflow, and MinIO for object storage. The Airflow services use a custom image (`Dockerfile.airflow`: base Airflow \+ `mlflow`, `boto3`, docker provider), because task code runs inside them and needs its libraries there — the base image and my code's requirements are decoupled from the workload image (`Dockerfile`), which pins the agent/eval environment via `uv.lock`.

Details that matter:

- The Airflow containers mount `runs/` and the Docker socket, with `group_add: ${DOCKER_GID}` so DockerOperator can talk to the host daemon (the container-side rerun of "add your user to the docker group").  
- Inside the compose network, service names are DNS names: the DAG talks to `http://mlflow:5000` and `http://minio:9000`; `localhost` means "this container" and is only correct in a browser on the forwarded ports. MLflow's tracking URI is therefore configuration, not code.  
- MLflow's DNS-rebinding protection rejects the compose-internal hostname by default; the service is configured to allow `mlflow:5000` (allowlisting our internal name, rather than downgrading the server to make the error go away).  
- `restart: unless-stopped` on the stateful services — an earlier outage ("MLflow wasn't running because its terminal died") is now structurally impossible.  
- Secrets enter containers only at runtime via `.env` → compose `environment:` blocks; the images contain none (`.dockerignore` keeps `.env` out of build contexts). `.env.example` documents every variable with the command that produces the correct value.

### Object storage: Nebius vs. MinIO

The upload code targets any S3-compatible store via `S3_ENDPOINT_URL`. The original plan was Nebius Object Storage; my course account turned out not to have permission to grant the service account a write role. Rather than block, the compose stack includes MinIO as an S3-compatible stand-in — the pipeline code is identical, and switching to Nebius when permissions arrive is a two-variable change in `.env`. I consider this a feature demonstration rather than a workaround: the endpoint was configuration from the start precisely so the storage provider is swappable.

## MLflow tracking

Experiment `agent-evals`, one MLflow run per pipeline run, named by `run_id`. Logged: params (`run_id`, `model`, `subset`, `split`, `task_slice`, `workers`, `cost_limit`), metrics (`submitted_instances`, `resolved_instances`, `resolution_rate`), tags `artifact_path` and `remote_uri`, plus the three small JSONs as MLflow artifacts. Bulk artifacts (trajectories, eval logs) stay in the run folder and object storage — MLflow holds references, not gigabytes.

`resolution_rate` divides by *submitted* instances. The harness also reports `total_instances` (the full dataset size, e.g. 500 for Verified) — kept in `metrics.json` for honesty, never used as a denominator.

Screenshots: `screenshots/airflow_dag.png`, `screenshots/mlflow_runs.png`, `screenshots/object_storage_artifacts.png`.

## A completed evaluation

Example run (`subset=verified`, `split=test`, `task_slice=0:2`, Kimi-K2.6): 2 submitted, 2 completed, 1 resolved (50%).

- `astropy__astropy-12907` — **resolved**: a one-line logic fix (`= 1` → `= right`); both FAIL\_TO\_PASS tests pass, all 13 PASS\_TO\_PASS intact.  
- `astropy__astropy-13033` — **unresolved**: the patch applied cleanly and broke nothing (20/20 PASS\_TO\_PASS), but the required test expects a specific "missing required column" error message the agent didn't produce.

That pair is SWE-bench in miniature: it measures exact behavioral match against the maintainer's fix, not plausibility — a "reasonable" patch scores zero, and trajectories are how you diagnose the near-misses.

## How to run

git clone \<repo-url\> && cd mlops-e2e-ml-pipeline

cp .env.example .env        \# fill in values; each entry documents how

docker build \-t mlops-eval:latest .        \# workload image

docker compose up airflow-init             \# one-time DB setup

docker compose up \-d \--build               \# everything else

Airflow UI on :8080 (`airflow`/`airflow`), MLflow on :5000, MinIO console on :9001 (forward the ports over SSH). Unpause `evaluate_agent`, hit Trigger, adjust params in the form. Prerequisites per the README: a VM with Docker, your user in the `docker` group, and a `NEBIUS_API_KEY`.

This recipe is battle-tested in the most direct way possible: my VM was deleted mid-project, and everything came back from the repo. See below.

## Trials and lessons learned

This project produced far more debugging than typing, and I want to record the lessons honestly — partly because the rubric values traceability, and partly because these are the lessons the assignment was designed to teach.

**Green doesn't mean correct.** The starter DAG called `subprocess.run` without `check=True`: a crashed agent produced a green task. Later, my own metrics fallback could return a truthy dict full of zeros, making "no reports found" look like a valid 0% run. Both got the same fix — fail loudly — and the payoff came immediately: when I once passed the literal string `"preds_path"` instead of the variable, `CalledProcessError` printed the full argv and the bug was obvious in seconds. A related application-level version: when the API key silently went missing, the agent still exited 0, wrote empty patches, and the failure surfaced two tasks later. The guard now fails the DAG *import* if the key is empty. Empty secrets must never travel.

**Configuration is a delivery chain, and every hop can drop the package.** The value in `.env` is not the value in the container: it must survive `.env` → compose variable substitution → a service's `environment:` block → container start (env is frozen then — editing `.env` changes nothing running) → possibly `private_environment` into a task container. I debugged breaks at literally every hop, including: `.env` written as `KEY = "value"` (spaces — my parser missed it and silently fell back to whatever the shell happened to export, which changed when I restarted services); a variable present-but-empty (compose substitutes `""` for missing vars — botocore rejects an empty region where an *unset* one would have defaulted fine); a missing `https://` scheme. Diagnosis that works: print `repr()` of config values at the failure boundary — config bugs are string bugs, and repr makes invisible junk visible.

**State lives somewhere, and you'd better know where.** Airflow's CLI doesn't talk to the server; both read the same metadata DB — so running the CLI as root pointed it at `/root/airflow`, a parallel empty universe ("no such table"). The standalone's `admin`/`admin` didn't survive the move to compose because users live in the metadata DB and I'd swapped databases. A DAG that "wasn't found" was a stopped DAG processor — my file was perfect and invisible, because CLI commands report what the processor last wrote, not what's on disk. And an editor buffer that hadn't been saved to disk cost half an hour of debugging a file that was correct on screen and wrong on disk.

**Airflow specifics that cost me real time.** A `@dag`\-decorated function must be *called* at module level — defining it does nothing, and there's no error, just no DAG. A decorator binds to the next definition: I once pasted a helper class between `@dag(...)` and the function, and the decorator turned my class into a "DAG" with baffling errors. Jinja templates render only in fields listed in an operator's `template_fields` — my `working_dir` template went through literally, and Docker happily created a directory named `{{ ti.xcom_pull(...) }}` (fix: a three-line subclass adding `working_dir` to `template_fields`; diagnostic: the Rendered Template tab shows what actually resolved).

**Tools have opinions about where output goes.** The SWE-bench harness writes its report and logs relative to its own cwd, not next to the predictions. Under subprocess I relocated outputs with `shutil.move`; under DockerOperator the fix became declarative — point `working_dir` inside the run folder and outputs land correctly by construction. Preferring "make the tool write to the right place" over "chase files afterwards" removed a whole class of leaks.

**Unbounded agents will spend your money.** My first-ever run went 232 steps on one instance with `--cost-limit 0` before I killed it. Every run now has a dollar budget (`MSWEA_GLOBAL_COST_LIMIT`) and `run_agent` has an `execution_timeout` — cost caps bound dollars, timeouts bound hours, and they protect against different failure modes. Also learned: retries fix *transient* failures, not absent services — a retry cannot outwait an MLflow server that nobody started, but `restart: unless-stopped` can make sure somebody did.

**The environment is an input to the experiment.** The subprocess-era pipeline depended on unrecorded VM state: a venv built at some point, tools on PATH, the right user in the right group, services started by hand in terminals that might die. Docker turned the execution environment into a versioned, pinned artifact (`Dockerfile` \+ `uv.lock`); compose did the same for the platform. The definitive proof arrived uninvited: **my VM was deleted mid-project.** Everything in git — code, images-as-code, compose, `.env.example` — came back with `git clone` and two commands. Everything that was VM-local state — run artifacts, MLflow history, the metadata DB — was gone. That is the entire case for the S3 upload step and for infrastructure-as-code, experienced rather than argued.

**Cloud permissions are part of the pipeline.** The S3 step worked flawlessly against every obstacle except one I couldn't code around: my account couldn't grant the storage role. The chain key → service account → role → project has to connect end to end, and the last links are administrative, not technical. Designing against the S3 *interface* (configurable endpoint) meant the administrative blocker cost a compose service, not a redesign.

## Status and future work

Implemented: configurable six-task DAG, containerized agent/eval stages, self-describing run artifacts with manifest, object-storage upload with URI provenance, MLflow tracking, full docker-compose deployment, documented environment template.

Future work: switch MinIO → Nebius Object Storage when permissions are granted (two env values); evaluate partial predictions when the cost budget trips mid-batch; immutable image tags (`mlops-eval:<git-sha>`) logged to MLflow for complete environment provenance; a HuggingFace cache volume for the task containers to speed up repeated dataset loads.  
