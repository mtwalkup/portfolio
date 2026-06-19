# Healthcare

> Work in progress.

A small healthcare data pipeline on Airflow 3.x. Bronze tasks land source files
as-is and later layers reshape them. Right now there's one DAG that pulls the CMS
synthetic outpatient claims file.  Currently only pulls the csv file down.

```
airflow/
  dags/   cms_outpatient_bronze_load.py
  tests/  parse-only DAG checks
```

## Running a DAG locally

Run the file directly to execute one DagRun in-process. Handy for breakpoints,
since it skips the scheduler and worker:

```bash
python healthcare/airflow/dags/cms_outpatient_bronze_load.py
```

Or go through Airflow's own discovery/run machinery:

```bash
AIRFLOW__CORE__DAGS_FOLDER="$PWD/healthcare/airflow/dags" \
AIRFLOW__CORE__LOAD_EXAMPLES=False \
airflow dags test cms_outpatient_bronze_load
```

Both are also set up as VS Code tasks and launch configs.

## Dev tooling

Ruff for lint/format, pre-commit for the git hooks (config lives at the repo root):

```bash
ruff check .
ruff format .

pre-commit install         # once per checkout
pre-commit run --all-files
```
