# Healthcare

> Work in progress.

A small healthcare data pipeline on Airflow 3.x. Bronze tasks land source files
as-is and later layers reshape them. Right now there's one DAG that pulls the CMS
synthetic outpatient claims file and loads it into the database as a raw,
all-text bronze table.

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

## Loading into Neon

The load task targets an Airflow connection named `healthcare_<env>`. You pick
`env` on the trigger form each run (a dropdown: development or production), so
both environments run from one Airflow. The default the form starts on is
`development`, or whatever `PIPELINE_ENV` is set to on the worker. Use
underscores, not hyphens, so the id also works when supplied via an env var.

You'll need a connection for each environment you actually run — e.g. both
`healthcare_development` and `healthcare_production`.

Get the connection string from the Neon console (your project → Connect) and
register it one of these ways.

Quickest for local runs — an env var Airflow picks up automatically (the var
name is `AIRFLOW_CONN_` + the connection id, upper-cased):

```bash
export AIRFLOW_CONN_HEALTHCARE_DEVELOPMENT='postgresql://USER:PASSWORD@HOST/DB?sslmode=require'
```

Or store it in Airflow's metadata db:

```bash
airflow connections add healthcare_development \
  --conn-uri 'postgresql://USER:PASSWORD@HOST/DB?sslmode=require'
```

Or in the UI: run `airflow standalone`, open http://localhost:8080, log in (the
admin password is in `$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`),
then **Admin → Connections → add**. Set Connection Id `healthcare_development`,
type Postgres, fill in host/database/login/password from Neon, and put
`{"sslmode": "require"}` in the Extra field.

The task drops and rebuilds `bronze.cms_outpatient` from the CSV header on every
run, so it's idempotent — re-running replaces the data, never appends. Verify
from Neon's SQL editor:

```sql
select count(*) from bronze.cms_outpatient;
```

## Dev tooling

Ruff for lint/format, pre-commit for the git hooks (config lives at the repo root):

```bash
ruff check .
ruff format .

pre-commit install         # once per checkout
pre-commit run --all-files
```
