from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    ScheduleDefinition,
    define_asset_job,
)

# scheduled job: runs all models tagged "daily"
run_daily_models_job = define_asset_job(
    name="posylka_de_daily",
    selection=AssetSelection.tag("tag", "daily"),
    description="posylka de: run models tagged with daily (scheduled)",
)

# schedule: every day at 05:00 UTC
daily_schedule = ScheduleDefinition(
    job=run_daily_models_job,
    cron_schedule="0 5 * * *",
    description="posylka de: run daily models every day at 05:00 utc",
    default_status=DefaultScheduleStatus.RUNNING,
)

# ad-hoc job: run all models at once
run_all_models_job = define_asset_job(
    name="posylka_de_run_all",
    selection=AssetSelection.all(),
    description="posylka de: run all dbt models (manual)",
)
