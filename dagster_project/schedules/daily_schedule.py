from dagster import (
    AssetSelection,
    ScheduleDefinition,
    define_asset_job,
)

# scheduled job: runs all models tagged "daily"
run_daily_models_job = define_asset_job(
    name="run_daily_models",
    selection=AssetSelection.tag("tag", "daily"),
    description="run models tagged with daily (scheduled)",
)

# schedule: every day at 05:00 UTC
daily_schedule = ScheduleDefinition(
    job=run_daily_models_job,
    cron_schedule="0 5 * * *",
    description="run daily models every day at 05:00 utc",
)

# ad-hoc job: run all models at once
run_all_models_job = define_asset_job(
    name="run_all_models",
    selection=AssetSelection.all(),
    description="run all dbt models (manual)",
)
