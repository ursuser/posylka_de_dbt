from dagster import (
    AssetSelection,
    DefaultScheduleStatus,
    ScheduleDefinition,
    define_asset_job,
)
from dagster_dbt import build_dbt_asset_selection
from dagster_project.assets.dbt_assets import posylka_de_dbt_assets

# scheduled job: runs all models tagged "daily" (dbt selection syntax)
daily_dbt_selection = build_dbt_asset_selection(
    [posylka_de_dbt_assets],
    dbt_select="tag:daily",
)

run_daily_models_job = define_asset_job(
    name="posylka_de_daily",
    selection=daily_dbt_selection,
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
