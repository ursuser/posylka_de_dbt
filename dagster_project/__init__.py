from dagster import Definitions
from .assets.dbt_assets import posylka_de_dbt_assets
from .resources.dbt_resource import dbt_resource
from .resources.email_on_failure import email_on_run_failure
from .schedules.daily_schedule import (
    run_daily_models_job,
    daily_schedule,
    run_all_models_job,
)

defs = Definitions(
    assets=[posylka_de_dbt_assets],
    jobs=[
        run_daily_models_job,
        run_all_models_job,
    ],
    schedules=[
        daily_schedule,
    ],
    sensors=[
        email_on_run_failure,
    ],
    resources={
        "dbt": dbt_resource,
    },
)
