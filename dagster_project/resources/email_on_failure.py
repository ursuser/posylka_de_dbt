from dagster import RunFailureSensorContext, run_failure_sensor
from . import send_alert_email


@run_failure_sensor
def email_on_run_failure(context: RunFailureSensorContext):
    job_name = context.dagster_run.job_name
    error = context.failure_event.message if context.failure_event else "unknown error"

    send_alert_email(
        subject=f"[dagster] posylka_de: {job_name} failed",
        body=f"job: {job_name}\n\nerror:\n{error}",
    )
