import subprocess
from pathlib import Path
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

dbt_project_dir = Path(__file__).parent.parent.parent
dbt_manifest_path = dbt_project_dir / "target" / "manifest.json"


@dbt_assets(
    manifest=dbt_manifest_path,
)
def posylka_de_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    context.log.info("pulling latest code from git...")
    result = subprocess.run(
        ["git", "pull"],
        cwd=str(dbt_project_dir),
        capture_output=True,
        text=True,
    )
    context.log.info(f"git pull: {result.stdout.strip()}")
    if result.returncode != 0:
        raise RuntimeError(f"git pull failed: {result.stderr.strip()}")

    context.log.info("regenerating dbt manifest...")
    parse_result = subprocess.run(
        ["dbt", "parse", "--profiles-dir", "/root/.dbt", "--target", "prod"],
        cwd=str(dbt_project_dir),
        capture_output=True,
        text=True,
    )
    context.log.info(f"dbt parse: {parse_result.stdout.strip()}")
    if parse_result.returncode != 0:
        raise RuntimeError(f"dbt parse failed: {parse_result.stderr.strip()}")

    yield from dbt.cli(["build"], context=context).stream()
