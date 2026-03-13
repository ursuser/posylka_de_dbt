from pathlib import Path
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

dbt_manifest_path = Path(__file__).parent.parent.parent / "target" / "manifest.json"


@dbt_assets(
    manifest=dbt_manifest_path,
)
def posylka_de_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
