from pathlib import Path
from dagster_dbt import DbtCliResource

dbt_project_dir = Path(__file__).parent.parent.parent

dbt_resource = DbtCliResource(
    project_dir=str(dbt_project_dir),
    profiles_dir=str(Path.home() / ".dbt"),
    target="prod",
)
