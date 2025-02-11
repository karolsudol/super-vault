
from prefect_dbt.cli.commands import DbtCoreOperation
from prefect import flow
import os
@flow
def trigger_dbt_flow() -> str:
    result = DbtCoreOperation(
        commands=["dbt build -t prod"],
        project_dir=os.getenv('DBT_PROJECT_DIR'),
        profiles_dir=os.getenv('DBT_PROFILES_DIR')
    ).run()
    return result

if __name__ == "__main__":
    trigger_dbt_flow()