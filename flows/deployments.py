from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from prefect.filesystems import GitHub

# Import all your flows
from clickhouse import create_table_flow, write_data_flow, read_data_flow
from dbt import trigger_dbt_flow
from prefect import show_stars
from super_vault import flow as super_vault_flow

# Configure GitHub storage block
github_block = GitHub.load("github-flows")

def create_deployment(flow, name, cron=None, tags=None):
    """Helper function to create deployments with consistent settings"""
    return Deployment.build_from_flow(
        flow=flow,
        name=name,
        storage=github_block,
        schedule=(CronSchedule(cron=cron) if cron else None),
        tags=tags or [],
        work_queue_name="default"
    )

if __name__ == "__main__":
    # Create deployments for ClickHouse flows
    clickhouse_deployments = [
        create_deployment(
            flow=create_table_flow,
            name="clickhouse-create-table",
            tags=["clickhouse"]
        ),
        create_deployment(
            flow=write_data_flow,
            name="clickhouse-write-data",
            cron="0 */6 * * *",  # Every 6 hours
            tags=["clickhouse"]
        ),
        create_deployment(
            flow=read_data_flow,
            name="clickhouse-read-data",
            cron="0 */12 * * *",  # Every 12 hours
            tags=["clickhouse"]
        )
    ]

    # Create deployment for DBT flow
    dbt_deployment = create_deployment(
        flow=trigger_dbt_flow,
        name="dbt-build",
        cron="0 0 * * *",  # Daily at midnight
        tags=["dbt"]
    )

    # Create deployment for GitHub stars flow
    github_deployment = create_deployment(
        flow=show_stars,
        name="github-stars",
        cron="0 */24 * * *",  # Daily
        tags=["github"]
    )

    # Create deployment for SuperVault flow
    supervault_deployment = create_deployment(
        flow=super_vault_flow,
        name="supervault",
        cron="*/30 * * * *",  # Every 30 minutes
        tags=["blockchain"]
    )

    # Deploy all flows
    all_deployments = clickhouse_deployments + [dbt_deployment, github_deployment, supervault_deployment]
    for deployment in all_deployments:
        deployment.apply() 