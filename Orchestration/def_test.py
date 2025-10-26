# ==================== #
#                      #
#       Imports        #
#                      #
# ==================== #
from pathlib import Path
import dagster as dg
import dlt
from dagster_dlt import DagsterDltResource, dlt_assets
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

# ==================== #
#                      #
#    Import DLT code   #
#                      #
# ==================== #
import sys
sys.path.insert(0,'../dlt_code')  # path to your DLT folder

from test_load import jobads_source  # <-- your updated DLT file


# ==================== #
#                      #
#       DLT Asset      #
#                      #
# ==================== #

# Create DLT resource for Dagster
dlt_resource = DagsterDltResource()

# Define where your DuckDB file will live
db_path = Path(__file__).parents[1] / "data_warehouse" / "job_ads.duckdb"

# DLT -> Dagster Asset definition
@dlt_assets(
    dlt_source=jobads_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="job_ads_pipeline",
        dataset_name="staging",
        destination=dlt.destinations.duckdb(db_path),
    ),
)
def dlt_load(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
    """Load job ads data into DuckDB warehouse using DLT"""
    yield from dlt.run(context=context)


# ==================== #
#                      #
#       DBT Asset      #
#                      #
# ==================== #
# (Keep this ready for later DBT models integration)

dbt_project_directory = Path(__file__).parents[1] / "dbt_code"
profiles_dir = Path.home() / ".dbt"

dbt_project = DbtProject(
    project_dir=dbt_project_directory,
    profiles_dir=profiles_dir
)

dbt_resource = DbtCliResource(project_dir=dbt_project)

# Ensure manifest file is ready
dbt_project.prepare_if_dev()

@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """Run DBT transformations"""
    yield from dbt.cli(["build"], context=context).stream()


# ==================== #
#                      #
#         Jobs         #
#                      #
# ==================== #

#DLT job
job_dlt = dg.define_asset_job(
    "job_dlt",
    selection=dg.AssetSelection.keys("dlt_jobads_source_jobsearch_resource"),
)

# DBT job
job_dbt = dg.define_asset_job(
    "job_dbt",
    selection=dg.AssetSelection.key_prefixes("warehouse", "marts"),)


# ==================== #
#                      #
#       Schedule       #
#                      #
# ==================== #

schedule_dlt = dg.ScheduleDefinition(
    job=job_dlt,
    cron_schedule="08 21 * * *"  # Runs every day 21:08 UTC
)


# ==================== #
#                      #
#        Sensor        #
#                      #
# ==================== #

@dg.asset_sensor(
    asset_key=dg.AssetKey("dlt_jobads_source_jobsearch_resource"),
   job_name="job_dbt"
   )
def dlt_load_sensor():
    yield dg.RunRequest()


# ==================== #
#                      #
#     Definitions      #
#                      #
# ==================== #

defs = dg.Definitions(
    assets=[dlt_load,dbt_models],
    resources={"dlt": dlt_resource,"dbt": dbt_resource},
    jobs=[job_dlt, job_dbt],
    schedules=[schedule_dlt],
    sensors=[dlt_load_sensor],
)
