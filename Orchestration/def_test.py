# ==================== #
#        Imports       #
# ==================== #
from pathlib import Path
import dagster as dg
import dlt
from dagster_dlt import DagsterDltResource, dlt_assets
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

# ==================== #
#    Import DLT code   #
# ==================== #
import sys
# Ensure the path is correct if running Dagster from a different directory
sys.path.insert(0, str(Path(__file__).parents[1] / "dlt_code")) 

from test_load import jobads_source  # <-- your updated DLT file


# ==================== #
#      DLT Asset       #
# ==================== #

# Create DLT resource for Dagster
dlt_resource = DagsterDltResource()

# Define where your DuckDB file will live
db_path = Path(__file__).parents[1] / "duckdb_warehouse" / "job_ads.duckdb"

# Define the DLT Asset Key as a constant for easy reference
DLT_JOBADS_ASSET_KEY = dg.AssetKey(["dlt_jobads_source_jobsearch_resource"])

# DLT -> Dagster Asset definition
@dlt_assets(
    dlt_source=jobads_source(),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="job_ads_pipeline",
        dataset_name="staging",
        destination=dlt.destinations.duckdb(str(db_path)),
    ),
    # This asset will be named 'dlt_jobads_source_jobsearch_resource'
)
def dlt_load(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
    """Load job ads data into DuckDB warehouse using DLT"""
    yield from dlt.run(context=context)


# ==================== #
#  Source Asset (The Glue) #
# ==================== #

# 🔑 FIX 1: Define a SourceAsset that represents the staging table created by DLT.
# This SourceAsset must be named to match how dbt refers to the source table 
# in its staging models (e.g., 'staging.job_ads').
staging_job_ads_table = dg.SourceAsset(
    key=["staging", "job_ads"],  # The database schema and table name
    # We explicitly declare that the actual upstream dependency is the DLT asset
    metadata={"upstream_asset_key": DLT_JOBADS_ASSET_KEY.to_string()}
)


# ==================== #
#      DBT Asset       #
# ==================== #

dbt_project_directory = Path(__file__).parents[1] / "dbt_code"
profiles_dir = Path.home() / ".dbt"

dbt_project = DbtProject(
    project_dir=dbt_project_directory,
    profiles_dir=profiles_dir
)

dbt_resource = DbtCliResource(project_dir=dbt_project)

# Ensure manifest file is ready (important for asset loading)
dbt_project.prepare_if_dev()

@dbt_assets(
    manifest=dbt_project.manifest_path,
)
# 🔑 FINAL FIX: Change the injected parameter name from 'staging_job_ads_table' 
# to 'job_ads' (the last element of the SourceAsset key: ["staging", "job_ads"]).
# This is required for Dagster's dbt integration to link the dependency correctly.
def dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource, job_ads):
    """Run DBT transformations"""
    # The 'job_ads' parameter is only used to create the dependency.
    yield from dbt.cli(["build"], context=context).stream()


# ==================== #
#         Jobs         #
# ==================== #

# DLT job - now uses the specific asset key for precision
job_dlt = dg.define_asset_job(
    "job_dlt",
    selection=dg.AssetSelection.keys(DLT_JOBADS_ASSET_KEY),
)

# DBT job - assuming your dbt models are prefixed with 'warehouse' or 'marts'
job_dbt = dg.define_asset_job(
    "job_dbt",
    selection=dg.AssetSelection.key_prefixes("warehouse", "marts"),
)


# ==================== #
#       Schedule       #
# ==================== #

schedule_dlt = dg.ScheduleDefinition(
    job=job_dlt,
    cron_schedule="08 21 * * *"  # Runs every day 21:08 UTC
)


# ==================== #
#        Sensor        #
# ==================== #

@dg.asset_sensor(
    asset_key=DLT_JOBADS_ASSET_KEY,
    job_name="job_dbt"
    )
def dlt_load_sensor(context: dg.SensorExecutionContext):
    # Check if the DLT asset was updated successfully
    if context.latest_materialization_key():
        yield dg.RunRequest()


# ==================== #
#      Definitions     #
# ==================== #

defs = dg.Definitions(
    # 🔑 FIX 3: Include the new SourceAsset in the assets list
    assets=[dlt_load, dbt_models, staging_job_ads_table],
    resources={"dlt": dlt_resource,"dbt": dbt_resource},
    jobs=[job_dlt, job_dbt],
    schedules=[schedule_dlt],
    sensors=[dlt_load_sensor],
)