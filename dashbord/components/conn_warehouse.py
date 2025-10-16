import os
from dotenv import load_dotenv
import snowflake.connector
import pandas as pd


def get_job_list(query='SELECT * FROM JOBTECH_ANALYSIS.MARTS.MART_MAIN'):

    load_dotenv()

    with snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    ) as conn:

        df = pd.read_sql(query, conn)

        return df