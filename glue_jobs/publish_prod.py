import boto3
from datetime import datetime

# create a string with the current UTC datetime
# convert all special characters to underscores
# this will be used in the table name and in the bucket path in S3 where the table is stored
DATETIME_NOW_INT_STR = str(datetime.utcnow()).replace('-', '_').replace(' ', '_').replace(':', '_').replace('.', '_')

client = boto3.client('athena')

# Refresh the table
queryStart = client.start_query_execution(
    QueryString = f"""
    INSERT INTO nasa_neows_prod

    SELECT
        *
    FROM "neow_db"."neows_data_parquet"

    ;
    """,
    QueryExecutionContext = {
        'Database': 'neow_db'
    }, 
    ResultConfiguration = { 'OutputLocation': 's3://nasa-neows-query-results/'}
)