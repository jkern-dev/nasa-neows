import boto3 
client = boto3.client('athena')

# Refresh the table
queryStart = client.start_query_execution(
    QueryString = """
    CREATE TABLE neows_data_parquet WITH 
    (external_location = 's3://nasa-neows-parquet/',
    format='PARQUET',
    write_compression='SNAPPY',
    partitioned_by = ARRAY['close_approach_date'])
    AS

     SELECT 
        id,
        neow_name,
        neo_ref_id,
        estimated_diameter_meters_max,
        estimated_diameter_meters_min,
        round(estimated_diameter_meters_max - estimated_diameter_meters_min,3) as estimated_diameter_variance_meters,
        ((estimated_diameter_meters_max - estimated_diameter_meters_min) / estimated_diameter_meters_max) as estimated_diameter_ratio_of_max,
        relative_velocity,
        dense_rank() over (partition by close_approach_date order by relative_velocity desc) as neow_velocity_rank,
        miss_distance,
        dense_rank() over (partition by close_approach_date order by miss_distance asc) as closest_neow,
        orbiting_body,
        is_sentry_object,
        close_approach_date
    FROM "neow_db"."nasa_neows_firehose_jk"
    WHERE close_approach_date not in (select close_approach_date from "neow_db"."nasa_neows_prod")

    ;
    """,
    QueryExecutionContext = {
        'Database': 'neow_db'
    },
    ResultConfiguration = {'OutputLocation': 's3://nasa-neows-query-results'}

)