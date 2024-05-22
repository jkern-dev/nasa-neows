import sys
import awswrangler as wr

# this check counts the number of NULL rows in the temp_C column
# if any rows are NULL, the check returns a number > 0
CHECK_DIAMETER = f"""
select 
    sum(case when estimated_diameter_meters_min > estimated_diameter_meters_max then 1 else 0 end) as res_col
from neow_db.neows_data_parquet
;
"""

# run the quality check
df = wr.athena.read_sql_query(sql=CHECK_DIAMETER, database="neow_db")

# exit if we get a result > 0
# else, the check was successful
if df['res_col'][0] > 0:
    sys.exit('Results returned. Quality check failed.')
else:
    print('Quality check passed.')