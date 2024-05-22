import json
import boto3
import urllib3
from datetime import datetime
from datetime import timedelta

FIREHOSE_NAME = 'PUT-S3-qGMwB'

def lambda_handler(event, context):
    http = urllib3.PoolManager()
    start_date = datetime.now()
    end_date = start_date + timedelta(days = 6)
    r = http.request("GET",f"https://api.nasa.gov/neo/rest/v1/feed?start_date={str(start_date.year)}-{str(start_date.month)}-{str(start_date.day)}.&end_date={str(end_date.year)}-{str(end_date.month)}-{str(end_date.day)}&api_key=podVq7AvcHukl6WfLJQcdDaCjtqF55gTjfqxQ5Y3")
    r_dict = json.loads(r.data.decode(encoding='utf-8', errors = 'strict'))
    neows = r_dict['near_earth_objects']
    # look through each day of the week neows
    lookup_date = start_date 
    final_msg = ''
    while lookup_date.day <= end_date.day:
        neow_date = lookup_date.strftime('%Y') +'-'+lookup_date.strftime('%m')+'-'+lookup_date.strftime('%d')
        lookup_date += timedelta(days = 1)
        for n in neows[neow_date]:
            processed_dict = {}    
            processed_dict['id'] = n['id']
            processed_dict['neow_name'] = n['name']
            processed_dict['neo_ref_id'] = int(n['neo_reference_id'])
            processed_dict['estimated_diameter_meters_max'] = n['estimated_diameter']['meters']['estimated_diameter_max']
            processed_dict['estimated_diameter_meters_min'] = n['estimated_diameter']['meters']['estimated_diameter_min']
            processed_dict['is_potentially_hazardous'] = str(n['is_potentially_hazardous_asteroid'])
            processed_dict['close_approach_date'] = n['close_approach_data'][0]['close_approach_date']
            processed_dict['relative_velocity'] = float(n['close_approach_data'][0]['relative_velocity']['miles_per_hour'])
            processed_dict['miss_distance'] = float(n['close_approach_data'][0]['miss_distance']['miles'])
            processed_dict['orbiting_body'] = n['close_approach_data'][0]['orbiting_body']
            processed_dict['is_sentry_object'] = str(n['is_sentry_object'])
            processed_dict['row_ts'] = str(datetime.now())
            msg = str(processed_dict) + '\n'
            final_msg += msg

    fh = boto3.client('firehose')
    reply = fh.put_record(
        DeliveryStreamName = FIREHOSE_NAME,
        Record = {
            'Data': final_msg
        }
    )
    return reply