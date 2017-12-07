import os
import boto3
import sys
import requests
import time
import json

s3 = boto3.resource('s3')
os.environ["LD_LIBRARY_PATH"] = os.getcwd()


def get_members(params):
    # Set the offset parameter and make the request
    r = requests.get("https://api.meetup.com/members/", params=params)
    r.raise_for_status()
    # If no response is found
    if len(r.text) == 0:
        time.sleep(5)
        return get_members(params)
    data = r.json()
    return [row['id'] for row in data['results']]


def run(event, context):
    # Set path
    sys.path.append(os.getcwd())
    # Read the input data. Note: the S3 file is empty,
    # but the file has been used to trigger this function
    # and the file 'key' corresponds to the MeetUp member ID
    trigger_file = event["Records"][0]["s3"]["object"]
    key = trigger_file["key"]
    bucket = "tier-0-inputs"
    print("Got file", key)
    obj = s3.Bucket(bucket).Object(key)
    obj.delete()
    # Get the MeetUp group ID and urlname
    prefix = "_".join(key.split("_")[0:-2])+"_"
    group_id = key.split("_")[-1]
    group_urlname = key.split("_")[-2]

    max_results = 200
    offset = 0
    member_ids = []
    while True:
        params = dict(offset=offset, page=max_results,
                      key='6d265b6478231312541560545821f25',
                      group_id=group_id)
        _results = get_members(params)
        member_ids += _results
        if len(_results) < max_results:
            break
        offset += 1

    output = []
    for member_id in set(member_ids):
        row = dict(member_id=member_id,
                   group_urlname=group_urlname,
                   group_id=group_id)
        output.append(row)

    # Copy output to S3
    bucket = s3.Bucket('tier-0')
    key = group_id+"_"+group_urlname
    fname = "/tmp/"+prefix+key+".json"
    with open(fname, 'w') as f:
        json.dump(output, f, sort_keys=True, indent=4)
    bucket.upload_file(fname, prefix+key)
    print("Done", key)

    return {
        'message': "Done "+key
    }
