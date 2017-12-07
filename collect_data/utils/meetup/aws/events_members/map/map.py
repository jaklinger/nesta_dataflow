import os
import boto3
import sys
import requests
import time
import json
import random

s3 = boto3.resource('s3')
os.environ["LD_LIBRARY_PATH"] = os.getcwd()


def get_members(params):
    # Set the offset parameter and make the request
    r = requests.get("https://api.meetup.com/2/rsvps", params=params)
    r.raise_for_status()
    # If no response is found
    if len(r.text) == 0:
        time.sleep(5)
        return get_members(params)
    data = r.json()
    print("\tData has length", len(data["results"]))
    return [row['member']["member_id"] for row in data['results']]


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
    prefix = "_".join(key.split("_")[0:-1])+"_"
    event_id = key.split("_")[-1]

    api_keys = ["497355443664b516a1659a6a9c48",
                "6d265b6478231312541560545821f25",
                "6579334475358f3f5615584c1e1247",
                "713f1174501b26786b527e62464f3864",
                "201ae3319585f363b3536542864446e",
                "271c51246e4d5a5b5349a12166a3431",
                "1104d42668166a5833295e785d2b75",
                "481a7f197f75f2a2533516f5e1c64a",
                "565e3b16624d2a644b3c437f552a572"]
    api_key = random.choice(api_keys)

    max_results = 200
    offset = 0
    member_ids = []
    while True:
        params = {"sign": True,
                  "event_id": event_id,
                  "offset": offset,
                  "format": "json",
                  "photo-host": "public",
                  "fields": "",
                  "order": "event",
                  "desc": False,
                  "page": 200,
                  "key": api_key}
        _results = get_members(params)
        member_ids += _results
        if len(_results) < max_results:
            break
        offset += 1

    output = []
    for member_id in set(member_ids):
        row = dict(member_id=member_id,
                   event_id=event_id)
        output.append(row)

    # Copy output to S3
    bucket = s3.Bucket('tier-0')
    key = str(event_id)
    fname = "/tmp/"+prefix+key+".json"
    with open(fname, 'w') as f:
        json.dump(output, f, sort_keys=True, indent=4)
    bucket.upload_file(fname, prefix+key)
    print("Done", key)

    return {
        'message': "Done "+key
    }
