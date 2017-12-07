import os
import boto3
import sys
import requests
import json


s3 = boto3.resource('s3')
os.environ["LD_LIBRARY_PATH"] = os.getcwd()
desired_keys = [('category', 'name'), ('category', 'shortname'),
                'description', 'created', 'country', 'city', 'id',
                'lat', 'lon', 'members', 'name', 'topics']


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
    # Get the MeetUp member ID, and extract groups for the ID
    urlname = key.replace("group_meetup_", "")
    params = dict(sign='true', fields='topics',
                  key='6d265b6478231312541560545821f25')
    r = requests.get('https://api.meetup.com/{}'.format(urlname),
                     params=params)
    info = r.json()

    #
    row = dict(urlname=urlname)
    # Generate the field names and values, if they exist
    for key in desired_keys:
        field_name = key
        try:
            # If the key is just a string
            if type(key) == str:
                value = info[key]
            # Otherwise, assume its a list of keys
            else:
                field_name = "_".join(key)
                # Recursively assign the list of keys
                value = info
                for k in key:
                    value = value[k]
        # Ignore fields which aren't found (these will appear
        # as NULL in the database anyway)                                                       
        except KeyError:
            continue
        row[field_name] = value

    # Copy output to S3
    bucket = s3.Bucket('tier-0')
    fname = "/tmp/group_meetup_"+urlname+".json"
    with open(fname, 'w') as f:
        json.dump(row, f, sort_keys=True, indent=4)
    bucket.upload_file(fname, "group_meetup_"+urlname)
    print("Done", urlname)

    return {
        'message': "Done "+urlname
    }


if __name__ == "__main__":
    run(None, None)
