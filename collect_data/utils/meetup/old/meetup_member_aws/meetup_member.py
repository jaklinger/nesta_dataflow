import os
import boto3
import sys
import requests
import json


s3 = boto3.resource('s3')
os.environ["LD_LIBRARY_PATH"] = os.getcwd()


def get_member_groups(info, member_id):
    output = []
    row = dict(member_id=int(member_id))
    if 'memberships' not in info:
        print('ERROR: {} : No info'.format(member_id))
        output.append(row)
        return output
    if 'member' not in info['memberships']:
        print('ERROR: {} : No info'.format(member_id))
        output.append(row)
        return output
    for membership in info['memberships']['member']:
        urlname = membership['group']['urlname']
        row = dict(member_id=int(member_id),
                   group_urlname=urlname)
        output.append(row)
    return output


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
    key = key.replace("meetup_", "")
    params = dict(sign='true', fields='memberships',
                  key='6d265b6478231312541560545821f25')
    r = requests.get('https://api.meetup.com/members/{}'.format(key),
                     params=params)
    info = r.json()
    output = get_member_groups(info, key)

    # Copy output to S3
    bucket = s3.Bucket('tier-0')
    fname = "/tmp/meetup_"+key+".json"
    with open(fname, 'w') as f:
        json.dump(output, f, sort_keys=True, indent=4)
    bucket.upload_file(fname, "meetup_"+key)
    print("Done", key)

    return {
        'message': "Done "+key
    }


if __name__ == "__main__":
    run(None, None)
