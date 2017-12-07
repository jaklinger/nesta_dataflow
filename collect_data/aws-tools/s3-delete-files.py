import boto3
s3 = boto3.resource('s3')
bucket = "tier-0"
prefix = "meetup_event_member_"
objs = s3.Bucket(bucket).objects
objs.filter(Prefix=prefix).delete()
