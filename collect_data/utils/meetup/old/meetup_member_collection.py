import requests
import ratelim
import pandas as pd
import logging

# Local imports
from utils.common.datapipeline import DataPipeline

# Specify rate limits
RATELIM_DUR = 50 * 60
RATELIM_QUERIES = 8500


@ratelim.patient(RATELIM_QUERIES, RATELIM_DUR)
def collect_member_info(member_id, api_key):
    params = dict(sign='true', fields='memberships', key=api_key)
    r = requests.get('https://api.meetup.com/members/{}'.format(member_id),
                     params=params)
    return r.json()


def run(config):
    # Read dataframe with member IDs
    members = pd.read_csv(config['parameters']['src'])
    member_ids = set(members.id.values)
    logging.info("Processing %s member ids", len(member_ids))
    # Parse unique member-group combinations
    api_key = config['Meetup']['api-key']
    member_groups = []
    for member_id in member_ids:
        info = collect_member_info(member_id, api_key)
        if 'memberships' not in info:
            print('ERROR: {} : No info'.format(member_id))
            continue
        if 'member' not in info['memberships']:
            print('ERROR: {} : No info'.format(member_id))
            continue
        for membership in info['memberships']['member']:
            urlname = membership['group']['urlname']
            row = dict(member_id=int(member_id),
                       group_urlname=urlname)
            member_groups.append(row)

    logging.info("Got %s rows of data", len(member_groups))
    # Save to disk
    with DataPipeline(config) as dp:
        for row in member_groups:
            dp.insert(row)
