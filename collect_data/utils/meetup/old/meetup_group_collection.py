import requests
import ratelim
import ast
import logging

# Local imports
from utils.common.db_utils import read_all_results
from utils.common.datapipeline import DataPipeline

# Specifiy the rate limits
RATELIM_DUR = 50 * 60
RATELIM_QUERIES = 8500


@ratelim.patient(RATELIM_QUERIES, RATELIM_DUR)
def collect_group_info(urlname, api_key):
    params = dict(key=api_key, sign='true', fields='topics')
    r = requests.get('https://api.meetup.com/{}'.format(urlname),
                     params=params)
    return r.json()


def run(config):
    member_groups = read_all_results(config, 'input_db', 'input_table')

    # Collect group info
    groups = set(row['group_urlname'] for row in member_groups)
    logging.info("Got %s from %s", len(groups), len(member_groups))

    # A list of field names to extract from the data
    desired_keys = ast.literal_eval(config['parameters']['desired_keys'])
    # Loop through groups
    group_info = []
    api_key = config["Meetup"]["api-key"]
    for urlname in groups:
        info = collect_group_info(urlname, api_key)
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
        group_info.append(row)

    with DataPipeline(config) as dp:
        for row in group_info:
            dp.insert(row)
