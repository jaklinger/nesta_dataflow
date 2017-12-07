import requests
import logging
import time
import ratelim

# Local imports
from utils.common.db_utils import read_all_results
from utils.common.datapipeline import DataPipeline
# from retrying import retry

RATELIM_DUR = 50 * 60
RATELIM_QUERIES = 8500


# @retry(wait_random_min=2000, wait_random_max=60000, stop_max_attempt_number=10)
@ratelim.patient(RATELIM_QUERIES, RATELIM_DUR)
def get_events(urlname, params):
    # Set the offset parameter and make the request
    r = requests.get("https://api.meetup.com/{}/events".format(urlname),
                     params=params)
    #if r.status_code in (410, 404):
    #    return []
    try:
        r.raise_for_status()
    except Exception as err:
        return str(err)
    # If no response is found
    if len(r.text) == 0:
        time.sleep(5)
        return get_events(urlname, params)
    return r.json()


def filter_groups(groups, config):
    '''Filter groups matching target country/category'''
    groups = [row for row in groups
              if row['country_name'] == config['parameters']['country']]
    groups = [row for row in groups
              if row['category_id'] == int(config['parameters']['category'])]
    return groups


def run(config):
    # Get already collected groups
    done_groups = read_all_results(config, 'output_db', 'table_name')
    done_groups = set(row['group_id'] for row in done_groups)
    # Get all possible input groups
    groups = read_all_results(config, 'input_db', 'input_table')
    groups = filter_groups(groups, config)
    groups = set((row['id'], row['urlname'])
                 for row in groups
                 if row['id'] not in done_groups)

    # Collect group info
    logging.info("Got %s distinct groups from database", len(groups))

    # Collect events
    api_key = config["Meetup"]["api-key"]
    max_results = 200
    output = []
    failed = []
    for group_id, group_urlname in groups:
        events = []
        for desc in (True, False):
            params = dict(page=max_results, key=api_key,
                          desc=desc, status="past")
            result = get_events(group_urlname, params)
            if type(result) == str:
                print(result)
                print(group_urlname)
                failed.append([group_urlname, desc])
                continue
            events += result
        # Consolidate required info into output
        for info in events:
            attendance = None
            if "manual_attendance_count" in info:
                attendance = info["manual_attendance_count"]
            yes_rsvp_count = info["yes_rsvp_count"]
            row = dict(event_id=info["id"],
                       group_urlname=group_urlname,
                       group_id=group_id,
                       event_time=info["time"],
                       event_manual_attendance_count=attendance,
                       event_yes_rsvp_count=yes_rsvp_count)
            output.append(row)

    logging.info("Warning: %s events failed:\n%s", len(failed), failed)
    # Write to output
    logging.info("Got %s rows of data", len(output))
    with DataPipeline(config) as dp:
        for row in output:
            dp.insert(row)
