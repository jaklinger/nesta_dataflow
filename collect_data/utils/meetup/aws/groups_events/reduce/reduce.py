import logging
from utils.common.datapipeline import DataPipeline
from utils.common.aws_tools import get_all_by_prefix


def run(config=None):
    #prefix = "_".join(('meetup',
    #                   config['parameters']['country'].replace(" ", "_"),
    #                   config['parameters']['category']))+"_"
    prefix = "meet_groups_events_expanded_"
    data = get_all_by_prefix(prefix, 'tier-0')
    #good_results = 0
    #for row in data:
    #    if "group_id" in row:
    #        good_results += 1
    logging.info("\tGot %s group-event pairs, of which %s good results",
                 len(data))

    # Write data
    logging.info("\tWriting to table")
    with DataPipeline(config) as dp:
        for row in data:
            dp.insert(row)
