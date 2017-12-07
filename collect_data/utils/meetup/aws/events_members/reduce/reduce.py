import logging
from utils.common.datapipeline import DataPipeline
from utils.common.aws_tools import get_all_by_prefix


def run(config=None):
#    prefix = "_".join(('event_meetup'))  # ,
#                       config['parameters']['country'].replace(" ", "_"),
#                       config['parameters']['category']))+"_"
    prefix = "event_meetup_"
    data = get_all_by_prefix(prefix, 'tier-0')
    logging.info("\tGot %s member-event pairs.", len(data))
    print(data)

    # Write data
    logging.info("\tWriting to table")
    with DataPipeline(config) as dp:
        for row in data:
            dp.insert(row)
