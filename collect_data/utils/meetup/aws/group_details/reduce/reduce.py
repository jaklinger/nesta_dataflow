import logging
from utils.common.datapipeline import DataPipeline
from utils.common.aws_tools import get_all_by_prefix


def run(config=None):
#    prefix = "_".join(('group_meetup_',
#                       config['parameters']['country'].replace(" ", "_"),
#                       config['parameters']['category']))+"_"
    prefix = 'group_meetup_241117_'
    data = get_all_by_prefix(prefix, 'tier-0')
    logging.info("\tGot %s groups.", len(data))

    # Write data
    logging.info("\tWriting to table")
    with DataPipeline(config) as dp:
        for row in data:
            dp.insert(row)
