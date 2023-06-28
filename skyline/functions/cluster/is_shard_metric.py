"""
is_shard_metric.py
"""
from os import uname as os_uname
import copy
import zlib

import settings
try:
    HORIZON_SHARDS = copy.deepcopy(settings.HORIZON_SHARDS)
except:
    HORIZON_SHARDS = {}
number_of_horizon_shards = 0
this_host = str(os_uname()[1])
if this_host == 'skyline-test-1-fra1':
    DEVELOPMENT = True
    HORIZON_SHARDS = {
        'skyline-test-1-fra1': 0,
        'another-test-node-1': 1,
        'another-test-node-2': 2,
    }
else:
    DEVELOPMENT = False
HORIZON_SHARD = 0
if HORIZON_SHARDS:
    number_of_horizon_shards = len(HORIZON_SHARDS)
    HORIZON_SHARD = HORIZON_SHARDS[this_host]


# @added 20230201 - Feature #4792: functions.metrics_manager.manage_inactive_metrics
def is_shard_metric(metric):
    """
    Determine if a metric belong to a cluster node.

    :param self: the self object
    :type self: object
    :return: purged
    :rtype: int

    """
    try:
        shard_metric = False
        shard_number = 0
        if not HORIZON_SHARDS:
            return shard_metric
        metric_as_bytes = str(metric).encode()
        value = zlib.adler32(metric_as_bytes)
        for shard_node in list(HORIZON_SHARDS.keys()):
            modulo_result = value % number_of_horizon_shards
            shard_number = HORIZON_SHARDS[shard_node]
            if modulo_result == shard_number:
                if shard_number == HORIZON_SHARD:
                    shard_metric = True
                    break
    except:
        shard_metric = None
    return shard_metric
