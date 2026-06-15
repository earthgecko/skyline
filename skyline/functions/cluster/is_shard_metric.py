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
# @modified 20240112 - Task #5178: Build and test skyline v4.1.0
#                      Feature #4792: functions.metrics_manager.manage_inactive_metrics
# Added benchmark of testing on 200000 Prometheus, kubernetes, loki, cortex,
# kubectl, thanos, mimir, cilium and node metrics
def is_shard_metric(base_name):
    """
    Fast efficient sharding to determine if a metric belong to a cluster node.
    This will shard 200000 metric names from Prometheus metrics for kubernetes,
    loki, cortex, kubectl, thanos, mimir, cilium node, etc in 0.9s
    200000 metrics took: 0.916738748550415 seconds, 80349 shard metrics, 0 errors
    These are long metric names and the following dict demonstrates the lengths
    of the metrics names (bins are rounded up to the nearest hundred)
    {700: 59232,
    400: 76705,
    800: 25142,
    100: 15038,
    200: 1095,
    500: 14420,
    600: 3577,
    300: 2306,
    900: 1316,
    1100: 403,
    1000: 506,
    1200: 259,
    1300: 1}
    The method is fit for purpose.
    
    :param base_name: the metric base_name
    :type metric: str
    :return: shard_metric
    :rtype: bool

    """
    try:
        shard_metric = False
        shard_number = 0
        if not HORIZON_SHARDS:
            # Standalone so it does belong to this shard
            # return shard_metric
            return True
        metric_as_bytes = str(base_name).encode()
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
