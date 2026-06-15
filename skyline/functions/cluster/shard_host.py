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


# @added 20240520 - Feature #5354: functions.redis.populate_redis_metric
#                   Feature #5352: vista - bigquery
# Copied from the method used in flux/listen.py with some slight modification
def shard_host(base_name, return_fqdn=False):
    """
    Return the shard host or FQDN to which a base_name belongs.
    """
    shost = this_host
    if not HORIZON_SHARDS:
        return shost
    try:
        metric_as_bytes = str(base_name).encode()
        value = zlib.adler32(metric_as_bytes)
        modulo_result = value % len(HORIZON_SHARDS)
        for shost in HORIZON_SHARDS:
            if modulo_result == HORIZON_SHARDS[shost]:
                # Handle cluster fqdn being different from the hostname
                # by determining the URL FQDN from REMOTE_SKYLINE_INSTANCES
                if return_fqdn:
                    for item in settings.REMOTE_SKYLINE_INSTANCES:
                        if shost in item[3]:
                            return item[0]
                return shost
    except:
        shost = None
    return shost
