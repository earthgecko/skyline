"""
get_authoritative_node.py
"""
from os import uname
import zlib

from settings import HORIZON_SHARDS


# @added 20220509 - Feature #4564: authoritative_node
def get_authoritative_node(base_name):
    """
    Return the authoritative_node for a metric

    :param base_name: the base_name of the metric to determine the latest
        anomaly for.  Can be None if metric_id is passed as a positive int
    :type base_name: str
    :return: skip
    :rtype: boolean
    """
    authoritative_node = str(uname()[1])
    if not HORIZON_SHARDS:
        return authoritative_node

    number_of_horizon_shards = len(HORIZON_SHARDS)
    metric_as_bytes = str(base_name).encode()
    value = zlib.adler32(metric_as_bytes)
    modulo_result = value % number_of_horizon_shards
    for shard_node in list(HORIZON_SHARDS.keys()):
        if HORIZON_SHARDS[shard_node] == modulo_result:
            authoritative_node = shard_node
            break
    return authoritative_node
