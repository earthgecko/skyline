"""
get_authoritative_node.py
"""
import logging
from os import uname
import zlib

from settings import HORIZON_SHARDS, REMOTE_SKYLINE_INSTANCES, SKYLINE_URL

# @added 20230205 - Feature #4830: webapp - panorama_plot_anomalies - all_events
#                   Feature #4834: webapp - api - get_all_activity
# Convert labelled metric name to base_name
from functions.metrics.get_base_name_from_labelled_metrics_name import get_base_name_from_labelled_metrics_name


# @added 20220509 - Feature #4564: authoritative_node
# @modified 20230504 - Feature #4564: authoritative_node
# Handle cluster fqdn being different from the hostname
# by determining the URL FQDN from REMOTE_SKYLINE_INSTANCES
def get_authoritative_node(current_skyline_app, base_name, return_fqdn=False):
    """
    Return the authoritative_node for a metric

    :param base_name: the base_name of the metric to determine the latest
        anomaly for.  Can be None if metric_id is passed as a positive int
    :type base_name: str
    :return: skip
    :rtype: boolean
    """
    authoritative_node = str(uname()[1])

    # @added 20230504 - Feature #4564: authoritative_node
    # Handle cluster fqdn being different from the hostname
    # by determining the URL FQDN from REMOTE_SKYLINE_INSTANCES
    if return_fqdn:
        if REMOTE_SKYLINE_INSTANCES:
            for item in REMOTE_SKYLINE_INSTANCES:
                if authoritative_node in item[3]:
                    authoritative_node = item[0]
                    break
        else:
            authoritative_node = SKYLINE_URL

    if not HORIZON_SHARDS:
        return authoritative_node

    # @added 20230205 - Feature #4830: webapp - panorama_plot_anomalies - all_events
    #                   Feature #4834: webapp - api - get_all_activity
    # Convert labelled metric name to base_name
    use_base_name = str(base_name)
    if base_name.startswith('labelled_metrics.'):
        try:
            use_base_name = get_base_name_from_labelled_metrics_name(current_skyline_app, base_name)
        except Exception as err:
            current_skyline_app_logger = current_skyline_app + 'Log'
            current_logger = logging.getLogger(current_skyline_app_logger)
            current_logger.error('error :: get_authoritative_node :: get_base_name_from_labelled_metrics_name failed for %s - %s' % (
                str(base_name), err))

    number_of_horizon_shards = len(HORIZON_SHARDS)
    # @added 20230205 - Feature #4830: webapp - panorama_plot_anomalies - all_events
    #                   Feature #4834: webapp - api - get_all_activity
    # Convert labelled metric name to base_name
    # metric_as_bytes = str(base_name).encode()
    metric_as_bytes = str(use_base_name).encode()

    value = zlib.adler32(metric_as_bytes)
    modulo_result = value % number_of_horizon_shards
    for shard_node in list(HORIZON_SHARDS.keys()):
        if HORIZON_SHARDS[shard_node] == modulo_result:
            authoritative_node = shard_node

            # @added 20230504 - Feature #4564: authoritative_node
            # Handle cluster fqdn being different from the hostname
            # by determining the URL FQDN from REMOTE_SKYLINE_INSTANCES
            if return_fqdn:
                for item in REMOTE_SKYLINE_INSTANCES:
                    if authoritative_node in item[3]:
                        authoritative_node = item[0]
                        break

            break
    return authoritative_node
