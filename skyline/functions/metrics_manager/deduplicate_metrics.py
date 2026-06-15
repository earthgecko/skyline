"""
deduplicate_metrics.py
"""
import logging
import copy
from os import uname as os_uname
from time import time
import traceback

from functions.database.queries.get_all_active_db_metric_names import get_all_active_db_metric_names
from functions.database.queries.get_all_db_metric_names import get_all_db_metric_names

# @added 20230201 - Feature #4838: functions.metrics.get_namespace_metric.count
import settings
from functions.cluster.is_shard_metric import is_shard_metric
from functions.database.queries.set_metrics_as_inactive import set_metrics_as_inactive
# @added 20231223 - Task #5188: Optimise redis renames
#                   Task #5178: Build and test skyline v4.1.0
from functions.redis.redis_rename_key import redis_rename_key

skyline_app = 'analyzer'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)

# @added 20230201 - Feature #4838: functions.metrics.get_namespace_metric.count
# Handle shard metrics only
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

full_uniques = '%sunique_metrics' % settings.FULL_NAMESPACE

LOCAL_DEBUG = False


# @added 20250109 - Bug #5522: Handle duplicate metric names
def deduplicate_metrics(self):
    """
    Determine the duplicate metrics and deduplicate them.
    - Update anomalies table and set the metric_id of duplicate metrics to the
      original metric_id.
    - Updated ionosphere fps for duplicate metrics and set the metric_id to the
      original metric_id.
    - If fps exist for duplicate metrics ensure that the original metric_id has
      z_fp_metricid and z_ts_metricid tables if not create them or copy/rename
      first duplicate metric tables.
    - If not renamed/copy, select all rows from z_fp_metricid and z_ts_metricid
      and insert them into the original metric_id z_fp_metricid and
      z_ts_metricid tables.
    - Remove the duplicate metric z_fp_metricid and z_ts_metricid tables.
    - Update ionosphere_layers table and change the metric_id of duplicate
      metrics to the metric_id of the original_metric.
    - Update layers_algorithms table and change the metric_id of duplicate
      metrics to the metric_id of the original_metric.
    - Update ionosphere_layers_matched table and change the metric_id of
      duplicate metrics to the metric_id of the original_metric.
    - Update luminosity table and change the metric_id of duplicate metrics to
      the metric_id of the original_metric.
    - Update anomalies_type table and change the metric_id of duplicate metrics
      to the metric_id of the original_metric.
    - Update motifs_matched table and change the metric_id of duplicate metrics
      to the metric_id of the original_metric.
    - Update cloudburst table and change the metric_id of duplicate metrics to
      the metric_id of the original_metric.
    - Update cloudbursts table and change the metric_id of duplicate metrics to
      the metric_id of the original_metric.
    - Update metric_group table and change the metric_id of duplicate metrics to
      the metric_id of the original_metric.
    - Update metric_group table and change the related_metric_id of duplicate
      metrics to the metric_id of the original_metric.
    - Update metric_group_info table and change the metric_id of duplicate
      metrics to the metric_id of the original_metric.
    - Update comments table and change the metric_id of duplicate metrics to
      the metric_id of the original_metric.
    - Update anomalies_updated table and change the metric_id of duplicate
      metrics to the metric_id of the original_metric.
    - Update alias_features_profile table and change the metric_id of duplicate
      metrics to the metric_id of the original_metric.
    - Update alias_features_profile table and change the metric_id of duplicate
      metrics to the metric_id of the original_metric.
    - Update alias_features_profile table and change the original_metric_id of
      duplicate metrics to the metric_id of the original_metric.
    - allow dry-run and log what would be done.

    HOW DOES THIS WORK for labelled_metrics?  Where the metric name uses the id?
    - For labelled_metrics fps copy the features_profile dirs and files for the
      duplicate labelled_metrics name to the original labelled_metrics name.
    - Check the TSDB file in Redis (if shard node) if a TSDB file exists for the
      duplicate metric, select the data. If a TSDB file exists for the original
      labelled_metric, select the data. Combine the 2 and deduplicate. Remove
      the original metric TSDB key, submit the combined data
    - Due to text resources that make references to the labelled_metrics name
      it is not feasible really.  Simple with Graphite metrics, but not
      labelled_metrics.
    - The TSDB key is often active for the highest duplicate metric id


    :param self: the self object
    :type self: object
    :return: purged
    :rtype: int

    """
    function_str = 'metrics_manager :: deduplicate_metrics'
    start = time()
    logger.info('%s :: running metrics deduplication' % function_str)
    deduplicated_metrics = {}
    logger.info('%s :: completed in %s seconds' % (
        function_str, str(time() - start)))

    return deduplicated_metrics
