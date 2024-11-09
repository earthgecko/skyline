import logging
import traceback
from os import uname
from os import getpid
from time import time

import settings
from functions.redis.get_metric_timeseries import get_metric_timeseries
from functions.thunder.send_event import thunder_send_event
from functions.thunder.check_thunder_failover_key import check_thunder_failover_key
from custom_algorithms import run_custom_algorithm_on_timeseries

skyline_app = 'thunder'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)

# The required THUNDER directories which are failed over to and
# used in the event that Redis is down
THUNDER_EVENTS_DIR = '%s/thunder/events' % settings.SKYLINE_TMP_DIR
THUNDER_KEYS_DIR = '%s/thunder/keys' % settings.SKYLINE_TMP_DIR

this_host = str(uname()[1])


# @added 20230927 - 
def thunder_check_flux_invalid_metrics(self):
    """
    Determine invalid_metrics for namespaces and send a thunder alert when they
    start to send invalid metrics.

    :param self: the self object
    :type self: object
    :return: success
    :rtype: boolean

    """

    function_str = 'functions.thunder.thunder_flux_invalid_metrics'

    check_app = 'flux'
    event_type = 'listen.invalid_metrics'
    base_name = 'skyline.%s.%s.%s' % (check_app, this_host, event_type)

    success = True
    now = int(time())

    check_dict = {}
    hash_key = 'thunder.%s.%s' % (check_app, event_type)
    try:
        check_dict = self.redis_conn_decoded.hgetall(hash_key)
    except Exception as e:
        logger.error('error :: %s :: could not get the Redis %s key - %s' % (
            function_str, hash_key, e))

    return success
