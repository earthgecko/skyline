"""
process_graphite_fail_queue.py
"""
from ast import literal_eval
import logging
# bandit [B403:blacklist] Consider possible security implications associated
# with pickle module.  These have been considered.
import pickle  # nosec B403
import socket
import struct
from time import time, sleep
import traceback

import settings

from skyline_functions import get_redis_conn_decoded
from functions.graphite.send_graphite_metric import send_graphite_metric

try:
    from settings import CARBON_HOST
except:
    CARBON_HOST = ''
try:
    from settings import CARBON_PORT
except:
    CARBON_PORT = ''
try:
    from settings import SKYLINE_METRICS_CARBON_HOST
    skyline_metrics_carbon_host = SKYLINE_METRICS_CARBON_HOST
except:
    skyline_metrics_carbon_host = CARBON_HOST
try:
    from settings import SKYLINE_METRICS_CARBON_PORT
    skyline_metrics_carbon_port = SKYLINE_METRICS_CARBON_PORT
except:
    skyline_metrics_carbon_port = CARBON_PORT

skyline_app = 'horizon'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)


# @modified 20230512 - Feature #4904: Handle send_graphite_metric failure
def process_graphite_fail_queue(self):
    """
    Sends the skyline_app metrics that are in the skyline.graphite_fail_queue

    :param self: the self object
    :type self: object
    :return: sent_count
    :rtype: int

    """

    def pickle_data_to_graphite(data):

        message = None
        try:
            payload = pickle.dumps(data, protocol=2)
            header = struct.pack("!L", len(payload))
            message = header + payload
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: horizon.worker :: process_graphite_fail_queue :: failed to pickle to send to Graphite - %s' % err)
            return False
        if message:
            try:
                sock = socket.socket()
                sock.connect((skyline_metrics_carbon_host, skyline_metrics_carbon_port))
                sock.sendall(message)
                sock.close()
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: worker :: process_graphite_fail_queue :: failed to send pickle data to Graphite')
                return False
        else:
            logger.error(traceback.format_exc())
            logger.error('error :: worker :: process_graphite_fail_queue :: failed to pickle metric data into message')
            return False
        return True

    def submit_pickle_data_to_graphite(pickle_data):

        try:
            number_of_datapoints = len(pickle_data)
        except Exception as err:
            logger.error('error :: horizon.worker :: process_graphite_fail_queue :: could not determine number_of_datapoints from len(pickle_data) - %s' % str(err))
            return False

        data_points_sent = 0
        smallListOfMetricTuples = []
        tuples_added = 0

        for data in pickle_data:
            try:
                smallListOfMetricTuples.append(data)
                tuples_added += 1
                if tuples_added >= 480:
                    try:
                        pickle_data_sent = pickle_data_to_graphite(smallListOfMetricTuples)
                    except Exception as e:
                        logger.error('error :: horizon.worker :: process_graphite_fail_queue :: pickle_data_to_graphite error - %s' % str(e))
                        pickle_data_sent = False

                    # Reduce the speed of submissions to Graphite
                    # if there are lots of data points
                    if number_of_datapoints > 4000:
                        sleep(0.3)
                    if pickle_data_sent:
                        data_points_sent += tuples_added
                        logger.info('horizon.worker :: process_graphite_fail_queue :: sent %s/%s of %s data points to Graphite via pickle' % (
                            str(tuples_added), str(data_points_sent),
                            str(number_of_datapoints)))
                        smallListOfMetricTuples = []
                        tuples_added = 0
                    else:
                        logger.error('error :: horizon.worker :: process_graphite_fail_queue :: failed to send %s data points to Graphite via pickle' % (
                            str(tuples_added)))
                        return False
            except Exception as e:
                logger.error('error :: horizon.worker :: process_graphite_fail_queue :: error handling data in pickle_data - %s' % str(e))
                return False

        if smallListOfMetricTuples:
            # @modified 20201207 - Task #3864: flux - try except everything
            try:
                tuples_to_send = len(smallListOfMetricTuples)
                pickle_data_sent = pickle_data_to_graphite(smallListOfMetricTuples)
                if pickle_data_sent:
                    data_points_sent += tuples_to_send
                    logger.info('horizon.worker :: process_graphite_fail_queue :: sent the last %s/%s of %s data points to Graphite via pickle' % (
                        str(tuples_to_send), str(data_points_sent),
                        str(number_of_datapoints)))
                else:
                    logger.error('error :: horizon.worker :: process_graphite_fail_queue :: failed to send the last %s data points to Graphite via pickle' % (
                        str(tuples_to_send)))
                    return False
            except Exception as e:
                logger.error('error :: horizon.worker :: process_graphite_fail_queue :: error in smallListOfMetricTuples pickle_data_to_graphite - %s' % str(e))
                return False
        return True

    sent_count = 0
    time_now = int(time())

    try:
        self.redis_conn_decoded.setex('skyline.processing.graphite_fail_queue', 59, time_now)
    except Exception as err:
        logger.error('error :: horizon.worker :: process_graphite_fail_queue :: failed to setex Redis key skyline.processing.graphite_fail_queue - %s' % (
            err))

    process_set = 'skyline.graphite_fail_queue.horizon.%s' % str(time_now)
    # Rename the set so it can be processed in isolation
    try:
        self.redis_conn_decoded.rename('skyline.graphite_fail_queue', process_set)
    except Exception as err:
        logger.error('error :: horizon.worker :: process_graphite_fail_queue :: failed to rename Redis set skyline.graphite_fail_queue to %s - %s' % (
            process_set, err))
        return sent_count

    # Get the items to send to Graphite
    graphite_fail_queue = []
    try:
        graphite_fail_queue = list(self.redis_conn_decoded.smembers(process_set))
    except Exception as err:
        logger.error('error :: horizon.worker :: process_graphite_fail_queue :: smembers failed on Redis set %s - %s' % (process_set, err))
    items_to_process_count = len(graphite_fail_queue)
    logger.info('horizon.worker :: process_graphite_fail_queue :: processing %s items in the graphite_fail_queue' % str(items_to_process_count))

    # Do not delete, just expire the set for debug
    try:
        self.redis_conn_decoded.expire(process_set, 600)
    except Exception as err:
        logger.error('error :: horizon.worker :: process_graphite_fail_queue :: failed to set expire on Redis set %s - %s' % (process_set, err))

    # Iterate the queue
    graphite_queue = []
    for item in graphite_fail_queue:
        try:
            item_list = literal_eval(item)
            metric = item_list[0]
            value = item_list[1]
            ts = item_list[2]
            graphite_queue.append([item, metric, value, ts])
        except Exception as err:
            logger.error('error :: horizon.worker :: process_graphite_fail_queue :: failed to evaluate %s - %s' % (str(item), err))

    # Sort the queue by timestamp so that the oldest data is submitted first
    sorted_graphite_queue = sorted(graphite_queue, key=lambda x: x[3])

    # Record any failures to be readded to skyline.graphite_fail_queue
    items_to_readd = []

    pickle_data = []
    for item, metric, value, ts in sorted_graphite_queue:
        tuple_data = (metric, (int(ts), float(value)))
        pickle_data.append(tuple_data)
        items_to_readd.append(item)

    if pickle_data:
        try:
            pickle_data_submitted = submit_pickle_data_to_graphite(pickle_data)
        except Exception as err:
            logger.error('error :: horizon.worker :: process_graphite_fail_queue :: queue Empty failed to submit_pickle_data_to_graphite - %s' % err)
            pickle_data_submitted = False

    if pickle_data_submitted:
        sent_count = len(pickle_data)
        logger.info('horizon.worker :: process_graphite_fail_queue :: submitted %s metrics to Graphite' % (
            str(sent_count)))
        logger.info('horizon.worker :: process_graphite_fail_queue :: pickle_data sample: %s' % (
            str(pickle_data[0:5])))
        try:
            self.redis_conn_decoded.setex('skyline.graphite_ok', 65, 1)
            logger.info('horizon.worker :: process_graphite_fail_queue :: created skyline.graphite_ok Redis key to allow apps to submit directly')
        except Exception as err:
            logger.error('error :: horizon.worker :: process_graphite_fail_queue :: failed to create Redis key skyline.graphite_ok - %s' % err)
        try:
            self.redis_conn_decoded.delete('skyline.graphite_fail')
            logger.info('horizon.worker :: process_graphite_fail_queue :: removed skyline.graphite_fail Redis key to allow apps to submit directly')
        except Exception as err:
            logger.error('error :: horizon.worker :: process_graphite_fail_queue :: failed to delete Redis key skyline.graphite_fail - %s' % err)
        try:
            self.redis_conn_decoded.delete('skyline.processing.graphite_fail_queue')
        except Exception as err:
            logger.error('error :: horizon.worker :: process_graphite_fail_queue :: failed to setex Redis key skyline.processing.graphite_fail_queue - %s' % (
                err))

    if not pickle_data_submitted:
        added = 0
        try:
            added = self.redis_conn_decoded.sadd('skyline.graphite_fail_queue', *set(items_to_readd))
        except Exception as err:
            logger.error('error :: horizon.worker :: failed to add %s to Redis set skyline.graphite_fail_queue - %s' % (
                str(item), process_set, err))
            added = 0
        logger.info('horizon.worker :: process_graphite_fail_queue :: readded %s metrics to the skyline.graphite_fail_queue Redis set' % (
            str(added)))

    return sent_count
