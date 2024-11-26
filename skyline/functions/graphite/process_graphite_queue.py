"""
process_graphite_queue.py
"""
from ast import literal_eval
import logging
# bandit [B403:blacklist] Consider possible security implications associated
# with pickle module.  These have been considered.
import pickle  # nosec B403
import socket
import struct
from ast import literal_eval
from time import time, sleep
import traceback
import os

import settings

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

CARBON_PICKLE_PORT = 0
try:
    from settings import CARBON_PICKLE_PORT
    skyline_metrics_carbon_pickle_port = CARBON_PICKLE_PORT
except:
    skyline_metrics_carbon_pickle_port = CARBON_PORT
try:
    from settings import SKYLINE_METRICS_CARBON_PICKLE_PORT
    skyline_metrics_carbon_pickle_port = SKYLINE_METRICS_CARBON_PICKLE_PORT
except:
    skyline_metrics_carbon_pickle_port = int(skyline_metrics_carbon_pickle_port)

this_host = str(os.uname()[1])


# @added 20240101 - Feature #5192: skyline_graphite_metrics_single_submit
def process_graphite_queue(self, current_skyline_app, skyline_graphite_metrics_single_submit_data):
    """
    Sends the skyline_app metrics that are in the skyline.graphite_metrics_single_submit

    :param self: the self object
    :type self: object
    :return: sent_count
    :rtype: int

    """

    current_skyline_app_logger = str(current_skyline_app) + 'Log'
    current_logger = logging.getLogger(current_skyline_app_logger)

    def pickle_data_to_graphite(data):

        current_logger.info('process_graphite_queue :: building pickle data to send %s metrics to Graphite on %s:%s' % (
            str(len(data)), str(skyline_metrics_carbon_host),
            str(skyline_metrics_carbon_pickle_port)))

        message = None
        try:
            payload = pickle.dumps(data, protocol=2)
            header = struct.pack("!L", len(payload))
            message = header + payload
        except Exception as err:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: process_graphite_queue :: failed to pickle to send to Graphite - %s' % err)
            return False
        if message:
            current_logger.info('process_graphite_queue :: built pickle data to send %s metrics to Graphite on %s:%s' % (
                str(len(data)), str(skyline_metrics_carbon_host),
                str(skyline_metrics_carbon_pickle_port)))
            try:
                sock = socket.socket()
                sock.connect((skyline_metrics_carbon_host, skyline_metrics_carbon_pickle_port))
                sock.sendall(message)
                sock.close()
            except:
                current_logger.error(traceback.format_exc())
                current_logger.error('error :: process_graphite_queue :: failed to send pickle data to Graphite')
                return False
        else:
            current_logger.error(traceback.format_exc())
            current_logger.error('error :: process_graphite_queue :: failed to pickle metric data into message')
            return False

        current_logger.info('process_graphite_queue :: sent %s metrics in pickle data to Graphite on %s:%s' % (
            str(len(data)), str(skyline_metrics_carbon_host),
            str(skyline_metrics_carbon_pickle_port)))
        return True

    def submit_pickle_data_to_graphite(pickle_data):

        try:
            number_of_datapoints = len(pickle_data)
        except Exception as err:
            current_logger.error('error :: process_graphite_queue :: could not determine number_of_datapoints from len(pickle_data) - %s' % str(err))
            return False

        data_points_sent = 0
        smallListOfMetricTuples = []
        tuples_added = 0

        for data in pickle_data:
            try:
                smallListOfMetricTuples.append(data)
                tuples_added += 1
                if tuples_added >= 480:
                    pickle_data_sent = False
                    try:
                        pickle_data_sent = pickle_data_to_graphite(smallListOfMetricTuples)
                    except Exception as err:
                        current_logger.error('error :: process_graphite_queue :: pickle_data_to_graphite error - %s' % err)
                        pickle_data_sent = False

                    # Reduce the speed of submissions to Graphite
                    # if there are lots of data points
                    if number_of_datapoints > 4000:
                        sleep(0.3)
                    if pickle_data_sent:
                        data_points_sent += tuples_added
                        current_logger.info('process_graphite_queue :: sent %s/%s of %s data points to Graphite via pickle' % (
                            str(tuples_added), str(data_points_sent),
                            str(number_of_datapoints)))
                        delete_keys = []
                        for item in smallListOfMetricTuples:
                            key = '%s.%s' % (str(item[1][0]), str(item[0]))
                            delete_keys.append(key)
                            # current_logger.debug('debug :: process_graphite_queue :: sent %s' % str(item))
                        if delete_keys:
                            try:
                                self.redis_conn_decoded.hdel('skyline.graphite_metrics_single_submit', *set(delete_keys))
                                current_logger.info('process_graphite_queue :: ran hdel %s keys skyline.graphite_metrics_single_submit' % (
                                    str(len(delete_keys))))
                            except Exception as err:
                                current_logger.error('error :: process_graphite_queue :: failed to hdel from skyline.graphite_metrics_single_submit, err: %s' % (
                                    err))
                        smallListOfMetricTuples = []
                        tuples_added = 0
                    else:
                        current_logger.error('error :: process_graphite_queue :: failed to send %s data points to Graphite via pickle' % (
                            str(tuples_added)))
                        return False
            except Exception as err:
                current_logger.error('error :: process_graphite_queue :: error handling data in pickle_data - %s' % err)
                return False

        if smallListOfMetricTuples:
            # @modified 20201207 - Task #3864: flux - try except everything
            try:
                tuples_to_send = len(smallListOfMetricTuples)
                pickle_data_sent = False
                pickle_data_sent = pickle_data_to_graphite(smallListOfMetricTuples)
                if pickle_data_sent:
                    data_points_sent += tuples_to_send
                    current_logger.info('process_graphite_queue :: sent the last %s/%s of %s data points to Graphite via pickle' % (
                        str(tuples_to_send), str(data_points_sent),
                        str(number_of_datapoints)))
                    delete_keys = []
                    for item in smallListOfMetricTuples:
                        key = '%s.%s' % (str(item[1][0]), str(item[0]))
                        delete_keys.append(key)
                        # current_logger.debug('debug :: process_graphite_queue :: sent %s' % str(item))
                    if delete_keys:
                        try:
                            self.redis_conn_decoded.hdel('skyline.graphite_metrics_single_submit', *set(delete_keys))
                            current_logger.info('process_graphite_queue :: ran hdel %s keys skyline.graphite_metrics_single_submit' % (
                                str(len(delete_keys))))
                        except Exception as err:
                            current_logger.error('error :: process_graphite_queue :: failed to hdel from skyline.graphite_metrics_single_submit, err: %s' % (
                                err))
                else:
                    current_logger.error('error :: process_graphite_queue :: failed to send the last %s data points to Graphite via pickle' % (
                        str(tuples_to_send)))
                    return False
            except Exception as e:
                current_logger.error('error :: process_graphite_queue :: error in smallListOfMetricTuples pickle_data_to_graphite - %s' % str(e))
                return False
        return True

    sent_count = 0
    time_now = int(time())

    # @added 20240101 - Feature #5192: skyline_graphite_metrics_single_submit
    #                   Task #5244: Add CentOS Stream 9 to dawn build
    #                   Task #5178: Build and test skyline v4.1.0
    # Remove keys with invalid data
    invalid_keys = []

    # @added 20240726 - Feature #5400: analyzer_batch - stats
    analyzer_batch_stats = {}
    batch_processing_namespaces_count = 0
    try:
        batch_processing_namespaces_count = self.redis_conn_decoded.scard('metrics_manager.batch_processing_namespaces')
    except Exception as err:
        current_logger.error('error :: process_graphite_queue :: failed to scard from metrics_manager.batch_processing_namespaces, err: %s' % (
            err))
    if batch_processing_namespaces_count:
        analyzer_batch_stats = {
            'total_analyzed': 0,
            'total_anomalies': 0,
            'run_time': 0,
        }
        analyzer_batch_stats_raw = None
        try:
            analyzer_batch_stats_raw = self.redis_conn_decoded.hgetall('analyzer_batch.stats')
        except Exception as err:
            current_logger.error('error :: process_graphite_queue :: failed to hgetall from analyzer_batch.stats, err: %s' % (
                err))
        if analyzer_batch_stats_raw:
            try:
                self.redis_conn_decoded.delete('analyzer_batch.stats')
            except Exception as err:
                # @modified 20241106 - Task #5526: Build v5.0.0 and upgrade deps
                # bandit interpreting as SQL because of the term delete
                current_logger.error('error :: process_graphite_queue :: failed to delete from analyzer_batch.stats, err: %s' % (
                    err))  # nosec B608
            try:
                analyzer_batch_metrics = list(analyzer_batch_stats.keys())
                for pid, pid_dict in analyzer_batch_stats_raw.items():
                    for metric_key in pid_dict.keys():
                        analyzer_batch_metrics.append(metric_key)
                analyzer_batch_metrics = set(analyzer_batch_metrics)
                for metric in analyzer_batch_metrics:
                    values = []
                    for pid, pid_dict in analyzer_batch_stats_raw.items():
                        for metric_key in pid_dict.keys():
                            if metric_key == metric:
                                values.append(float(pid_dict[metric_key]))
                    if len(values) > 0:
                        value = sum(values)
                        if metric == 'run_time':
                            value = sum(values) / len(values)
                        analyzer_batch_stats[metric] = value
            except Exception as err:
                current_logger.error('error :: process_graphite_queue :: failed to determine analyzer_batch.stats, err: %s' % (
                    err))
        for metric, value in analyzer_batch_stats.items():
            metric = 'skyline.analyzer_batch.%s.%s' % (this_host, metric)
            key = '%s.%s' % (str(time_now), metric)
            data = {'metric': metric, 'value': value, 'timestamp': time_now}
            skyline_graphite_metrics_single_submit_data[key] = str(data)

    # Iterate the queue
    graphite_queue = []
    for key, dict_str in skyline_graphite_metrics_single_submit_data.items():
        try:
            data_dict = literal_eval(dict_str)
            metric = data_dict['metric']

            # @modified 20240101 - Feature #5192: skyline_graphite_metrics_single_submit
            #                      Task #5244: Add CentOS Stream 9 to dawn build
            #                      Task #5178: Build and test skyline v4.1.0
            # Test for types and remove keys with invalid data
            if isinstance(data_dict['value'], str):
                try:
                    data_dict['value'] = float(data_dict['value'])
                except Exception as err:
                    current_logger.info('warning :: process_graphite_queue :: failed to evaluate data_dict[\'value\'] string a float for %s, value: %s, err: %s' % (
                        str(metric), str(data_dict['value']), err))
            elif isinstance(data_dict['value'], float):
                value = float(data_dict['value'])
            elif isinstance(data_dict['value'], int):
                value = float(data_dict['value'])
            elif data_dict['value'] == 0:
                value = 0.0
            elif data_dict['value'] == '0':
                value = 0.0
            else:
                current_logger.info('warning :: process_graphite_queue :: failed to evaluate value as a float for %s, value: %s, skipping' % (
                    str(metric), str(data_dict['value'])))
                invalid_keys.append(key)
                continue
            try:
                ts = int(data_dict['timestamp'])
            except Exception as err:
                current_logger.info('warning :: process_graphite_queue :: failed to evaluate  ts as an int for %s, timestamp: %s, skipping, err: %s' % (
                    str(metric), str(data_dict['timestamp']), err))
                invalid_keys.append(key)
                continue
            graphite_queue.append([data_dict, metric, value, ts])
        except Exception as err:
            current_logger.error('error :: process_graphite_queue :: failed to evaluate %s - %s' % (str(key), err))

    # @added 20240101 - Feature #5192: skyline_graphite_metrics_single_submit
    #                   Task #5244: Add CentOS Stream 9 to dawn build
    #                   Task #5178: Build and test skyline v4.1.0
    # Remove keys with invalid data
    if invalid_keys:
        try:
            self.redis_conn_decoded.hdel('skyline.graphite_metrics_single_submit', *set(invalid_keys))
            current_logger.info('process_graphite_queue :: ran hdel %s invalid keys in skyline.graphite_metrics_single_submit' % (
                str(len(invalid_keys))))
        except Exception as err:
            current_logger.error('error :: process_graphite_queue :: failed to hdel from skyline.graphite_metrics_single_submit, err: %s' % (
                err))

    # Sort the queue by timestamp so that the oldest data is submitted first
    sorted_graphite_queue = sorted(graphite_queue, key=lambda x: x[3])
    current_logger.info('process_graphite_queue :: %s sorted items to send' % str(len(sorted_graphite_queue)))
    current_logger.info('process_graphite_queue :: sorted_graphite_queue[0:2]: %s' % str(sorted_graphite_queue[0:2]))
    current_logger.info('process_graphite_queue :: sorted_graphite_queue[-2:]: %s' % str(sorted_graphite_queue[-2:]))

    # Record any failures to be readded to skyline.graphite_fail_queue
    items_to_readd = []

    pickle_data = []
    for item, metric, value, ts in sorted_graphite_queue:
        tuple_data = (metric, (int(ts), float(value)))
        pickle_data.append(tuple_data)
        items_to_readd.append(str(item))

    pickle_data_submitted = False
    if pickle_data:
        try:
            pickle_data_submitted = submit_pickle_data_to_graphite(pickle_data)
        except Exception as err:
            current_logger.error('error :: process_graphite_queue :: submit_pickle_data_to_graphite failed - %s' % err)
            pickle_data_submitted = False

    if pickle_data_submitted:
        sent_count = len(pickle_data)
        current_logger.info('process_graphite_queue :: submitted %s metrics to Graphite' % (
            str(sent_count)))
        current_logger.info('process_graphite_queue :: pickle_data sample, pickle_data[0:5]: %s' % (
            str(pickle_data[0:5])))
        # For debug
        key = 'skyline.graphite_metrics_single_submit.%s.processed.%s' % (current_skyline_app, str(time_now))
        try:
            self.redis_conn_decoded.setex(key, 600, str(pickle_data))
        except Exception as err:
            current_logger.error('error :: process_graphite_queue :: failed to create Redis key %s, err: %s' % (
                key, err))

    if not pickle_data_submitted:
        added = 0
        # @modified 20240101 - Feature #5192: skyline_graphite_metrics_single_submit
        #                      Task #5244: Add CentOS Stream 9 to dawn build
        #                      Task #5178: Build and test skyline v4.1.0
        # Only sadd if data to add
        if len(items_to_readd) > 0:
            try:
                added = self.redis_conn_decoded.sadd('skyline.graphite_fail_queue', *set(items_to_readd))
            except Exception as err:
                current_logger.error('error :: process_graphite_queue :: failed to add %s to Redis set skyline.graphite_fail_queue - %s' % (
                    str(len(items_to_readd)), err))
                added = 0
            current_logger.info('process_graphite_queue :: readded %s metrics to the skyline.graphite_fail_queue Redis set' % (
                str(added)))

    # @added 20240726 - Feature #5400: analyzer_batch - stats
    if len(analyzer_batch_stats) > 0:
        sent_count = sent_count - len(analyzer_batch_stats)

    return sent_count
