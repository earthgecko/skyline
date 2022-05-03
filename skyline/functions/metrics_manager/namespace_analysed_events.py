"""
namespace_analysed_events.py
"""
import logging
from time import time, strftime, gmtime
import datetime
from collections import defaultdict

skyline_app = 'analyzer'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)


# @added 20220420 - Feature #4530: namespace.analysed_events
def namespace_analysed_events(self):
    """

    Create and manage the metrics_manager.namespace.analysed_events Redis hash
    from the app Redis hashes

    :param self: the self object
    :type self: object
    :return: boolean
    :rtype: list

    """

    today_date_string = str(strftime('%Y-%m-%d', gmtime()))
    today_dt = datetime.datetime.strptime(today_date_string, '%Y-%m-%d')
    today_timestamp = int(datetime.datetime.timestamp(today_dt))

    namespace_analysed_events_dict = {}
    try:
        namespace_analysed_events_dict = self.redis_conn_decoded.hgetall('metrics_manager.namespace.analysed_events')
    except Exception as err:
        logger.error('error :: metrics_manager :: namespace_analysed_events :: failed to hgetall metrics_manager.namespace.analysed_events - %s' % str(err))

    update_dates = [today_date_string]
    if str(today_timestamp) not in list(namespace_analysed_events_dict.keys()):
        yesterday_date_string = str(strftime('%Y-%m-%d', gmtime((time() - 86400))))
        update_dates = [yesterday_date_string, today_date_string]

    namespace_analysed_events_keys = []
    try:
        for key in self.redis_conn_decoded.scan_iter('namespace.analysed_events.*'):
            namespace_analysed_events_keys.append(key)
    except Exception as err:
        logger.error('error :: metrics_manager :: namespace_analysed_events :: failed to scan_iter namespace.analysed_events.* - %s' % str(err))

    for date_string in update_dates:
        redis_keys = []
        new_values = {}
        try:
            del new_totals
        except:
            pass
        total = 0
        new_totals = defaultdict(int)
        for key in namespace_analysed_events_keys:
            if date_string in key:
                redis_keys.append(key)
        for key in redis_keys:
            app = key.split('.')[2]
            key_dict = {}
            try:
                key_dict = self.redis_conn_decoded.hgetall(key)
            except Exception as err:
                logger.error('error :: metrics_manager :: namespace_analysed_events :: failed to hgetall %s- %s' % (
                    key, str(err)))
            if key_dict:
                new_values[app] = {}
                for namespace in list(key_dict.keys()):
                    new_values[app][namespace] = int(float(key_dict[namespace]))
                    new_totals[namespace] += int(float(key_dict[namespace]))
                    total += int(float(key_dict[namespace]))
        new_dict = {}
        if new_values:
            new_dict['date'] = date_string
            new_dict['total'] = total
            new_dict['total_analysed_events'] = {}
            for namespace in list(new_totals.keys()):
                new_dict['total_analysed_events'][namespace] = new_totals[namespace]
            for app in list(new_values.keys()):
                new_dict[app] = {}
                for namespace in list(new_values[app].keys()):
                    new_dict[app][namespace] = new_values[app][namespace]
        if new_dict:
            dt = datetime.datetime.strptime(date_string, '%Y-%m-%d')
            use_timestamp = int(datetime.datetime.timestamp(dt))
            try:
                self.redis_conn_decoded.hset('metrics_manager.namespace.analysed_events', use_timestamp, str(new_dict))
                logger.info('metrics_manager :: namespace_analysed_events :: updated %s data in metrics_manager.namespace.analysed_events' % date_string)
            except Exception as err:
                logger.error('error :: metrics_manager :: namespace_analysed_events :: failed to hset yesterday data in metrics_manager.namespace.analysed_events - %s' % str(err))

    for timestamp_str in list(namespace_analysed_events_dict.keys()):
        if int(timestamp_str) < (int(time()) - (86400 * 90)):
            try:
                self.redis_conn_decoded.hdel('metrics_manager.namespace.analysed_events', timestamp_str)
                logger.info('metrics_manager :: namespace_analysed_events :: pruned %s data from metrics_manager.namespace.analysed_events' % timestamp_str)
            except Exception as err:
                logger.error('error :: metrics_manager :: namespace_analysed_events :: failed to hdel %s in metrics_manager.namespace.analysed_events - %s' % (
                    timestamp_str, str(err)))

    return True
