import logging
import traceback
from time import time, sleep
from threading import Thread
from multiprocessing import Process

import os
from os import kill, getpid
import os.path
from ast import literal_eval

import settings
from skyline_functions import (
    # @modified 20220726 - Task #2732: Prometheus to Skyline
    #                      Branch #4300: prometheus
    # Moved send_graphite_metric
    # mkdir_p, get_redis_conn, get_redis_conn_decoded, send_graphite_metric,
    mkdir_p, get_redis_conn, get_redis_conn_decoded,
    write_data_to_file)
from thunder_alerters import thunder_alert
from functions.redis.update_set import update_redis_set
from functions.filesystem.remove_file import remove_file
from functions.thunder.check_thunder_failover_key import check_thunder_failover_key
from functions.thunder.alert_on_stale_metrics import alert_on_stale_metrics
from functions.thunder.alert_on_no_data import alert_on_no_data
# @added 20220202 - Feature #4412: flux - quota - thunder alert
from functions.thunder.alert_on_quota_exceeded import alert_on_quota_exceeded

# @added 20231121 - Feature #5104: boundary - external_settings
# Limit cardinality
from functions.thunder.alert_on_boundary_cardinality_breach import alert_on_boundary_cardinality_breach

# @added 20220726 - Task #2732: Prometheus to Skyline
#                   Branch #4300: prometheus
from functions.graphite.send_graphite_metric import send_graphite_metric

# @added 20240319 - Feature #5312: thunder - 207 alert
from functions.thunder.alert_on_invalid_metrics import alert_on_invalid_metrics

skyline_app = 'thunder'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
skyline_app_loglock = '%s.lock' % skyline_app_logfile
skyline_app_logwait = '%s.wait' % skyline_app_logfile

this_host = str(os.uname()[1])
thunder_redis_set = 'thunder.events'
thunder_done_redis_set = 'thunder.events.done'

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

# The required THUNDER directories which are failed over to and
# used in the event that Redis is down
THUNDER_EVENTS_DIR = '%s/thunder/events' % settings.SKYLINE_TMP_DIR
THUNDER_KEYS_DIR = '%s/thunder/keys' % settings.SKYLINE_TMP_DIR

skyline_app_graphite_namespace = 'skyline.%s%s' % (skyline_app, SERVER_METRIC_PATH)

LOCAL_DEBUG = False


class Thunder(Thread):
    """
    The Thunder class which controls the thunder thread and spawned
    processes. Thunder is ONLY for alerting on Skyline operations.
    Thunder checks on the other hand are carried out by analyzer/metrics_manager
    and other Skyline apps, which send events to thunder.
    thunder/rolling carries out internal and external checks and sends any events
    to thunder.
    """

    def __init__(self, parent_pid):
        """
        Initialize the Thunder

        Create the :obj:`self.redis_conn` connection
        Create the :obj:`self.redis_conn_decoded` connection

        """
        super(Thunder, self).__init__()
        self.redis_conn = get_redis_conn(skyline_app)
        self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)
        self.daemon = True
        self.parent_pid = parent_pid
        self.current_pid = getpid()

    def check_if_parent_is_alive(self):
        """
        Self explanatory
        """
        try:
            kill(self.current_pid, 0)
            kill(self.parent_pid, 0)
        except:
            logger.info('warning :: parent or current process dead')
            exit(0)

    def spin_thunder_process(self, i, validated_event_details, redis_item, event_file):
        """
        Roll some thunder.

        :param i: python process id

        :return: anomalous
        :rtype: boolean
        """

        def create_alert_cache_key(cache_key, expiry, timestamp):
            try:
                set_alert_cache_key = self.redis_conn.setex(cache_key, expiry, timestamp)
                if set_alert_cache_key:
                    logger.info('set Redis key %s with %s TTL' % (
                        cache_key, str(expiry)))
            except Exception as err:
                logger.error('error :: set_alert_cache_key failed setting key - %s - %s' % (
                    cache_key, err))
                # Add a key file
                thunder_keys_file = '%s/%s' % (THUNDER_KEYS_DIR, cache_key)
                thunder_keys_file_data = {'timestamp': timestamp, 'expiry': expiry}
                try:
                    write_data_to_file(
                        skyline_app, thunder_keys_file, 'w',
                        str(thunder_keys_file_data))
                    logger.info('added Redis failover thunder_keys_file  %s' % (thunder_keys_file))
                except Exception as err:
                    logger.error('error :: failed to add Redis failover thunder_keys_file - %s - %s' % (thunder_keys_file, err))

        def remove_event(redis_item, event_file, do_log=True):
            if redis_item:
                # Delete the item from the Redis set
                try:
                    # removed_item = update_redis_set(
                    # @added 20220303 - Feature #4412: flux - quota - thunder alert
                    # do_log = True
                    try:
                        redis_item_dict = literal_eval(redis_item)
                        if isinstance(redis_item_dict, dict):
                            if redis_item_dict['event_type'] == 'metric_quota_exceeded':
                                do_log = False
                                logger.info('not logging total event string as metric_quota_exceeded, sampling with data removed')
                                try:
                                    del redis_item_dict['data']
                                except Exception as err:
                                    logger.error('error :: failed to removed data key from dict - %s' % err)
                                logger.info('sample: %s' % str(redis_item_dict))
                            # @added 20231121 - Feature #5104: boundary - external_settings
                            # Limit cardinality
                            if redis_item_dict['event_type'] == 'boundary_cardinality_breach':
                                do_log = False
                                logger.info('not logging total event string as boundary_cardinality_breach, sampling with data removed')
                                try:
                                    del redis_item_dict['data']['rejected_metrics']
                                except Exception as err:
                                    logger.error('error :: failed to removed data key from dict - %s' % err)
                                logger.info('sample: %s' % str(redis_item_dict))

                    except Exception as err:
                        logger.error('error :: failed literal_eval the redis_item - %s' % err)

                    update_redis_set(
                        skyline_app, thunder_redis_set, redis_item,
                        # @modified 20220303 - Feature #4412: flux - quota - thunder alert
                        # 'remove', log=True)
                        'remove', do_log)
                    # if removed_item:
                    #     logger.error('error :: could not determine event_details from %s Redis set entry (removed) - %s' % (
                    #         thunder_redis_set, str(redis_item)))
                except Exception as err:
                    logger.error('error :: could not remove item from Redis set %s - %s' % (
                        thunder_redis_set, err))
            if event_file:
                # Delete the bad event_file
                removed_file = False
                try:
                    removed_file = remove_file(skyline_app, event_file)
                except Exception as err:
                    logger.error('error :: could not remove event_file %s - %s' % (
                        event_file, err))
                if removed_file:
                    logger.info('event_file removed - %s' % (
                        str(event_file)))

        spin_start = time()
        spin_thunder_process_pid = os.getpid()

        # @added 20220303 - Feature #4412: flux - quota - thunder alert
        # Do not log out metric_quota_exceeded as has too much data
        log_validated_event_details = True
        event_type = None
        try:
            event_type = str(validated_event_details['event_type'])
            if event_type == 'metric_quota_exceeded':
                log_validated_event_details = False

            # @added 20231121 - Feature #5104: boundary - external_settings
            # Limit cardinality
            if event_type == 'boundary_cardinality_breach':
                log_validated_event_details = False

            # @added 20240319 - Feature #5312: thunder - 207 alert
            if event_type == 'invalid_data':
                log_validated_event_details = False

        except Exception as err:
            logger.error('error :: failed literal_eval the redis_item - %s' % err)

        if log_validated_event_details:
            logger.info('spin_thunder_process - %s, processing check - %s' % (
                str(spin_thunder_process_pid), str(validated_event_details)))
        else:
            logger.info('spin_thunder_process - %s, processing check for %s (not logging validated_event_details)' % (
                str(spin_thunder_process_pid), str(event_type)))

        try:
            level = str(validated_event_details['level'])
            event_type = str(validated_event_details['event_type'])
            message = str(validated_event_details['message'])
            app = str(validated_event_details['app'])
            metric = str(validated_event_details['metric'])
            source = str(validated_event_details['source'])
            expiry = int(validated_event_details['expiry'])
            timestamp = validated_event_details['timestamp']
            alert_vias = validated_event_details['alert_vias']
            data = validated_event_details['data']
            event_file = validated_event_details['event_file']
        except Exception as err:
            logger.error('error :: spin_thunder_process :: failed to determine variables from event_details - %s' % (
                err))
            # return

        # Handle thunder/rolling alerts first, these are defined by source being
        # thunder, thunder/rolling does not assign alert_vias per alert the
        # defaults are used
        if source == 'thunder' and alert_vias == ['default']:
            logger.info('spin_thunder_process - thunder rolling event')
            alert_vias = []
            alert_via_smtp = True
            alert_via_slack = False
            alert_via_pagerduty = False
            try:
                alert_via_smtp = settings.THUNDER_CHECKS[app][event_type]['alert_via_smtp']
                if alert_via_smtp:
                    logger.info('spin_thunder_process - alert_via_smtp: %s' % str(alert_via_smtp))
            except KeyError:
                alert_via_smtp = True
            except Exception as err:
                logger.error('error :: failed to determine alert_via_smtp for %s.%s check - %s' % (
                    app, event_type, err))
            if alert_via_smtp:
                alert_vias.append('alert_via_smtp')
                logger.info('spin_thunder_process - alert_via_smtp appended to alert_vias')
            try:
                alert_via_slack = settings.THUNDER_CHECKS[app][event_type]['alert_via_slack']
                logger.info('spin_thunder_process - alert_via_slack: %s' % str(alert_via_slack))
            except KeyError:
                logger.error(traceback.format_exc())
                logger.error('spin_thunder_process - alert_via_slack KeyError')
                alert_via_slack = False
            except Exception as err:
                logger.error('error :: failed to determine alert_via_slack for %s.%s check - %s' % (
                    app, event_type, err))
            if alert_via_slack:
                alert_vias.append('alert_via_slack')
                logger.info('spin_thunder_process - alert_via_slack appended to alert_vias')
            try:
                alert_via_pagerduty = settings.THUNDER_CHECKS[app][event_type]['alert_via_pagerduty']
                if alert_via_pagerduty:
                    logger.info('spin_thunder_process - alert_via_pagerduty: %s' % str(alert_via_pagerduty))
            except KeyError:
                alert_via_pagerduty = False
            except Exception as err:
                logger.error('error :: failed to determine alert_via_smtp for %s.%s check - %s' % (
                    app, event_type, err))
            if alert_via_pagerduty:
                alert_vias.append('alert_via_pagerduty')
                logger.info('spin_thunder_process - alert_via_pagerduty appended to alert_vias')

            subject = message
            body = str(data)
            alerts_sent = 0
            logger.info('spin_thunder_process - thunder rolling event alert_vias: %s' % str(alert_vias))
            for alert_via in alert_vias:
                alert_sent = False
                try:
                    if alert_via == 'alert_via_slack':
                        title = 'Skyline Thunder - %s' % level.upper()
                        with_subject = subject.replace(level, '')
                        title = title + with_subject
                        alert_sent = thunder_alert(alert_via, title, body)
                    if alert_via == 'alert_via_smtp':
                        title = 'Skyline Thunder - %s' % level.upper()
                        with_subject = subject.replace(level, '')
                        final_subject = title + with_subject
                        alert_sent = thunder_alert(alert_via, final_subject, data['status'])
                    if alert_via == 'alert_via_pagerduty':
                        alert_sent = thunder_alert(alert_via, subject, str(body))
                    if alert_sent:
                        logger.info('sent thunder_alert(%s, %s' % (
                            str(alert_via), str(subject)))
                        alerts_sent += 1
                except Exception as err:
                    logger.error('error :: failed to alert_via %s for %s.%s check - %s' % (
                        alert_via, app, event_type, err))
                cache_key = 'thunder.alert.%s.%s' % (app, event_type)
            if alerts_sent:
                if level == 'alert':
                    create_alert_cache_key(cache_key, expiry, timestamp)
                remove_event(redis_item, event_file)
            logger.info('%s alerts sent for the %s alert_vias' % (
                str(alerts_sent), str(len(alert_vias))))

        # stale metric alerts
        if source == 'analyzer' and event_type == 'stale_metrics':
            alerts_sent_dict = {}
            try:
                parent_namespace = data['namespace']
                stale_metrics = data['stale_metrics']
                alerts_sent_dict = alert_on_stale_metrics(self, level, message, parent_namespace, stale_metrics, data)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: alert_on_stale_metrics failed for %s - %s' % (
                    parent_namespace, err))
            all_sent = False
            if alerts_sent_dict:
                all_sent = alerts_sent_dict['all_sent']
                logger.info('%s alerts of %s sent for stale_metrics on %s' % (
                    str(alerts_sent_dict['to_send']),
                    str(alerts_sent_dict['sent']), parent_namespace))
            if not all_sent:
                logger.info('warning :: all alerts were not sent - %s' % (
                    str(alerts_sent_dict)))
            if all_sent:
                if level == 'alert':
                    cache_key = 'thunder.alert.%s.%s.%s.%s' % (
                        app, event_type, level, str(timestamp))
                    create_alert_cache_key(cache_key, expiry, timestamp)
                remove_event(redis_item, event_file)

        # no_data alerts
        if source == 'analyzer' and event_type == 'no_data':
            alerts_sent_dict = {}
            parent_namespace = None
            try:
                parent_namespace = data['namespace']
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: could not determine parent_namespace from data %s - %s' % (
                    str(data), err))
                remove_event(redis_item, event_file)
            send_no_data_alert = True
            if parent_namespace:
                if level == 'alert':
                    thunder_no_data_alert_key = 'thunder.alert.no_data.%s' % parent_namespace
                    thunder_no_data_alert_key_exists = False
                    try:
                        thunder_no_data_alert_key_exists = self.redis_conn_decoded.get(thunder_no_data_alert_key)
                        if thunder_no_data_alert_key_exists:
                            send_no_data_alert = False
                            logger.info('Redis key %s exists, not send no_data alert for %s' % (
                                thunder_no_data_alert_key, parent_namespace))
                            remove_event(redis_item, event_file)
                    except Exception as err:
                        logger.error('error :: failed Redis key %s - %s' % (
                            thunder_no_data_alert_key, err))
            if parent_namespace and send_no_data_alert:
                try:
                    alerts_sent_dict = alert_on_no_data(self, level, message, parent_namespace, data)
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: could not remove event_file %s - %s' % (
                        event_file, err))

                all_sent = False
                if alerts_sent_dict:
                    all_sent = alerts_sent_dict['all_sent']
                    logger.info('%s alerts of %s sent for no_data on %s' % (
                        str(alerts_sent_dict['to_send']),
                        str(alerts_sent_dict['sent']), parent_namespace))
                if not all_sent:
                    logger.info('warning :: all alerts were not sent - %s' % (
                        str(alerts_sent_dict)))
                if all_sent:
                    remove_event(redis_item, event_file)

        # @added 20231121 - Feature #5104: boundary - external_settings
        # Limit cardinality
        if source == 'analyzer' and event_type == 'boundary_cardinality_breach':
            logger.info('handling alert for boundary_cardinality_breach')
            alerts_sent_dict = {}
            try:
                parent_namespace = data['namespace_prefix']
                rejected_metrics = data['rejected_metrics']
                alerts_sent_dict = alert_on_boundary_cardinality_breach(self, level, message, parent_namespace, expiry, rejected_metrics, data)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: alert_on_boundary_cardinality_breach failed for %s - %s' % (
                    message, err))
            all_sent = False
            if alerts_sent_dict:
                all_sent = alerts_sent_dict['all_sent']
                logger.info('%s alerts of %s sent for metric_quota_exceeded on %s' % (
                    str(alerts_sent_dict['to_send']),
                    str(alerts_sent_dict['sent']), parent_namespace))
            if not all_sent:
                logger.info('warning :: all alerts were not sent - %s' % (
                    str(alerts_sent_dict)))
            if all_sent:
                cache_key = 'thunder.alert.%s.%s.%s.%s.%s' % (
                    app, event_type, level, parent_namespace, str(timestamp))
                create_alert_cache_key(cache_key, expiry, str(validated_event_details))
                remove_event(redis_item, event_file, do_log=False)

        # @added 20220202 - Feature #4412: flux - quota - thunder alert
        # metrics over quota alerts
        if source == 'flux' and event_type == 'metric_quota_exceeded':
            alerts_sent_dict = {}
            try:
                parent_namespace = data['namespace']
                rejected_metrics = data['rejected_metrics']
                alerts_sent_dict = alert_on_quota_exceeded(self, level, message, parent_namespace, expiry, rejected_metrics, data)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: alert_on_quota_exceeded failed for %s - %s' % (
                    parent_namespace, err))
            all_sent = False
            if alerts_sent_dict:
                all_sent = alerts_sent_dict['all_sent']
                logger.info('%s alerts of %s sent for metric_quota_exceeded on %s' % (
                    str(alerts_sent_dict['to_send']),
                    str(alerts_sent_dict['sent']), parent_namespace))
            if not all_sent:
                logger.info('warning :: all alerts were not sent - %s' % (
                    str(alerts_sent_dict)))
            if all_sent:
                cache_key = 'thunder.alert.%s.%s.%s.%s.%s' % (
                    app, event_type, level, parent_namespace, str(timestamp))
                create_alert_cache_key(cache_key, expiry, str(validated_event_details))
                remove_event(redis_item, event_file)

        # @added 20240319 - Feature #5312: thunder - 207 alert
        if source == 'flux' and event_type == 'invalid_data':
            alerts_sent_dict = {}
            try:
                parent_namespace = data['namespace']
                alerts_sent_dict = alert_on_invalid_metrics(self, level, message, parent_namespace, timestamp, data)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: alert_on_invalid_metrics failed for %s - %s' % (
                    parent_namespace, err))
            all_sent = False
            in_expiry = False
            if alerts_sent_dict:
                all_sent = alerts_sent_dict['all_sent']
                in_expiry = alerts_sent_dict['in_expiry']
                if not in_expiry:
                    logger.info('%s alerts of %s sent for invalid_metrics on %s' % (
                        str(alerts_sent_dict['to_send']),
                        str(alerts_sent_dict['sent']), parent_namespace))
                else:
                    ttl = alerts_sent_dict['ttl']
                    logger.info('no alerts for invalid_metrics on %s were sent, in_expiry, ttl: %s seconds' % (
                        parent_namespace, str(ttl)))
            if not all_sent:
                logger.info('warning :: all alerts were not sent - %s' % (
                    str(alerts_sent_dict)))
            if all_sent and not in_expiry:
                cache_key = 'thunder.alert.%s.%s.%s.%s.%s' % (
                    app, event_type, level, parent_namespace, str(timestamp))
                create_alert_cache_key(cache_key, 3600, str(validated_event_details))
                remove_event(redis_item, event_file)

        # @added 20240521 - Feature #5352: vista - bigquery
        if event_type == 'bq_backfill_failed':
            if not settings.SLACK_ENABLED:
                logger.info('%s thunder alert would be sent to slack for %s with %s' % (
                    event_type, message, str(data)))
                remove_event(redis_item, event_file)
            else:
                subject = message
                body = str(data)
                alerts_sent = 0
                alert_via = 'alert_via_slack'
                logger.info('spin_thunder_process - thunder rolling event alert_via: %s' % str(alert_via))
                alert_sent = False
                try:
                    title = 'Skyline Thunder - %s' % level.upper()
                    with_subject = subject.replace(level, '')
                    title = title + with_subject
                    alert_sent = thunder_alert(alert_via, title, body)
                except Exception as err:
                    logger.error('error :: failed to alert_via %s for %s.%s, err: %s' % (
                        alert_via, app, event_type, err))
                cache_key = 'thunder.alert.%s.%s' % (app, event_type)
                if alerts_sent:
                    alerts_sent = 1
                    if level == 'alert':
                        create_alert_cache_key(cache_key, expiry, timestamp)
                    remove_event(redis_item, event_file)
                logger.info('%s alerts sent for %s' % (str(alerts_sent), event_type))

        spin_end = time() - spin_start
        logger.info('spin_thunder_process took %.2f seconds' % spin_end)

        return

    def run(self):
        """
        - Called when the process intializes.

        - Determine if Redis is up and discover checks to run.

        - If Redis is down and discover checks to run from the filesystem.

        - Process event.

        - Wait for the processes to finish.

        - Repeat.

        """

        # Log management to prevent overwriting
        # Allow the bin/<skyline_app>.d to manage the log
        if os.path.isfile(skyline_app_logwait):
            try:
                os.remove(skyline_app_logwait)
            except OSError:
                logger.error('error - failed to remove %s, continuing' % skyline_app_logwait)

        now = time()
        log_wait_for = now + 5
        while now < log_wait_for:
            if os.path.isfile(skyline_app_loglock):
                sleep(.1)
                now = time()
            else:
                now = log_wait_for + 1

        logger.info('starting %s run' % skyline_app)
        if os.path.isfile(skyline_app_loglock):
            logger.error('error - bin/%s.d log management seems to have failed, continuing' % skyline_app)
            try:
                os.remove(skyline_app_loglock)
                logger.info('log lock file removed')
            except OSError:
                logger.error('error - failed to remove %s, continuing' % skyline_app_loglock)
        else:
            logger.info('bin/%s.d log management done' % skyline_app)

        try:
            SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
            if SERVER_METRIC_PATH == '.':
                SERVER_METRIC_PATH = ''
            logger.info('SERVER_METRIC_PATH is set from settings.py to %s' % str(SERVER_METRIC_PATH))
        except:
            SERVER_METRIC_PATH = ''
            logger.info('warning :: SERVER_METRIC_PATH is not declared in settings.py, defaults to \'\'')
        logger.info('skyline_app_graphite_namespace is set to %s' % str(skyline_app_graphite_namespace))

        if not os.path.exists(settings.SKYLINE_TMP_DIR):
            try:
                mkdir_p(settings.SKYLINE_TMP_DIR)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to create %s - %s' % (settings.SKYLINE_TMP_DIR, err))

        # Create the required THUNDER directories which are failed over to and
        # used in the event that Redis is down
        if not os.path.exists(THUNDER_EVENTS_DIR):
            try:
                mkdir_p(THUNDER_EVENTS_DIR)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to create %s - %s' % (THUNDER_EVENTS_DIR, err))
        if not os.path.exists(THUNDER_KEYS_DIR):
            try:
                mkdir_p(THUNDER_KEYS_DIR)
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to create %s - %s' % (THUNDER_KEYS_DIR, err))

        last_sent_to_graphite = int(time())
        thunder_alerts_sent = 0
        last_check_for_events_on_filesystem = int(last_sent_to_graphite)

        while True:
            now = time()
            # Make sure Redis is up
            try:
                self.redis_conn.ping()
                logger.info('Redis ping OK')
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: cannot connect to redis at socket path %s - %s' % (
                    settings.REDIS_SOCKET_PATH, err))
                sleep(10)
                try:
                    self.redis_conn = get_redis_conn(skyline_app)
                    logger.info('connected via get_redis_conn')
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: not connected via get_redis_conn - %s' % err)
                # continue
            try:
                self.redis_conn_decoded.ping()
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: not connected via get_redis_conn_decoded - %s' % err)
                sleep(10)
                try:
                    self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)
                    logger.info('connected via get_redis_conn_decoded')
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: cannot connect via get_redis_conn_decoded - %s' % err)
                # continue

            # Determine if any metric has been added to process
            thunder_last_run = int(time())
            total_thunder_events_item_count = 0
            while True:

                validated_event_details = {}

                current_timestamp = int(time())

                if total_thunder_events_item_count == 0:
                    if (current_timestamp - thunder_last_run) < 3:
                        sleep(2)
                thunder_last_run = int(current_timestamp)

                # Report app AND Redis as up
                redis_is_up = False
                try:
                    redis_is_up = self.redis_conn.setex(skyline_app, 120, current_timestamp)
                    if redis_is_up:
                        try:
                            logger.info('set thunder Redis key')
                            self.redis_conn.setex('redis', 120, current_timestamp)
                        except Exception as err:
                            logger.error(traceback.format_exc())
                            logger.error('error :: could not update the Redis redis key - %s' % (
                                err))
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: could not update the Redis %s key - %s' % (
                        skyline_app, err))

                if not settings.THUNDER_ENABLED:
                    logger.info('sleeping 59 seconds settings.THUNDER_ENABLED not set')
                    sleep(59)
                    logger.info('breaking after 59 seconds settings.THUNDER_ENABLED not set')
                    break

                # break to send metrics
                if int(time()) >= (last_sent_to_graphite + 60):
                    logger.info('breaking as last_sent_to_graphite was > 59 seconds ago')
                    break

                # Determine events to process from the Redis set
                thunder_events = []
                if redis_is_up:
                    try:
                        # @modified 20220110 - Bug #4364: Prune old thunder.events
                        #                      Branch #1444: thunder
                        # thunder_events = self.redis_conn_decoded.smembers(thunder_redis_set)
                        thunder_events = list(self.redis_conn_decoded.smembers(thunder_redis_set))
                    except Exception as err:
                        logger.error('error :: could not query Redis for set %s - %s' % (thunder_redis_set, err))

                # If no data was returned from Redis ensure thunder_events is
                # a set so that any event_files can be added to the set
                if thunder_events:
                    logger.info('%s entries in thunder.events Redis set' % str(len(thunder_events)))
                    if not isinstance(thunder_events, set):
                        thunder_events = set(thunder_events)
                else:
                    logger.info('no entries in thunder.events Redis set')
                    # @modified 20240206 - Task #4962: Build and test skyline v4.0.0
                    # Ensure this is a set so it can be added to not appended
                    # thunder_events = []
                    thunder_events = set([])

                # Check the filesystem for failover event files
                filesystem_check_timestamp = int(time())
                if (last_check_for_events_on_filesystem + 60) >= filesystem_check_timestamp:
                    last_check_for_events_on_filesystem = filesystem_check_timestamp
                    logger.info('checking for failover event files in %s' % THUNDER_EVENTS_DIR)
                    thunder_event_files_count = 0
                    for root, dirs, files in os.walk(THUNDER_EVENTS_DIR):
                        if files:
                            for file in files:
                                event_file = '%s/%s' % (root, file)
                                try:
                                    data_dict = None
                                    try:
                                        with open(event_file, 'r') as f:
                                            data_dict_str = f.read()
                                    except Exception as err:
                                        logger.error('error :: failed to open event_file: %s - %s' % (event_file, err))

                                    try:
                                        data_dict = literal_eval(data_dict_str)

                                        # @modified 20240206 - Task #4962: Build and test skyline v4.0.0
                                        # Do not do multiple actions here do
                                        # them individually.  Moved to below
                                        # so that only literal_eval is being
                                        # done here
                                        # if data_dict:
                                        #     data_dict['event_file'] = event_file
                                        #     # thunder_events.add(str(data_dict))
                                        #     thunder_events.append(str(data_dict))
                                    except Exception as err:
                                        logger.error('error :: failed to literal_eval event_file: %s - %s' % (event_file, err))
                                    # @added 20240206 - Task #4962: Build and test skyline v4.0.0
                                    # Moved from above to run individually and
                                    # not in the literal_eval state
                                    if data_dict:
                                        try:
                                            data_dict['event_file'] = event_file
                                            # Add to the set, set has no append
                                            thunder_events.add(str(data_dict))
                                        except Exception as err:
                                            logger.error('error :: failed to add data to thunder_events set from event_file: %s to data_dict: %s, err: %s' % (
                                                str(event_file), str(data_dict), err))
                                except Exception as err:
                                    logger.error('failed evaluate event_file %s - %s' % (
                                        event_file, err))
                    logger.info('%s thunder failover event files found' % str(thunder_event_files_count))

                    # Check the filesystem for failover key files
                    logger.info('checking for failover keys in %s' % THUNDER_KEYS_DIR)
                    thunder_key_files_count = 0
                    for root, dirs, files in os.walk(THUNDER_KEYS_DIR):
                        if files:
                            for file in files:
                                thunder_key_file = '%s/%s' % (root, file)
                                try:
                                    key_dict = None
                                    try:
                                        with open(thunder_key_file, 'r') as f:
                                            key_dict_str = f.read()
                                        key_dict = literal_eval(key_dict_str)
                                        thunder_key_files_count += 1
                                    except Exception as err:
                                        logger.error('error :: failed to open thunder_key_file: %s - %s' % (
                                            thunder_key_file, err))
                                    timestamp = 0
                                    if key_dict:
                                        try:
                                            timestamp = key_dict['timestamp']
                                            expiry = int(key_dict['expiry'])
                                        except Exception as err:
                                            logger.error('error :: failed to determine timestamp and expiry from key_dict created from thunder_key_file: %s - %s' % (
                                                thunder_key_file, err))
                                    if timestamp:
                                        now = int(time())
                                        if (timestamp + expiry) >= now:
                                            expiry = 0
                                            try:
                                                removed_file = remove_file(thunder_key_file)
                                                if removed_file:
                                                    logger.info('removed expired thunder_key_file: %s' % (
                                                        thunder_key_file))
                                            except Exception as err:
                                                logger.error('error :: failed to remove %s, continuing - %s' % (
                                                    thunder_key_file, err))
                                        if (timestamp + expiry) <= now:
                                            expiry = now - (timestamp + expiry)
                                except Exception as err:
                                    logger.error('failed evaluate thunder_key_file: %s - %s' % (
                                        thunder_key_file, err))
                    logger.info('%s thunder failover key files found' % str(thunder_key_files_count))

                # @added 20220222 -Branch #1444: thunder
                # Create a list for redis_items
                redis_items = []

                total_thunder_events_item_count = len(thunder_events)
                validated_event_details = {}
                if thunder_events:
                    logger.info('getting a thunder event to process from the %s events' % str(total_thunder_events_item_count))
                    for index, event_item in enumerate(thunder_events):
                        # if validated_event_details:
                        #     break
                        try:
                            remove_item = False
                            redis_item = event_item
                            try:
                                event_details = literal_eval(event_item)
                            except Exception as err:
                                remove_item = True
                                event_details = None
                                logger.error('error :: could not determine event_details from %s Redis set entry - %s' % (
                                    thunder_redis_set, err))
                            missing_required_keys = False
                            if event_details:
                                try:
                                    event_type = str(event_details['event_type'])
                                except KeyError:
                                    event_type = str(event_details['type'])
                                except Exception as err:
                                    logger.error('error :: failed to determine type from event_details dict - %s' % (
                                        err))
                                    event_type = False
                                # @modified 20231121 - Feature #5104: boundary - external_settings
                                # Limit cardinality
                                # if event_type != 'metric_quota_exceeded':
                                if event_type not in ['metric_quota_exceeded', 'boundary_cardinality_breach']:
                                    logger.info('validating thunder event_details: %s' % str(event_details))
                                else:
                                    logger.info('validating thunder event_details for %s' % str(event_type))
                                try:
                                    level = str(event_details['level'])
                                except KeyError:
                                    level = 'alert'
                                except Exception as err:
                                    logger.error('error :: failed to determine level from event_details dict set to alert - %s' % (
                                        err))
                                    level = 'alert'
                                validated_event_details['level'] = level
                                try:
                                    event_type = str(event_details['event_type'])
                                except KeyError:
                                    event_type = str(event_details['type'])
                                except Exception as err:
                                    logger.error('error :: failed to determine type from event_details dict - %s' % (
                                        err))
                                    event_type = False
                                validated_event_details['event_type'] = event_type
                                try:
                                    message = str(event_details['message'])
                                except KeyError:
                                    message = False
                                except Exception as err:
                                    logger.error('error :: failed to determine message from event_details dict - %s' % (
                                        err))
                                    message = False
                                validated_event_details['message'] = message
                                try:
                                    app = str(event_details['app'])
                                except KeyError:
                                    app = False
                                except Exception as err:
                                    logger.error('error :: failed to determine app from event_details dict - %s' % (
                                        err))
                                    app = False
                                validated_event_details['app'] = app
                                try:
                                    metric = str(event_details['metric'])
                                except KeyError:
                                    metric = False
                                except Exception as err:
                                    logger.error('error :: failed to determine metric from event_details dict - %s' % (
                                        err))
                                    metric = False
                                validated_event_details['metric'] = metric
                                try:
                                    source = str(event_details['source'])
                                except KeyError:
                                    source = False
                                except Exception as err:
                                    logger.error('error :: failed to determine source from event_details dict - %s' % (
                                        err))
                                    source = False
                                validated_event_details['source'] = source
                                try:
                                    expiry = int(event_details['expiry'])
                                except KeyError:
                                    expiry = 900
                                except Exception as err:
                                    logger.error('error :: failed to determine expiry from event_details dict - %s' % (
                                        err))
                                    expiry = 900
                                validated_event_details['expiry'] = expiry
                                try:
                                    timestamp = event_details['timestamp']
                                except KeyError:
                                    timestamp = int(time())
                                except Exception as err:
                                    logger.error('error :: failed to determine timestamp from event_details dict - %s' % (
                                        err))
                                    timestamp = int(time())
                                validated_event_details['timestamp'] = timestamp
                                try:
                                    alert_vias = event_details['alert_vias']
                                except KeyError:
                                    alert_vias = []
                                except Exception as err:
                                    logger.error('error :: failed to determine alert_vias from event_details dict - %s' % (
                                        err))
                                    alert_vias = []
                                validated_event_details['alert_vias'] = alert_vias
                                if source == 'thunder':
                                    validated_event_details['alert_vias'] = ['default']
                                try:
                                    data = event_details['data']
                                except Exception as err:
                                    logger.error('error :: failed to determine data from event_details dict - %s' % (
                                        err))
                                    data = {'status': None}
                                validated_event_details['data'] = data
                                # Add the event_file, this is related to files used
                                # for events and keys where a Redis failure is
                                # experienced
                                try:
                                    event_file = event_details['event_file']
                                except KeyError:
                                    event_file = None
                                except Exception as err:
                                    logger.error('error :: failed to determine event_file from event_details dict - %s' % (
                                        err))
                                    event_file = None
                                validated_event_details['event_file'] = event_file

                                if not event_type:
                                    missing_required_keys = True
                                if not app:
                                    missing_required_keys = True
                                if not message:
                                    missing_required_keys = True

                            if missing_required_keys or remove_item:
                                logger.info('invalidating thunder event_details, missing_required_keys: %s' % str(missing_required_keys))
                                validated_event_details = {}
                                if not event_file:
                                    # Delete the bad item in the Redis set
                                    try:
                                        # @modified 20210907 - Bug #4258: cleanup thunder.events
                                        # removed_item = update_redis_set(
                                        update_redis_set(
                                            skyline_app, thunder_redis_set, event_item,
                                            'remove', True)
                                        # if removed_item:
                                        #     logger.error('error :: could not determine event_details from %s Redis set entry (removed) - %s' % (
                                        #         thunder_redis_set, str(event_item)))
                                    except Exception as err:
                                        logger.error('error :: could not remove bad item from Redis set %s - %s' % (
                                            thunder_redis_set, err))
                                else:
                                    # Delete the bad event_file
                                    removed_file = False
                                    try:
                                        removed_file = remove_file(skyline_app, event_file)
                                    except Exception as err:
                                        logger.error('error :: could not remove bad event_file %s - %s' % (
                                            event_file, err))
                                    if removed_file:
                                        logger.error('error :: could not determine event_details from the event_file (removed) - %s' % (
                                            str(event_file)))
                                continue
                        except Exception as err:
                            logger.error(traceback.format_exc())
                            logger.error('error :: validating and checking event - %s' % (
                                err))

                        if validated_event_details:
                            logger.info('thunder event_details validated')
                            if validated_event_details['source'] == 'thunder':
                                validated_event_details['alert_vias'] = ['default']
                            logger.info('thunder event_details validated')

                            # @added 20220222 -Branch #1444: thunder
                            # Add to list for redis_items
                            redis_items.append(redis_item)

                        # Check if an alert has gone out if so removed the item
                        if validated_event_details and level == 'alert':
                            alert_cache_key = 'thunder.alert.%s.%s.%s.%s' % (
                                app, event_type, level, str(timestamp))
                            alerted = None
                            try:
                                alerted = self.redis_conn_decoded.get(alert_cache_key)
                            except Exception as err:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to get %s Redis key - %s' % (
                                    alert_cache_key, err))
                            if not alerted:
                                alerted = check_thunder_failover_key(self, alert_cache_key)
                            if alerted:
                                logger.info('alert already sent for %s, removing event item' % alert_cache_key)
                                validated_event_details = {}
                                if redis_item:
                                    # Delete the item from the Redis set
                                    try:
                                        # @added 20220303 - Feature #4412: flux - quota - thunder alert
                                        do_log = True
                                        try:
                                            redis_item_dict = literal_eval(redis_item)
                                            if isinstance(redis_item_dict, dict):
                                                if redis_item_dict['event_type'] == 'metric_quota_exceeded':
                                                    do_log = False
                                                    logger.info('not logging total event string as metric_quota_exceeded, sampling with data removed')
                                                    try:
                                                        del redis_item_dict['data']
                                                    except Exception as err:
                                                        logger.error('error :: failed to removed data key from dict - %s' % err)
                                                    logger.info('sample: %s' % str(redis_item_dict))
                                                    
                                                # @added 20231121 - Feature #5104: boundary - external_settings
                                                # Limit cardinality
                                                if redis_item_dict['event_type'] == 'boundary_cardinality_breach':
                                                    do_log = False
                                                    logger.info('not logging total event string as boundary_cardinality_breach, sampling with data removed')
                                                    try:
                                                        del redis_item_dict['data']['rejected_metrics']
                                                    except Exception as err:
                                                        logger.error('error :: failed to removed data key from dict - %s' % err)
                                                    logger.info('sample: %s' % str(redis_item_dict))

                                        except Exception as err:
                                            logger.error('error :: failed literal_eval the redis_item - %s' % err)

                                        # @modified 20210907 - Bug #4258: cleanup thunder.events
                                        # removed_item = update_redis_set(
                                        update_redis_set(
                                            skyline_app, thunder_redis_set, redis_item,
                                            # @modified 20220303 - Feature #4412: flux - quota - thunder alert
                                            # 'remove', log=True)
                                            'remove', do_log)
                                        # if removed_item:
                                        #     logger.info('alert key exists, removed event_details from %s Redis set entry - %s' % (
                                        #         thunder_redis_set, str(redis_item)))
                                    except Exception as err:
                                        logger.error('error :: could not remove item from Redis set %s - %s' % (
                                            thunder_redis_set, err))
                                if event_file:
                                    # Delete the bad event_file
                                    removed_file = False
                                    try:
                                        removed_file = remove_file(skyline_app, event_file)
                                    except Exception as err:
                                        logger.error('error :: could not remove event_file %s - %s' % (
                                            event_file, err))
                                    if removed_file:
                                        logger.info('alert key exists, event_file removed - %s' % (
                                            str(event_file)))
                                continue

                        if validated_event_details:
                            # Check if the event has been actioned in the
                            # current run, if so skip.
                            # until the key expires
                            current_event_cache_key = 'thunder.current.%s.%s.%s.%s' % (
                                app, event_type, level, str(timestamp))
                            current_event = None
                            try:
                                current_event = self.redis_conn_decoded.get(current_event_cache_key)
                                if current_event:
                                    logger.info('current_event_cache_key exist in Redis %s for this event, skipping' % current_event_cache_key)
                            except Exception as err:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to get %s Redis key - %s' % (
                                    current_event_cache_key, err))
                            if not current_event:
                                current_event = check_thunder_failover_key(self, current_event_cache_key)
                                if current_event:
                                    logger.info('current_event_cache_key exist %s as a check_thunder_failover_key for this event, skipping' % current_event_cache_key)
                            if current_event:
                                validated_event_details = {}
                                logger.info('current_event_cache_key exist %s for this event, skipping' % current_event_cache_key)
                                # continue
                        if validated_event_details:
                            try:
                                self.redis_conn_decoded.setex(current_event_cache_key, 59, int(time()))
                            except Exception as err:
                                logger.error(traceback.format_exc())
                                logger.error('error :: failed to setex %s Redis key - %s' % (
                                    current_event_cache_key, err))
                                try:
                                    failover_key_file = '%s/%s' % (THUNDER_KEYS_DIR, current_event_cache_key)
                                    failover_key_data = {'timestamp': int(time()), 'expiry': 59}
                                    write_data_to_file(
                                        skyline_app, failover_key_file, 'w',
                                        str(failover_key_data))
                                    logger.info('added Redis failover - failover_key_file - %s' % (failover_key_file))
                                except Exception as err:
                                    logger.error('error :: failed to add Redis failover failover_key_file - %s - %s' % (failover_key_file, err))
                            # @modified 20220110 - Bug #4364: Prune old thunder.events
                            #                      Branch #1444: thunder
                            # redis_item = event_item
                            break

                if not validated_event_details:
                    sleep_for = 30
                    right_now = int(time())
                    next_send_to_graphite = last_sent_to_graphite + 60
                    if right_now >= next_send_to_graphite:
                        sleep_for = 0.1
                    if (next_send_to_graphite - right_now) < sleep_for:
                        sleep_for = next_send_to_graphite - right_now
                    logger.info('no validated_event_details sleeping for %s seconds' % str(sleep_for))
                    sleep(sleep_for)
                    if int(time()) >= (last_sent_to_graphite + 60):
                        logger.info('breaking to sending Graphite metrics')
                        break

                if validated_event_details:
                    logger.info('processing 1 event of %s thunder events to process' % str(total_thunder_events_item_count))

                    # Spawn processes
                    pids = []
                    spawned_pids = []
                    pid_count = 0

                    THUNDER_PROCESSES = 1
                    for i in range(1, THUNDER_PROCESSES + 1):
                        p = Process(target=self.spin_thunder_process, args=(i, validated_event_details, redis_item, event_file))
                        pids.append(p)
                        pid_count += 1
                        logger.info('starting spin_thunder_process')
                        p.start()
                        spawned_pids.append(p.pid)
                        thunder_alerts_sent += 1

                    # Send wait signal to zombie processes
                    # for p in pids:
                    #     p.join()
                    # Self monitor processes and terminate if any spin_thunder_process
                    # that has run for longer than 58 seconds
                    p_starts = time()
                    while time() - p_starts <= 58:
                        if any(p.is_alive() for p in pids):
                            # Just to avoid hogging the CPU
                            sleep(.1)
                        else:
                            # All the processes are done, break now.
                            time_to_run = time() - p_starts
                            logger.info('1 spin_thunder_process completed in %.2f seconds' % (time_to_run))
                            break
                    else:
                        # We only enter this if we didn't 'break' above.
                        logger.info('timed out, killing all spin_thunder_process processes')
                        for p in pids:
                            p.terminate()
                            # p.join()

                    for p in pids:
                        if p.is_alive():
                            logger.info('stopping spin_thunder_process - %s' % (str(p.is_alive())))
                            # @modified 20240202 - Task #5178: Build and test skyline v4.1.0
                            # p.join()
                            killing_pid = p.pid
                            logger.info('kill spin_thunder_process with pid: %s' % (str(killing_pid)))
                            p.terminate()
                            logger.info('killed spin_thunder_process process with pid: %s' % (str(killing_pid)))

                    # @added 20210907 - Bug #4258: cleanup thunder.events
                    # Remove event
                    # @modified 20220222 -Branch #1444: thunder
                    # Iterate redis_items list, rather than assuming a single
                    # redis_item
                    for redis_item in redis_items:
                        try:

                            # @added 20220303 - Feature #4412: flux - quota - thunder alert
                            do_log = True
                            try:
                                redis_item_dict = literal_eval(redis_item)
                                if isinstance(redis_item_dict, dict):
                                    if redis_item_dict['event_type'] == 'metric_quota_exceeded':
                                        do_log = False
                                        logger.info('not logging total event string as metric_quota_exceeded, sampling with data removed')
                                        try:
                                            del redis_item_dict['data']
                                        except Exception as err:
                                            logger.error('error :: failed to removed data key from dict - %s' % err)
                                        logger.info('sample: %s' % str(redis_item_dict))
                            except Exception as err:
                                logger.error('error :: failed literal_eval the redis_item - %s' % err)

                            update_redis_set(
                                skyline_app, thunder_redis_set, redis_item,
                                'remove', do_log)
                        except Exception as err:
                            logger.error('error :: could not remove item from Redis set %s - %s' % (
                                thunder_redis_set, err))

            if int(time()) >= (last_sent_to_graphite + 60):
                logger.info('sending Graphite metrics')

                logger.info('alerts.sent          :: %s' % str(thunder_alerts_sent))
                send_metric_name = '%s.alerts.sent' % skyline_app_graphite_namespace
                send_graphite_metric(self, skyline_app, send_metric_name, str(thunder_alerts_sent))

                last_sent_to_graphite = int(time())
                thunder_alerts_sent = 0

                # @modified 20210909 - Bug #4258: cleanup thunder.events
                # Not required here
                # try:
                #     thunder_events = self.redis_conn_decoded.smembers(thunder_redis_set)
                # except Exception as err:
                #     logger.error('error :: could not query Redis for set %s - %s' % (thunder_redis_set, err))

                # @added 20210907 - Bug #4258: cleanup thunder.events
                # The original version of thunder never removed the
                # thunder.events after processing, the event was only
                # removed if it was bad.  Therefore no stale, no_data or
                # recovered events were removed from the thunder.events
                # Redis set.
                # This feature works to be able to clean up the
                # thunder.events of any big thunder.events sets and manages
                # the set going forward.
                thunder_events_list = []
                logger.info('managing thunder.events Redis set and removing any items older than 86400')
                try:
                    thunder_events_list = list(self.redis_conn_decoded.smembers(thunder_redis_set))
                except Exception as err:
                    logger.error('error :: could not query Redis for set %s - %s' % (thunder_redis_set, err))
                if not thunder_events_list:
                    logger.info('managed thunder.events Redis set, no items in set')
                if thunder_events_list:
                    logger.info('managing %s items in thunder.events Redis set' % str(len(thunder_events_list)))
                    for thunder_event_str in thunder_events_list:
                        try:
                            thunder_event = None
                            remove_item = False
                            try:
                                thunder_event = literal_eval(thunder_event_str)
                            except Exception as err:
                                logger.error('error :: could not literal_eval(thunder_events_str) - %s' % str(err))
                            thunder_event_timestamp = 0
                            if thunder_event:
                                try:
                                    thunder_event_timestamp = int(float(thunder_event['timestamp']))
                                except KeyError:
                                    # No timestamp, remove event
                                    remove_item = thunder_event_str
                                    thunder_event_timestamp = None
                            if thunder_event_timestamp:
                                # @modified 20220307 -
                                # if thunder_event_timestamp > (last_sent_to_graphite - 86400):
                                if (current_timestamp - 86400) > thunder_event_timestamp:
                                    remove_item = thunder_event_str
                            if remove_item:
                                # Remove event

                                # @added 20220303 - Feature #4412: flux - quota - thunder alert
                                do_log = True
                                try:
                                    redis_item_dict = literal_eval(thunder_event_str)
                                    if isinstance(redis_item_dict, dict):
                                        if redis_item_dict['event_type'] == 'metric_quota_exceeded':
                                            do_log = False
                                            logger.info('removing event from thunder.events Redis set as the event is older than 86400 seconds')
                                            logger.info('not logging total event string as metric_quota_exceeded, sampling with data removed')
                                            try:
                                                del redis_item_dict['data']
                                            except Exception as err:
                                                logger.error('error :: failed to removed data key from dict - %s' % err)
                                            logger.info('sample: %s' % str(redis_item_dict))
                                except Exception as err:
                                    logger.error('error :: failed literal_eval the redis_item - %s' % err)

                                if do_log:
                                    logger.info('removing event from thunder.events Redis set as the event is older than 86400 seconds. event: %s' % (
                                        str(thunder_event)))
                                try:
                                    update_redis_set(
                                        # @modified 20220222 -Branch #1444: thunder
                                        # Use thunder_event_str rather than redis_item
                                        # skyline_app, thunder_redis_set, redis_item,
                                        skyline_app, thunder_redis_set,
                                        thunder_event_str, 'remove', do_log)
                                except Exception as err:
                                    logger.error('error :: could not remove item from Redis set %s - %s' % (
                                        thunder_redis_set, err))
                        except Exception as err:
                            logger.error(traceback.format_exc())
                            logger.error('error :: failed to manage a thunder_event in the thunder_events_list - %s' % err)
