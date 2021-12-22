import logging
from time import time, sleep
from threading import Thread
from multiprocessing import Process
import os
import sys
from os import kill, getpid
import traceback
from ast import literal_eval

import settings
from skyline_functions import get_redis_conn, get_redis_conn_decoded
from functions.metrics.get_metric_latest_anomaly import get_metric_latest_anomaly
from functions.database.queries.get_metric_group_info import get_metric_group_info
from functions.luminosity.get_cross_correlation_relationships import get_cross_correlation_relationships
from functions.luminosity.update_metric_group import update_metric_group

skyline_app = 'luminosity'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)
skyline_app_loglock = '%s.lock' % skyline_app_logfile
skyline_app_logwait = '%s.wait' % skyline_app_logfile

this_host = str(os.uname()[1])

try:
    SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

try:
    LUMINOSITY_RELATED_METRICS_MAX_5MIN_LOADAVG = settings.LUMINOSITY_RELATED_METRICS_MAX_5MIN_LOADAVG
except:
    LUMINOSITY_RELATED_METRICS_MAX_5MIN_LOADAVG = 3

skyline_app_graphite_namespace = 'skyline.%s%s' % (skyline_app, SERVER_METRIC_PATH)


class RelatedMetrics(Thread):
    """
    The RelatedMetrics class controls the luminosity/related_metrics thread and
    spawned processes. luminosity/related_metrics analyses the results of
    luminosity cross_correlations and related_metricss to create and maintain
    metric groups.
    """

    def __init__(self, parent_pid):
        """
        Initialize RelatedMetrics
        """
        super(RelatedMetrics, self).__init__()
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
            sys.exit(0)

    def find_related(self, i):
        """
        - Determine when a metric group was last updated.
        - Determine if any new anomalies have occurred on the metric.
        - If there are any new anomalies on the metric, determine if new
          cross_correlations or related_metrics have occured on a metric
        - If there are new cross_correlations or related_metricss calculate the
          metric group with the new data
        - Determine if the new data modifies the metric group, if so update.
        """
        max_execution_seconds = 50
        find_related_start = time()
        logger.info('related_metrics :: find_related :: process %s started' % str(i))

        current_5min_loadavg = os.getloadavg()[1]
        if current_5min_loadavg > LUMINOSITY_RELATED_METRICS_MAX_5MIN_LOADAVG:
            logger.info('related_metrics :: find_related :: not processing any metrics as current_5min_loadavg: %s, exceeds LUMINOSITY_RELATED_METRICS_MAX_5MIN_LOADAVG: %s' % (
                str(current_5min_loadavg),
                str(LUMINOSITY_RELATED_METRICS_MAX_5MIN_LOADAVG)))
            return

        metrics_to_process = []

        # If a set exists process that
        force_process = False
        try:
            metrics_to_process = list(self.redis_conn_decoded.smembers('luminosity.related_metrics.process_immediate'))
            if metrics_to_process:
                force_process = True
                logger.info('related_metrics :: find_related :: %s metrics found in luminosity.related_metrics.process_immediate: %s' % (
                    str(len(metrics_to_process)), str(metrics_to_process)))
                self.redis_conn_decoded.delete('luminosity.related_metrics.process_immediate')
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: related_metrics :: failed get aet.analyzer.smtp_alerter_metrics Redis key - %s' % (
                str(err)))
            metrics_to_process = []

        # Get all alerting metric basenames
        if not metrics_to_process:
            try:
                metrics_to_process = list(self.redis_conn_decoded.smembers('aet.analyzer.smtp_alerter_metrics'))
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: related_metrics :: failed get aet.analyzer.smtp_alerter_metrics Redis key - %s' % (
                    str(err)))
                metrics_to_process = []

        metrics_to_process_count = len(metrics_to_process)
        # Get through the population once per day
        optimal_metrics_per_minute = metrics_to_process_count / 1440
        # optimal_metrics_per_minute = metrics_to_process_count / 360

        if force_process:
            optimal_metrics_per_minute = metrics_to_process_count
            logger.info('related_metrics :: find_related :: force_process so optimal metrics to process per minute: %s' % str(optimal_metrics_per_minute))

        if optimal_metrics_per_minute < 1:
            optimal_metrics_per_minute = 1
        logger.info('related_metrics :: find_related :: optimal metrics to process per minute: %s' % str(optimal_metrics_per_minute))

        metric_names_with_ids = {}
        ids_with_metric_names = {}
        try:
            metric_names_with_ids = self.redis_conn_decoded.hgetall('aet.metrics_manager.metric_names_with_ids')
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: related_metrics :: find_related :: failed to get Redis hash aet.metrics_manager.metric_names_with_ids - %s' % str(err))
        if metric_names_with_ids:
            for c_metric_name in list(metric_names_with_ids.keys()):
                c_metric_id = int(str(metric_names_with_ids[c_metric_name]))
                ids_with_metric_names[c_metric_id] = c_metric_name

        metric_group_last_updated = {}
        try:
            metric_group_last_updated = self.redis_conn_decoded.hgetall('luminosity.metric_group.last_updated')
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: related_metrics :: find_related :: failed to get Redis hash luminosity.metric_group.last_updated - %s' % str(err))
            metric_group_last_updated = {}
        logger.info('related_metrics :: find_related :: all eligible metric names and ids determined')

        latest_anomalies = {}
        try:
            latest_anomalies = self.redis_conn_decoded.hgetall('panorama.metrics.latest_anomaly')
        except Exception as err:
            logger.error(traceback.format_exc())
            logger.error('error :: related_metrics :: find_related :: failed to get Redis hash luminosity.metric_group.last_updated - %s' % str(err))
            metric_group_last_updated = {}

        metrics_checked = []
        metric_groups_updated = []
        metrics_to_check = []
        metrics_skipped_recently_checked = []
        metrics_skipped_recent_anomaly = []
        metrics_skipped_no_anomaly = []

        # Determine which metrics have new anomalies and may require their
        # metric_group updated
        for base_name in metrics_to_process:
            if len(metrics_to_check) >= optimal_metrics_per_minute:
                break

            # Determine if the metric group needs to be built or checked
            metric_id = 0
            metric_id_str = None
            try:
                metric_id_str = metric_names_with_ids[base_name]
                if metric_id_str:
                    metric_id = int(metric_id_str)
            except KeyError:
                metric_id = 0
            if not metric_id:
                logger.error('error :: related_metrics :: find_related :: failed to get determine metric id for %s from Redis hash data aet.metrics_manager.metric_names_with_ids' % str(base_name))
                continue

            # force_process
            if force_process:
                logger.info('related_metrics :: find_related :: force_process, %s adding to check' % (
                    base_name))
                metrics_to_check.append([metric_id, base_name])
                continue

            metric_info_last_updated = 0
            try:
                metric_info_last_updated_str = None
                # Get the entire hash key once
                # metric_info_last_updated_str = self.redis_conn_decoded.hget('luminosity.metric_group.last_updated', metric_id)
                try:
                    metric_info_last_updated_str = metric_group_last_updated[str(metric_id)]
                except KeyError:
                    metric_info_last_updated_str = None
                if metric_info_last_updated_str:
                    metric_info_last_updated = int(str(metric_info_last_updated_str))
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: related_metrics :: find_related :: failed to get Redis hash aet.metrics_manager.metric_names_with_ids - %s' % str(err))
            # Check the DB just in case Redis data was lost this will stop all
            # metrics being reanalysed if the Redis data is lost
            metric_group_info = {}
            if not metric_info_last_updated:
                logger.info('debug :: related_metrics :: find_related :: %s last_updated timestamp not found in Redis hash luminosity.metric_group.last_updated querying DB' % (
                    base_name))
                try:
                    metric_group_info = get_metric_group_info(skyline_app, metric_id)
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: related_metrics :: find_related :: get_metric_group_info failed - %s' % str(err))
                try:
                    metric_info_last_updated = metric_group_info[metric_id]['last_updated']
                except KeyError:
                    metric_info_last_updated = 0
            # Never checked
            if not metric_info_last_updated:
                logger.info('related_metrics :: find_related :: %s has no last_updated timestamp adding to check' % (
                    base_name))
                metrics_to_check.append([metric_id, base_name])
                continue

            # if metric_info_last_updated > (int(find_related_start) - 86400):
            if metric_info_last_updated > (int(find_related_start) - 14400):
                metrics_skipped_recently_checked.append(base_name)
                continue

            metric_last_anomaly_ts = 0
            latest_anomaly = {}
            try:
                latest_anomaly_str = latest_anomalies[base_name]
                if latest_anomaly_str:
                    latest_anomaly = literal_eval(str(latest_anomaly_str))
                if latest_anomaly:
                    metric_last_anomaly_ts = latest_anomaly['anomaly_timestamp']
            except KeyError:
                metric_last_anomaly_ts = 0
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: related_metrics :: find_related :: failed literal_eval latest_anomaly - %s' % str(err))
                metric_last_anomaly_ts = 0

            if not metric_last_anomaly_ts:
                try:
                    # TODO optimise anomalies DB requests to be Redis instead
                    # params = {'latest': True}
                    # latest_metric_anomaly = get_anomalies(skyline_app, metric_id, params)
                    # Causes a lot of DB queries for metrics without anomalies
                    # latest_anomaly = get_metric_latest_anomaly(skyline_app, base_name, metric_id, False)
                    latest_anomaly = {}
                    if latest_anomaly:
                        try:
                            metric_last_anomaly_ts = latest_anomaly['anomaly_timestamp']
                        except Exception as err:
                            logger.error(traceback.format_exc())
                            logger.error('error :: related_metrics :: find_related :: failed to determine anomaly_timestamp for get_anomalies dict - %s' % str(err))
                            metric_last_anomaly_ts = 0
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: related_metrics :: find_related :: get_anomalies failed - %s' % str(err))
                    metric_last_anomaly_ts = 0

            if not metric_last_anomaly_ts:
                metrics_skipped_no_anomaly.append(base_name)
                try:
                    self.redis_conn_decoded.hset('luminosity.metric_group.last_updated', metric_id, int(find_related_start))
                except Exception as err:
                    logger.error(traceback.format_exc())
                    logger.error('error :: related_metrics :: find_related :: failed to update in metric_id: %s luminosity.metric_group.last_updated - %s' % (
                        str(metric_id), str(err)))
                continue

            # Allow correlations to occur
            if metric_last_anomaly_ts > (int(find_related_start) - 900):
                metrics_skipped_recent_anomaly.append(base_name)
                continue
            if metric_last_anomaly_ts > metric_info_last_updated:
                logger.info('related_metrics :: find_related :: recent anomaly on %s adding to check' % base_name)
                metrics_to_check.append([metric_id, base_name])

        logger.info('related_metrics :: find_related :: %s metrics in metrics_to_check' % str(len(metrics_to_check)))
        logger.info('related_metrics :: find_related :: metrics_to_check: %s' % str(metrics_to_check))
        for metric_id, base_name in metrics_to_check:
            time_now = int(time())
            running_for = time_now - int(find_related_start)
            if running_for > max_execution_seconds:
                logger.info('related_metrics :: find_related :: stopping after running for %s seconds, reaching max_execution_seconds' % str(running_for))
                break
            metrics_checked.append(base_name)
            cross_correlation_relationships = {}
            try:
                cross_correlation_relationships_dict = get_cross_correlation_relationships(base_name, metric_names_with_ids=metric_names_with_ids)
                if cross_correlation_relationships_dict:
                    cross_correlation_relationships = cross_correlation_relationships_dict[base_name]['cross_correlation_relationships']
                    logger.info('related_metrics :: find_related :: %s cross_correlation_relationships for %s' % (
                        str(len(list(cross_correlation_relationships.keys()))), base_name))
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: related_metrics :: find_related :: get_cross_correlation_relationships failed for %s - %s' % (
                    base_name, str(err)))
            try:
                self.redis_conn_decoded.hset('luminosity.metric_group.last_updated', metric_id, int(find_related_start))
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: related_metrics :: find_related :: failed to update in metric_id: %s luminosity.metric_group.last_updated - %s' % (
                    str(metric_id), str(err)))
            metric_group_update = False
            if cross_correlation_relationships:
                metric_group_update = True
            if metric_group_info:
                if metric_group_info['related_metrics'] > 0:
                    metric_group_update = True
            if force_process:
                metric_group_update = True

            if metric_group_update:
                updated_metric_group = update_metric_group(base_name, metric_id, cross_correlation_relationships, ids_with_metric_names)
                if updated_metric_group:
                    logger.info('related_metrics :: find_related :: updated metric group for %s, updated_metric_group: %s' % (
                        base_name, str(update_metric_group)))
                    metric_groups_updated.append(base_name)
                # metrics_skipped_recently_checked.append(base_name)

        logger.info('related_metrics :: find_related :: %s metrics skipped as recently checked' % (
            str(len(metrics_skipped_recently_checked))))
        logger.info('related_metrics :: find_related :: %s metrics skipped as recently they have a recent anomaly' % (
            str(len(metrics_skipped_recent_anomaly))))
        logger.info('related_metrics :: find_related :: %s metrics skipped as they have no anomalies' % (
            str(len(metrics_skipped_no_anomaly))))

        find_related_end = time() - find_related_start
        logger.info('related_metrics :: find_related :: %s metrics checked and %s metric_groups were updated, analysed took %.2f seconds' % (
            str(len(metrics_checked)), str(len(metric_groups_updated)),
            find_related_end))

        # related_metrics table
        # id, source_metric_id, timestamp, full_duration, resolution, processed
        # related_metricss table
        # id, related_metrics_id, related_metric_id, ppscore_1, ppscore_2
        # Maybe do not just do ppscore maybe use ruptures to identify metrics
        # that have changespoints in the same window
        return

    def run(self):
        """
        - Called when the process intializes.

        - Determine if Redis is up

        - Spawn a process_metric process to do analysis

        - Wait for the process to finish.

        - run_every 300 seconds
        """

        # Log management to prevent overwriting
        # Allow the bin/<skyline_app>.d to manage the log
        now = time()
        log_wait_for = now + 5
        while now < log_wait_for:
            if os.path.isfile(skyline_app_loglock):
                sleep(.1)
                now = time()
            else:
                now = log_wait_for + 1

        logger.info('related_metrics :: starting')

        while 1:
            now = time()

            # Make sure Redis is up
            try:
                self.redis_conn.ping()
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: related_metrics cannot connect to redis at socket path %s - %s' % (
                    settings.REDIS_SOCKET_PATH, e))
                sleep(10)
                try:
                    self.redis_conn = get_redis_conn(skyline_app)
                    self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)
                except Exception as e:
                    logger.info(traceback.format_exc())
                    logger.error('error :: related_metrics cannot connect to get_redis_conn - %s' % e)
                continue

            # Report app up
            try:
                self.redis_conn.setex('luminosity.related_metrics', 120, now)
                logger.info('related_metrics :: set luminosity.related_metrics Redis key')
            except Exception as err:
                logger.error(traceback.format_exc())
                logger.error('error :: related_metrics :: could not update the Redis luminosity.related_metrics key - %s' % str(err))

            now_timestamp = int(time())

            # Spawn process
            pids = []
            spawned_pids = []
            pid_count = 0
            for i in range(1, 1 + 1):
                try:
                    p = Process(target=self.find_related, args=(i,))
                    pids.append(p)
                    pid_count += 1
                    logger.info('related_metrics starting %s of 1 find_related processes' % (str(pid_count)))
                    p.start()
                    spawned_pids.append(p.pid)
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: related_metrics :: failed to spawn find_related_metrics process - %s' % e)

            # Self monitor processes and terminate if any find_related
            # has run for longer than run_every - 10
            p_starts = time()
            while time() - p_starts <= (120 - 10):
                if any(p.is_alive() for p in pids):
                    # Just to avoid hogging the CPU
                    sleep(.1)
                else:
                    # All the processes are done, break now.
                    time_to_run = time() - p_starts
                    logger.info('related_metrics :: find_related process completed in %.2f seconds' % (
                        time_to_run))
                    break
            else:
                # We only enter this if we didn't 'break' above.
                logger.info('related_metrics :: timed out, killing find_related process')
                for p in pids:
                    logger.info('related_metrics :: killing find_related process')
                    p.terminate()
                    logger.info('related_metrics :: killed find_related process')

            for p in pids:
                if p.is_alive():
                    try:
                        logger.info('related_metrics :: stopping find_related - %s' % (str(p.is_alive())))
                        p.terminate()
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: related_metrics :: failed to stop find_related - %s' % e)

            run_every = 60
            process_runtime = time() - now
            if process_runtime < run_every:
                sleep_for = (run_every - process_runtime)

                process_runtime_now = time() - now
                sleep_for = (run_every - process_runtime_now)

                logger.info('related_metrics :: sleeping for %.2f seconds due to low run time...' % sleep_for)
                sleep(sleep_for)
                try:
                    del sleep_for
                except Exception as e:
                    logger.error('error :: related_metrics :: failed to del sleep_for - %s' % e)
            try:
                del process_runtime
            except Exception as e:
                logger.error('error :: related_metrics :: failed to del process_runtime - %s' % e)
