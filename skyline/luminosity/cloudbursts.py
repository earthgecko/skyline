import logging
from time import time, sleep
from threading import Thread
from multiprocessing import Process
import os
from os import kill, getpid
import traceback
from sqlalchemy.sql import select

import settings
from skyline_functions import (
    get_redis_conn, get_redis_conn_decoded)
from database import get_engine, cloudburst_table_meta
# from functions.database.queries.base_name_from_metric_id import base_name_from_metric_id
from functions.metrics.get_base_name_from_metric_id import get_base_name_from_metric_id

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

skyline_app_graphite_namespace = 'skyline.%s%s' % (skyline_app, SERVER_METRIC_PATH)

full_uniques = '%sunique_metrics' % settings.FULL_NAMESPACE
LOCAL_DEBUG = False

run_every = 30
current_path = os.path.dirname(__file__)
root_path = os.path.dirname(current_path)


# @added 20210805 - Feature #4164: luminosity - cloudbursts
class Cloudbursts(Thread):
    """
    The Cloudbursts class which controls the luminosity/cloudbursts thread and
    spawned processes. luminosity/cloudbursts finds metrics related to each
    cloudburst that have been identified by significant changepoints using the
    m66 algorithm.
    """

    def __init__(self, parent_pid):
        """
        Initialize Cloudbursts
        """
        super(Cloudbursts, self).__init__()
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
            exit(0)

    def find_related(self, i):
        """
        Find metrics that are related to cloudbursts using the ppscore
        correlations.
        """
        spin_start = time()
        logger.info('cloudbursts :: find_related :: process started')

        def get_an_engine():
            try:
                engine, log_msg, trace = get_engine(skyline_app)
                return engine, log_msg, trace
            except Exception as e:
                trace = traceback.format_exc()
                logger.error(trace)
                log_msg = 'error :: cloudbursts :: find_related :: failed to get MySQL engine - %s' % e
                logger.error('error :: cloudbursts :: find_related :: failed to get MySQL engine - %s' % e)
                return None, log_msg, trace

        def engine_disposal(engine):
            if engine:
                try:
                    engine.dispose()
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: cloudbursts :: find_related :: calling engine.dispose() - %s' % e)
            return

        last_processed_cloudburst_id = None
        last_processed_cloudburst_id_key = 'luminosity.cloudbursts.find_related.last_processed_cloudburst_id'
        try:
            last_processed_cloudburst_id = self.redis_conn_decoded.get(last_processed_cloudburst_id_key)
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: cloudbursts :: find_related :: failed to get id from %s Redis key - %s' % (
                last_processed_cloudburst_id_key, e))
            last_processed_cloudburst_id = None

        try:
            engine, log_msg, trace = get_an_engine()
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: cloudbursts :: find_related :: could not get a MySQL engine to update cloudburst table - %s' % e)
        if not engine:
            logger.error('error :: cloudbursts :: find_related :: engine not obtained to to update cloudburst table')

        cloudburst_table = None
        if engine:
            try:
                cloudburst_table, log_msg, trace = cloudburst_table_meta(skyline_app, engine)
                logger.info(log_msg)
                logger.info('cloudbursts :: find_related :: cloudburst_table OK')
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: failed to get cloudburst_table meta - %s' % e)
                cloudburst_table = None
                return

        new_cloudbursts = {}
        one_hour_ago = int(time()) - 3600
        try:
            connection = engine.connect()
            if last_processed_cloudburst_id:
                stmt = select([cloudburst_table]).\
                    where(cloudburst_table.c.id > int(last_processed_cloudburst_id))
            else:
                # stmt = select([cloudburst_table]).\
                #     where(cloudburst_table.c.timestamp >= one_hour_ago)
                stmt = select([cloudburst_table]).\
                    where(cloudburst_table.c.added_at >= one_hour_ago)
            result = connection.execute(stmt)
            for row in result:
                cloudburst_id = row['id']
                new_cloudbursts[cloudburst_id] = {}
                new_cloudbursts[cloudburst_id]['id'] = cloudburst_id
                new_cloudbursts[cloudburst_id]['metric_id'] = row['metric_id']
                new_cloudbursts[cloudburst_id]['timestamp'] = row['timestamp']
                new_cloudbursts[cloudburst_id]['end'] = row['end']
                new_cloudbursts[cloudburst_id]['from_timestamp'] = row['from_timestamp']
                new_cloudbursts[cloudburst_id]['duration'] = row['duration']
                new_cloudbursts[cloudburst_id]['resolution'] = row['resolution']
                new_cloudbursts[cloudburst_id]['added_at'] = row['added_at']
            connection.close()
        except Exception as e:
            logger.error(traceback.format_exc())
            logger.error('error :: cloudbursts :: find_related :: could not determine new cloudbursts from cloudburst table - %s' % e)

        logger.info('cloudbursts :: find_related :: %s cloudbursts to check' % str(len(new_cloudbursts)))

        # Consolidate by from_timestamp so that the data is pulled from Graphite
        # once
        sorted_from_timestamps = []
        if len(new_cloudbursts) > 0:
            from_timestamps_dict = {}
            for cloudburst_id in list(new_cloudbursts.keys()):
                from_timestamp = new_cloudbursts[cloudburst_id]['from_timestamp']
                try:
                    from_timestamps_dict[from_timestamp][cloudburst_id] = from_timestamp
                except KeyError:
                    from_timestamps_dict[from_timestamp] = {}
                    from_timestamps_dict[from_timestamp][cloudburst_id] = from_timestamp
                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error('error :: cloudbursts :: find_related :: find to consolidate new cloudburst to from_timestamps_dict - %s' % e)
            from_timestamps = list(from_timestamps_dict.keys())
            sorted_from_timestamps = sorted(from_timestamps)

        pp_relationships = {}
        for working_from_timestamp in sorted_from_timestamps:
            source_metrics = []
            for cloudburst_id in list(new_cloudbursts.keys()):
                cloudburst_from_timestamp = new_cloudbursts[cloudburst_id]['from_timestamp']
                if cloudburst_from_timestamp == working_from_timestamp:
                    metric_id = new_cloudbursts[cloudburst_id]['metric_id']
                    # base_name = base_name_from_metric_id(skyline_app, metric_id, False)
                    base_name = get_base_name_from_metric_id(skyline_app, metric_id, False)
                    source_metrics.append(base_name)
            continue

            #### FROM HERE ON
            for source_metric in source_metrics:
                source_metrics_timeseries = {}
                source_metrics_timeseries[source_metric] = {}
                timeseries = metrics_timeseries[source_metric]['timeseries']
                timeseries = [item for item in timeseries if int(item[0]) > (until_timestamp - 86400)]
                # source_metrics_timeseries[source_metric]['timeseries'] = metrics_timeseries[source_metric]['timeseries']
                source_metrics_timeseries[source_metric]['timeseries'] = timeseries
                source_metrics_timeseries[source_metric]['length'] = len(timeseries)
                timer_start = timer()
                related_metrics = {}
                all_ppscores = {}
                all_pps = {}
                all_metrics = list(m66_candidate_metrics.keys())
                for base_name in all_metrics:
                    if source_metric == base_name:
                        continue
                    metrics = [
                        source_metric,
                        base_name,
                    ]
                    current_metrics_timeseries = source_metrics_timeseries.copy()
                    timeseries = []
                    for r_base_name in metrics:
                        if r_base_name == source_metric:
                            continue
                        try:
                            timeseries = metrics_timeseries[r_base_name]['timeseries']
                        except KeyError:
                            timeseries = []
                        if timeseries:
                            timeseries = [item for item in timeseries if int(item[0]) > (until_timestamp - 86400)]
                            current_metrics_timeseries[r_base_name] = {}
                            current_metrics_timeseries[r_base_name]['timeseries'] = timeseries
                    if not timeseries:
                        continue
                    longest_timeseries = max(enumerate(current_metrics_timeseries), key=lambda tup: len(tup[1]))[1]
                    shortest_timeseries = min(enumerate(current_metrics_timeseries), key=lambda tup: len(tup[1]))[1]
                    longest_timeseries_timestamps = [ts for ts, value in current_metrics_timeseries[longest_timeseries]['timeseries']]
                    if longest_timeseries == shortest_timeseries:
                        for r_base_name in metrics:
                            if r_base_name != longest_timeseries:
                                shortest_timeseries = r_base_name
                    source_longest = False
                    if longest_timeseries == source_metric:
                        source_longest = True
                    x = []
                    y = []
                    for ts, value in current_metrics_timeseries[shortest_timeseries]['timeseries']:
                        if ts in longest_timeseries_timestamps:
                            x_value_list = [item[1] for item in current_metrics_timeseries[longest_timeseries]['timeseries'] if item[0] == ts]
                            x_value = x_value_list[0]
                            if x_value:
                                x.append(x_value)
                                y.append(value)
                            # TODO only use if fully populated?
                    df = pd.DataFrame()
                    x_column = metrics[0]
                    y_column = metrics[1]
                    if source_longest:
                        df[x_column] = x
                        df[y_column] = y
                    else:
                        df[x_column] = y
                        df[y_column] = x
                    # pps.score(df, x_column, y_column)
                    ppscores_df = pps.matrix(df)
                    ppscores = ppscores_df['ppscore'].tolist()
                    all_ppscores[base_name] = {}
                    all_ppscores[base_name]['ppscores'] = ppscores
                    all_pps[base_name] = ppscores_df.to_dict()
                    if ppscores[1] > 0.5 or ppscores[2] > 0.5:
                        related_metrics[base_name] = ppscores_df.to_dict()
                        source_metric_dict_exists = {}
                        try:
                            source_metric_dict_exists = pp_relationships[source_metric]
                        except KeyError:
                            source_metric_dict_exists = {}
                        except Exception as e:
                            source_metric_dict_exists = {}
                            print(e)
                        if not source_metric_dict_exists:
                            pp_relationships[source_metric] = {}
                        pp_relationships[source_metric][base_name] = ppscores_df.to_dict()

                timer_end = timer()
                print('%s metrics took %.6f seconds, found %s potentially related metrics to %s' % (
                    str(len(all_metrics)), (timer_end - timer_start), str(len(related_metrics)), source_metric))
                related_metrics_summary = {}
                for base_name in list(related_metrics):
                    scores = [related_metrics[base_name]['ppscore'][1], related_metrics[base_name]['ppscore'][2]]
                    related_metrics_summary[base_name] = scores
                # related_metrics_summary
                unsorted_related_metrics = []
                for base_name in list(related_metrics):
                    scores = [related_metrics[base_name]['ppscore'][1], related_metrics[base_name]['ppscore'][2]]
                    ppscores_sum = sum(scores)
                    unsorted_related_metrics.append([base_name, ppscores_sum])
                sorted_related_metrics = sorted(unsorted_related_metrics, key=lambda x: x[1], reverse=True)
                source_df = timeseries_to_datetime_indexed_df(skyline_app, metrics_timeseries[source_metric]['timeseries'], False)
                for base_name, ppscores in sorted_related_metrics:
                    current_metrics_timeseries = source_metrics_timeseries.copy()
                    x_timeseries = []
                    x = []
                    y_timeseries = []
                    y = []
                    metrics = [
                        source_metric,
                        base_name,
                    ]
                    for r_base_name in metrics:
                        timeseries = []
                        try:
                            timeseries = metrics_timeseries[r_base_name]['timeseries']
                        except KeyError:
                            continue
                        if timeseries:
                            if r_base_name != source_metric:
                                timeseries = [item for item in timeseries if int(item[0]) > (until_timestamp - 86400)]
                                current_metrics_timeseries[r_base_name] = {}
                                current_metrics_timeseries[r_base_name]['timeseries'] = timeseries
                    longest_timeseries = max(enumerate(current_metrics_timeseries), key=lambda tup: len(tup[1]))[1]
                    shortest_timeseries = min(enumerate(current_metrics_timeseries), key=lambda tup: len(tup[1]))[1]
                    if longest_timeseries == shortest_timeseries:
                        for r_base_name in metrics:
                            if r_base_name != longest_timeseries:
                                shortest_timeseries = r_base_name
                    longest_timeseries_timestamps = [ts for ts, value in current_metrics_timeseries[longest_timeseries]['timeseries']]
                    y = []
                    for ts, value in current_metrics_timeseries[shortest_timeseries]['timeseries']:
                        if ts in longest_timeseries_timestamps:
                            x_value_list = [item[1] for item in current_metrics_timeseries[longest_timeseries]['timeseries'] if item[0] == ts]
                            x_value = x_value_list[0]
                            if x_value:
                                x.append(x_value)
                                x_timeseries.append([ts, x_value])
                                y.append(value)
                                y_timeseries.append([ts, value])

                        # TODO only use if fully populated?
                    # final_df = source_df.copy()
                    if longest_timeseries == source_metric:
                        final_df = timeseries_to_datetime_indexed_df(skyline_app, x_timeseries, False)
                        base_name_df = timeseries_to_datetime_indexed_df(skyline_app, y_timeseries, False)
                    else:
                        final_df = timeseries_to_datetime_indexed_df(skyline_app, y_timeseries, False)
                        base_name_df = timeseries_to_datetime_indexed_df(skyline_app, x_timeseries, False)
                    final_df.rename(columns={'value': source_metric}, inplace=True)
                    base_name_df.rename(columns={'value': base_name}, inplace=True)
                    base_name_values = base_name_df[base_name]
                    final_df = final_df.join(base_name_values)

                #    final_df = final_df.resample('10T').median()
                    ppscores_sum = sum(related_metrics_summary[base_name])
                    title = 'source metric: %s\nrelated changepoint: %s\nppscore (sum): %s' % (source_metric, base_name, str(ppscores_sum))
                    ax = final_df.plot(figsize=(12, 6), secondary_y=[base_name], title=title)
                    ax.lines[0].set_alpha(0.3)

        spin_end = time() - spin_start
        logger.info('cloudbursts :: find_related :: would find_related for %s cloudburst - took %.2f seconds' % (
            str(len(new_cloudbursts)), spin_end))

        if engine:
            engine_disposal(engine)

        # cloudburst table
        # id, source_metric_id, timestamp, full_duration, resolution, processed
        # cloudbursts table
        # id, cloudburst_id, related_metric_id, ppscore_1, ppscore_2
        # Maybe do not just do ppscore maybe use ruptures to identify metrics
        # that have changespoints in the same window
        return

    def run(self):
        """
        - Called when the process intializes.

        - Determine if Redis is up

        - Determine new cloudbursts to find related metrics on

        - Wait for the process to finish.

        - run_every 30 seconds
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

        logger.info('luminosity/cloudbursts :: starting')

        try:
            SERVER_METRIC_PATH = '.%s' % settings.SERVER_METRICS_NAME
            if SERVER_METRIC_PATH == '.':
                SERVER_METRIC_PATH = ''
        except Exception as e:
            SERVER_METRIC_PATH = ''
            logger.warn('warning :: luminosity/cloudbursts :: settings.SERVER_METRICS_NAME is not declared in settings.py, defaults to \'\' - %s' % e)

        while 1:
            now = time()

            # Make sure Redis is up
            try:
                self.redis_conn.ping()
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: cloudbursts cannot connect to redis at socket path %s - %s' % (
                    settings.REDIS_SOCKET_PATH, e))
                sleep(10)
                try:
                    self.redis_conn = get_redis_conn(skyline_app)
                    self.redis_conn_decoded = get_redis_conn_decoded(skyline_app)
                except Exception as e:
                    logger.info(traceback.format_exc())
                    logger.error('error :: cloudbursts cannot connect to get_redis_conn - %s' % e)
                continue

            # Report app up
            try:
                self.redis_conn.setex('luminosity.cloudbursts', 120, now)
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: cloudbursts :: could not update the Redis luminosity.cloudbursts key - %s' % e)

            # Spawn process
            pids = []
            spawned_pids = []
            pid_count = 0
            try:
                p = Process(target=self.find_related, args=(1,))
                pids.append(p)
                pid_count += 1
                logger.info('cloudbursts :: starting find_related process')
                p.start()
                spawned_pids.append(p.pid)
            except Exception as e:
                logger.error(traceback.format_exc())
                logger.error('error :: cloudbursts :: failed to spawn find_related process - %s' % e)

            # Self monitor processes and terminate if any find_cloudbursts
            # has run for longer than run_every - 10
            p_starts = time()
            while time() - p_starts <= 3600:
                if any(p.is_alive() for p in pids):
                    # Just to avoid hogging the CPU
                    sleep(.1)
                else:
                    # All the processes are done, break now.
                    time_to_run = time() - p_starts
                    logger.info('cloudbursts :: find_related process completed in %.2f seconds' % (
                        time_to_run))
                    break
            else:
                # We only enter this if we didn't 'break' above.
                logger.info('cloudbursts :: timed out, killing find_related process')
                for p in pids:
                    logger.info('cloudbursts :: killing find_related process')
                    p.terminate()
                    logger.info('cloudbursts :: killed find_related process')

            for p in pids:
                if p.is_alive():
                    try:
                        logger.info('cloudbursts :: stopping find_related - %s' % (str(p.is_alive())))
                        p.terminate()
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        logger.error('error :: cloudbursts :: failed to stop find_related - %s' % e)

            process_runtime = time() - now
            if process_runtime < run_every:
                sleep_for = (run_every - process_runtime)

                process_runtime_now = time() - now
                sleep_for = (run_every - process_runtime_now)

                logger.info('cloudbursts :: sleeping for %.2f seconds due to low run time...' % sleep_for)
                sleep(sleep_for)
                try:
                    del sleep_for
                except Exception as e:
                    logger.error('error :: cloudbursts :: failed to del sleep_for - %s' % e)
            try:
                del process_runtime
            except Exception as e:
                logger.error('error :: cloudbursts :: failed to del process_runtime - %s' % e)
