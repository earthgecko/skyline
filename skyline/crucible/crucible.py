import logging
try:
    from Queue import Empty
except:
    from queue import Empty
from redis import StrictRedis
import time
from time import time, sleep
from threading import Thread
from multiprocessing import Process, Manager, Queue
from msgpack import Unpacker, unpackb, packb
import os
from os.path import dirname, join, abspath, isfile
from os import path, kill, getpid, system, getcwd, listdir, makedirs
from sys import exit, version_info
import traceback
import re
import socket
import json
import gzip
import sys
import requests
try:
    import urlparse
except ImportError:
    import urllib.parse
try:
    import urllib2
except ImportError:
    import urllib.request
    import urllib.error
import errno
import imp
import datetime
import shutil

import os.path
# sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
# sys.path.insert(0, os.path.dirname(__file__))

import settings
from skyline_functions import mkdir_p, load_metric_vars, fail_check

from crucible_algorithms import run_algorithms

skyline_app = 'crucible'
skyline_app_logger = skyline_app + 'Log'
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = settings.LOG_PATH + '/' + skyline_app + '.log'
skyline_app_loglock = skyline_app_logfile + '.lock'
skyline_app_logwait = skyline_app_logfile + '.wait'

python_version = int(version_info[0])

this_host = str(os.uname()[1])

try:
    SERVER_METRIC_PATH = '.' + settings.SERVER_METRICS_NAME
    if SERVER_METRIC_PATH == '.':
        SERVER_METRIC_PATH = ''
except:
    SERVER_METRIC_PATH = ''

skyline_app_graphite_namespace = 'skyline.' + skyline_app + SERVER_METRIC_PATH

FULL_NAMESPACE = settings.FULL_NAMESPACE
ENABLE_CRUCIBLE_DEBUG = settings.ENABLE_CRUCIBLE_DEBUG
crucible_data_folder = str(settings.CRUCIBLE_DATA_FOLDER)
failed_checks_dir = crucible_data_folder + '/failed_checks'


class Crucible(Thread):
    def __init__(self, parent_pid):
        """
        Initialize Crucible
        """
        super(Crucible, self).__init__()
        self.daemon = True
        self.parent_pid = parent_pid
        self.current_pid = getpid()
        self.process_list = Manager().list()
        self.metric_variables = Manager().list()
        self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)

    def check_if_parent_is_alive(self):
        """
        Check if the parent process is alive
        """
        try:
            kill(self.current_pid, 0)
            kill(self.parent_pid, 0)
        except:
            exit(0)

    def spin_process(self, i, run_timestamp, metric_check_file):
        """
        Assign a metric for a process to analyze.

        :param i: python process id
        :param run_timestamp: the epoch timestamp at which this process was called
        :param metric_check_file: full path to the metric check file

        :return: returns True

        """

        child_process_pid = os.getpid()
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('child_process_pid - %s' % str(child_process_pid))

        self.process_list.append(child_process_pid)

        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('processing metric check - %s' % metric_check_file)

        if not os.path.isfile(str(metric_check_file)):
            logger.error('error :: file not found - metric_check_file - %s' % (str(metric_check_file)))
            return

        check_file_name = os.path.basename(str(metric_check_file))
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('check_file_name - %s' % check_file_name)
        check_file_timestamp = check_file_name.split('.', 1)[0]
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('check_file_timestamp - %s' % str(check_file_timestamp))
        check_file_metricname_txt = check_file_name.split('.', 1)[1]
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('check_file_metricname_txt - %s' % check_file_metricname_txt)
        check_file_metricname = check_file_metricname_txt.replace('.txt', '')
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('check_file_metricname - %s' % check_file_metricname)
        check_file_metricname_dir = check_file_metricname.replace('.', '/')
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('check_file_metricname_dir - %s' % check_file_metricname_dir)

        metric_failed_check_dir = failed_checks_dir + '/' + check_file_metricname_dir + '/' + check_file_timestamp
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('metric_failed_check_dir - %s' % metric_failed_check_dir)

        # failed_check_file = failed_checks_dir + '/' + check_file_name
        failed_check_file = metric_failed_check_dir + '/' + check_file_name

        # Load and validate metric variables
        try:
            metric_vars = load_metric_vars(skyline_app, str(metric_check_file))
        except:
            logger.error('error :: failed to import metric variables from check file -  %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            # TBD - a failed check Panorama update will go here, perhaps txt
            #       files are not the only "queue" that will be used, both, but
            #       Panorama, may be just a part of Skyline Flux, the flux DB
            #       would allow for a very nice, distributed "queue" and a
            #       distributed Skyline workforce...
            #       Any Skyline node could just have one role, e.g. lots of
            #       Skyline nodes running crucible only and instead of reading
            #       the local filesystem for input, they could read the Flux DB
            #       queue or both...
            return

        # Test metric variables
        # We use a pythonic methodology to test if the variables are defined,
        # this ensures that if any of the variables are not set for some reason
        # we can handle unexpected data or situations gracefully and try and
        # ensure that the process does not hang.

        # if len(str(metric_vars.metric)) == 0:
        # if not metric_vars.metric:
        try:
            metric_vars.metric
        except:
            logger.error('error :: failed to read metric variable from check file -  %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return
        else:
            metric = str(metric_vars.metric)
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('metric variable - metric - %s' % metric)

        # if len(metric_vars.value) == 0:
        # if not metric_vars.value:
        try:
            metric_vars.value
        except:
            logger.error('error :: failed to read value variable from check file -  %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return
        else:
            value = str(metric_vars.value)
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('metric variable - value - %s' % (value))

        # if len(metric_vars.from_timestamp) == 0:
        # if not metric_vars.from_timestamp:
        try:
            metric_vars.from_timestamp
        except:
            logger.error('error :: failed to read from_timestamp variable from check file -  %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return
        else:
            from_timestamp = str(metric_vars.from_timestamp)
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('metric variable - from_timestamp - %s' % from_timestamp)

        # if len(metric_vars.metric_timestamp) == 0:
        # if not metric_vars.metric_timestamp:
        try:
            metric_vars.metric_timestamp
        except:
            logger.error('error :: failed to read metric_timestamp variable from check file -  %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return
        else:
            metric_timestamp = str(metric_vars.metric_timestamp)
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('metric variable - metric_timestamp - %s' % metric_timestamp)

        # if len(metric_vars.algorithms) == 0:
        # if not metric_vars.algorithms:
        try:
            metric_vars.algorithms
        except:
            logger.error('error :: failed to read algorithms variable from check file setting to all' % (metric_check_file))
            algorithms = ['all']
        else:
            algorithms = []
            for i_algorithm in metric_vars.algorithms:
                algorithms.append(i_algorithm)
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('metric variable - algorithms - %s' % algorithms)

        # if len(metric_vars.anomaly_dir) == 0:
        # if not metric_vars.anomaly_dir:
        try:
            metric_vars.anomaly_dir
        except:
            logger.error('error :: failed to read anomaly_dir variable from check file -  %s' % (metric_check_file))
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return
        else:
            anomaly_dir = str(metric_vars.anomaly_dir)
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('metric variable - anomaly_dir - %s' % anomaly_dir)

        # if len(str(metric_vars.graphite_metric)) == 0:
        try:
            metric_vars.graphite_metric
        except:
            logger.info('failed to read graphite_metric variable from check file setting to False')
            # yes this is a string
            graphite_metric = 'False'
        else:
            graphite_metric = str(metric_vars.graphite_metric)
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('metric variable - graphite_metric - %s' % graphite_metric)

        # if len(str(metric_vars.run_crucible_tests)) == 0:
        try:
            metric_vars.run_crucible_tests
        except:
            logger.info('failed to read run_crucible_tests variable from check file setting to False')
            # yes this is a string
            run_crucible_tests = 'False'
        else:
            run_crucible_tests = str(metric_vars.run_crucible_tests)
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('metric variable - run_crucible_tests - %s' % run_crucible_tests)

        try:
            metric_vars.added_by
        except:
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('failed to read added_by variable from check file setting to crucible - set to crucible')
            added_by = 'crucible'
        else:
            added_by = str(metric_vars.added_by)
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('metric variable - added_by - %s' % added_by)

        try:
            metric_vars.run_script
        except:
            run_script = False
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('metric variable - run_script - not present set to False')
        else:
            run_script = str(metric_vars.run_script)
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('metric variable - run_script - %s' % run_script)

        # Only check if the metric does not a EXPIRATION_TIME key set, crucible
        # uses the alert EXPIRATION_TIME for the relevant alert setting contexts
        # whether that be analyzer, mirage, boundary, etc and sets its own
        # cache_keys in redis.  This prevents large amounts of data being added
        # in terms of tieseries json and image files, crucible samples at the
        # same EXPIRATION_TIME as alerts.

        source_app = 'crucible'
        expiration_timeout = 1800
        remove_all_anomaly_files = False
        check_expired = False
        check_time = time()

        if added_by == 'analyzer' or added_by == 'mirage':
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('Will check %s ALERTS' % added_by)
            if settings.ENABLE_ALERTS:
                if settings.ENABLE_CRUCIBLE_DEBUG:
                    logger.info('Checking %s ALERTS' % added_by)
                for alert in settings.ALERTS:
                    ALERT_MATCH_PATTERN = alert[0]
                    METRIC_PATTERN = metric
                    alert_match_pattern = re.compile(ALERT_MATCH_PATTERN)
                    pattern_match = alert_match_pattern.match(METRIC_PATTERN)
                    if pattern_match:
                        source_app = added_by
                        expiration_timeout = alert[2]
                        if settings.ENABLE_CRUCIBLE_DEBUG:
                            logger.info('matched - %s - %s - EXPIRATION_TIME is %s' % (source_app, metric, str(expiration_timeout)))
                        check_age = int(check_time) - int(metric_timestamp)
                        if int(check_age) > int(expiration_timeout):
                            check_expired = True
                            if settings.ENABLE_CRUCIBLE_DEBUG:
                                logger.info('the check is older than EXPIRATION_TIME for the metric - not checking - check_expired')

        if added_by == 'boundary':
            if settings.BOUNDARY_ENABLE_ALERTS:
                for alert in settings.BOUNDARY_METRICS:
                    ALERT_MATCH_PATTERN = alert[0]
                    METRIC_PATTERN = metric
                    alert_match_pattern = re.compile(ALERT_MATCH_PATTERN)
                    pattern_match = alert_match_pattern.match(METRIC_PATTERN)
                    if pattern_match:
                        source_app = 'boundary'
                        expiration_timeout = alert[2]
                        if settings.ENABLE_CRUCIBLE_DEBUG:
                            logger.info('matched - %s - %s - EXPIRATION_TIME is %s' % (source_app, metric, str(expiration_timeout)))
                        check_age = int(check_time) - int(metric_timestamp)
                        if int(check_age) > int(expiration_timeout):
                            check_expired = True
                            if settings.ENABLE_CRUCIBLE_DEBUG:
                                logger.info('the check is older than EXPIRATION_TIME for the metric - not checking - check_expired')

        cache_key = 'crucible.last_check.%s.%s' % (source_app, metric)
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('cache_key - crucible.last_check.%s.%s' % (source_app, metric))

        # Only use the cache_key EXPIRATION_TIME if this is not a request to
        # run_crucible_tests on a timeseries
        if run_crucible_tests == 'False':
            if check_expired:
                logger.info('check_expired - not checking Redis key')
                last_check = True
            else:
                if settings.ENABLE_CRUCIBLE_DEBUG:
                    logger.info('Checking if cache_key exists')
                try:
                    last_check = self.redis_conn.get(cache_key)
                except Exception as e:
                    logger.error('error :: could not query cache_key for %s - %s - %s' % (alerter, metric, e))
                    logger.info('all anomaly files will be removed')
                    remove_all_anomaly_files = True

            if not last_check:
                try:
                    self.redis_conn.setex(cache_key, expiration_timeout, packb(value))
                    logger.info('set cache_key for %s - %s with timeout of %s' % (source_app, metric, str(expiration_timeout)))
                except Exception as e:
                    logger.error('error :: could not query cache_key for %s - %s - %s' % (alerter, metric, e))
                    logger.info('all anomaly files will be removed')
                    remove_all_anomaly_files = True
            else:
                if check_expired:
                    logger.info('check_expired - all anomaly files will be removed')
                    remove_all_anomaly_files = True
                else:
                    logger.info('cache_key is set and not expired for %s - %s - all anomaly files will be removed' % (source_app, metric))
                    remove_all_anomaly_files = True

        # anomaly dir
        if not os.path.exists(str(anomaly_dir)):
            try:
                # mkdir_p(skyline_app, str(anomaly_dir))
                if python_version == 2:
                    mode_arg = int('0755')
                if python_version == 3:
                    mode_arg = mode=0o755
                os.makedirs(anomaly_dir, mode_arg)
                if settings.ENABLE_CRUCIBLE_DEBUG:
                    logger.info('created anomaly dir - %s' % str(anomaly_dir))
            except:
                logger.error('error :: failed to create anomaly_dir - %s' % str(anomaly_dir))

        if not os.path.exists(str(anomaly_dir)):
            logger.error('error :: anomaly_dir does not exist')
            fail_check(skyline_app, metric_failed_check_dir, str(metric_check_file))
            return
        else:
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('anomaly dir exists - %s' % str(anomaly_dir))

        failed_check_file = anomaly_dir + '/' + metric_timestamp + '.failed.check.' + metric + '.txt'
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('failed_check_file - %s' % str(failed_check_file))

        # Retrieve data from graphite is necessary
        anomaly_graph = anomaly_dir + '/' + metric + '.png'
        anomaly_json = anomaly_dir + '/' + metric + '.json'
        anomaly_json_gz = anomaly_json + '.gz'
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('anomaly_graph - %s' % str(anomaly_graph))
            logger.info('anomaly_json - %s' % str(anomaly_json))
            logger.info('anomaly_json_gz - %s' % str(anomaly_json_gz))

        # Some things added to crucible may not be added by a skyline app per se
        # and if run_crucible_tests is string True then no anomaly files should
        # be removed.
        if run_crucible_tests == 'True':
            remove_all_anomaly_files = False

        # Remove check and anomaly files if the metric has a EXPIRATION_TIME
        # cache_key set
        if remove_all_anomaly_files:
            if os.path.isfile(anomaly_graph):
                try:
                    os.remove(anomaly_graph)
                    if settings.ENABLE_CRUCIBLE_DEBUG:
                        logger.info('anomaly_graph removed - %s' % str(anomaly_graph))
                except OSError:
                    pass
            if os.path.isfile(anomaly_json):
                try:
                    os.remove(anomaly_json)
                    if settings.ENABLE_CRUCIBLE_DEBUG:
                        logger.info('anomaly_json removed - %s' % str(anomaly_json))
                except OSError:
                    pass
            if os.path.isfile(anomaly_json_gz):
                try:
                    os.remove(anomaly_json_gz)
                    if settings.ENABLE_CRUCIBLE_DEBUG:
                        logger.info('anomaly_json_gz removed - %s' % str(anomaly_json_gz))
                except OSError:
                    pass

            anomaly_txt_file = anomaly_dir + '/' + metric + '.txt'
            if os.path.isfile(anomaly_txt_file):
                try:
                    os.remove(anomaly_txt_file)
                    if settings.ENABLE_CRUCIBLE_DEBUG:
                        logger.info('anomaly_txt_file removed - %s' % str(anomaly_txt_file))
                except OSError:
                    pass

            # TBD - this data would have to be added to the panaorama DB before
            #       it is removed
            if os.path.isfile(str(metric_check_file)):
                try:
                    os.remove(str(metric_check_file))
                    if settings.ENABLE_CRUCIBLE_DEBUG:
                        logger.info('metric_check_file removed - %s' % str(metric_check_file))
                except OSError:
                    pass

            if os.path.exists(str(anomaly_dir)):
                try:
                    os.rmdir(str(anomaly_dir))
                    if settings.ENABLE_CRUCIBLE_DEBUG:
                        logger.info('anomaly_dir removed - %s' % str(anomaly_dir))
                except OSError:
                    pass

            logger.info('check and anomaly files removed')
            return

        # Check if the image exists
        if graphite_metric == 'True':

            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('graphite_metric - %s' % (graphite_metric))

            # Graphite timeouts
            connect_timeout = int(settings.GRAPHITE_CONNECT_TIMEOUT)
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('connect_timeout - %s' % str(connect_timeout))

            read_timeout = int(settings.GRAPHITE_READ_TIMEOUT)
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('read_timeout - %s' % str(read_timeout))

            graphite_until = datetime.datetime.fromtimestamp(int(metric_timestamp)).strftime('%H:%M_%Y%m%d')
            graphite_from = datetime.datetime.fromtimestamp(int(from_timestamp)).strftime('%H:%M_%Y%m%d')

            # graphite URL
            if settings.GRAPHITE_PORT != '':
                url = settings.GRAPHITE_PROTOCOL + '://' + settings.GRAPHITE_HOST + ':' + settings.GRAPHITE_PORT + '/render/?from=' + graphite_from + '&until=' + graphite_until + '&target=' + metric + '&format=json'
            else:
                url = settings.GRAPHITE_PROTOCOL + '://' + settings.GRAPHITE_HOST + '/render/?from=' + graphite_from + '&until=' + graphite_until + '&target=' + metric + '&format=json'
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('graphite url - %s' % (url))

            if not os.path.isfile(anomaly_graph):
                logger.info('retrieving png - surfacing %s graph from graphite from %s to %s' % (metric, graphite_from, graphite_until))

                image_url = url.replace('&format=json', '')
                graphite_image_file = anomaly_dir + '/' + metric + '.png'
                if 'width' not in image_url:
                    image_url += '&width=586'
                if 'height' not in image_url:
                    image_url += '&height=308'
                if settings.ENABLE_CRUCIBLE_DEBUG:
                    logger.info('graphite image url - %s' % (image_url))
                image_url_timeout = int(connect_timeout)

                image_data = None

                try:
                    image_data = urllib2.urlopen(image_url, timeout=image_url_timeout).read()
                    logger.info('url OK - %s' % (image_url))
                except urllib2.URLError:
                    image_data = None
                    logger.error('error :: url bad - %s' % (image_url))

                if image_data is not None:
                    with open(graphite_image_file, 'w') as f:
                        f.write(image_data)
                    logger.info('retrieved - %s' % (anomaly_graph))
                    if python_version == 2:
                        mode_arg = int('0644')
                    if python_version == 3:
                        mode_arg = '0o644'
                    os.chmod(graphite_image_file, mode_arg)
                else:
                    logger.error('error :: failed to retrieved - %s' % (anomaly_graph))
            else:
                if settings.ENABLE_CRUCIBLE_DEBUG:
                    logger.info('anomaly_graph file exists - %s' % str(anomaly_graph))

            if not os.path.isfile(anomaly_graph):
                logger.error('error :: retrieve failed to surface %s graph from graphite' % (metric))
            else:
                logger.info('graph image exists - %s' % (anomaly_graph))

            # Check if the json exists
            if not os.path.isfile(anomaly_json_gz):
                if not os.path.isfile(anomaly_json):
                    logger.info('surfacing timeseries data for %s from graphite from %s to %s' % (metric, graphite_from, graphite_until))
                    if requests.__version__ >= '2.4.0':
                        use_timeout = (int(connect_timeout), int(read_timeout))
                    else:
                        use_timeout = int(connect_timeout)
                    if settings.ENABLE_CRUCIBLE_DEBUG:
                        logger.info('use_timeout - %s' % (str(use_timeout)))

                    try:
                        r = requests.get(url, timeout=use_timeout)
                        js = r.json()
                        datapoints = js[0]['datapoints']
                        if settings.ENABLE_CRUCIBLE_DEBUG:
                            logger.info('data retrieved OK')
                    except:
                        datapoints = [[None, int(graphite_until)]]
                        logger.error('error :: data retrieval failed')

                    converted = []
                    for datapoint in datapoints:
                        try:
                            new_datapoint = [float(datapoint[1]), float(datapoint[0])]
                            converted.append(new_datapoint)
                        except:
                            continue

                    with open(anomaly_json, 'w') as f:
                        f.write(json.dumps(converted))
                    if python_version == 2:
                        mode_arg = int('0644')
                    if python_version == 3:
                        mode_arg = '0o644'
                    os.chmod(anomaly_json, mode_arg)
                    if settings.ENABLE_CRUCIBLE_DEBUG:
                        logger.info('json file - %s' % anomaly_json)

                if not os.path.isfile(anomaly_json):
                    logger.error('error :: failed to surface %s json from graphite' % (metric))
                    # Move metric check file
                    try:
                        shutil.move(metric_check_file, failed_check_file)
                        logger.info('moved check file to - %s' % failed_check_file)
                    except OSError:
                        logger.error('error :: failed to move check file to - %s' % failed_check_file)
                        pass
                    return

        # Check timeseries json exists - raw or gz
        if not os.path.isfile(anomaly_json):
            if not os.path.isfile(anomaly_json_gz):
                logger.error('error :: no json data found' % (metric))
                # Move metric check file
                try:
                    shutil.move(metric_check_file, failed_check_file)
                    logger.info('moved check file to - %s' % failed_check_file)
                except OSError:
                    logger.error('error :: failed to move check file to - %s' % failed_check_file)
                    pass
                return
            else:
                logger.info('timeseries json gzip exists - %s' % (anomaly_json_gz))
        else:
            logger.info('timeseries json exists - %s' % (anomaly_json))

        # If timeseries json and run_crucible_tests is str(False) gzip and
        # return here as there is nothing further to do
        if run_crucible_tests == 'False':
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('run_crucible_tests - %s' % run_crucible_tests)
            # gzip the json timeseries data
            if os.path.isfile(anomaly_json):
                if settings.ENABLE_CRUCIBLE_DEBUG:
                    logger.info('gzipping - %s' % anomaly_json)
                try:
                    f_in = open(anomaly_json)
                    f_out = gzip.open(anomaly_json_gz, 'wb')
                    f_out.writelines(f_in)
                    f_out.close()
                    f_in.close()
                    os.remove(anomaly_json)
                    if python_version == 2:
                        mode_arg = int('0644')
                    if python_version == 3:
                        mode_arg = '0o644'
                    os.chmod(anomaly_json_gz, mode_arg)
                    if settings.ENABLE_CRUCIBLE_DEBUG:
                        logger.info('gzipped - %s' % anomaly_json_gz)
                    try:
                        os.remove(metric_check_file)
                        logger.info('removed check file - %s' % metric_check_file)
                    except OSError:
                        pass
                    return
                except:
                    logger.error('error :: Failed to gzip data file - %s' % str(traceback.print_exc()))
                    try:
                        os.remove(metric_check_file)
                        logger.info('removed check file - %s' % metric_check_file)
                    except OSError:
                        pass
                    return

            if os.path.isfile(anomaly_json_gz):
                if settings.ENABLE_CRUCIBLE_DEBUG:
                    logger.info('gzip exists - %s' % anomaly_json)
                    try:
                        os.remove(metric_check_file)
                        logger.info('removed check file - %s' % metric_check_file)
                    except OSError:
                        pass
                    return
            nothing_to_do = 'true - for debug only'

        # self.check_if_parent_is_alive()
        # Run crucible algorithms
        logger.info('running crucible tests - %s' % (metric))

        timeseries_dir = metric.replace('.', '/')

        if os.path.isfile(anomaly_json_gz):
            if not os.path.isfile(anomaly_json):
                if settings.ENABLE_CRUCIBLE_DEBUG:
                    logger.info('ungzipping - %s' % anomaly_json_gz)
                try:
                    # with gzip.open(anomaly_json_gz, 'rb') as fr:
                    fr = gzip.open(anomaly_json_gz, 'rb')
                    raw_timeseries = fr.read()
                    fr.close()
                except Exception as e:
                    logger.error('error :: could not ungzip %s - %s' % (anomaly_json_gz, e))
                    traceback.print_exc()
                if settings.ENABLE_CRUCIBLE_DEBUG:
                    logger.info('ungzipped')
                    logger.info('writing to - %s' % anomaly_json)
                with open(anomaly_json, 'w') as fw:
                    fw.write(raw_timeseries)
                if settings.ENABLE_CRUCIBLE_DEBUG:
                    logger.info('anomaly_json done')
                if python_version == 2:
                    mode_arg = int('0644')
                if python_version == 3:
                    mode_arg = '0o644'
                os.chmod(anomaly_json, mode_arg)
        else:
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('No gzip - %s' % anomaly_json_gz)
            nothing_to_do = 'true - for debug only'

        if os.path.isfile(anomaly_json):
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('anomaly_json exists - %s' % anomaly_json)

        if os.path.isfile(anomaly_json):
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('loading timeseries from - %s' % anomaly_json)
            with open(anomaly_json, 'r') as f:
                timeseries = json.loads(f.read())
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('loaded timeseries from - %s' % anomaly_json)
        else:
            try:
                logger.error('error :: file not found - %s' % anomaly_json)
                shutil.move(metric_check_file, failed_check_file)
                if python_version == 2:
                    mode_arg = int('0644')
                if python_version == 3:
                    mode_arg = '0o644'
                os.chmod(failed_check_file, mode_arg)
                logger.info('moved check file to - %s' % failed_check_file)
            except OSError:
                logger.error('error :: failed to move check file to - %s' % failed_check_file)
                pass
            return

        start_timestamp = int(timeseries[0][0])
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('start_timestamp - %s' % str(start_timestamp))
        end_timestamp = int(timeseries[-1][0])
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('end_timestamp - %s' % str(end_timestamp))

        full_duration = end_timestamp - start_timestamp
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('full_duration - %s' % str(full_duration))

        self.check_if_parent_is_alive()

        run_algorithms_start_timestamp = int(time())
        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('run_algorithms_start_timestamp - %s' % str(run_algorithms_start_timestamp))

        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('run_algorithms - %s,%s,%s,%s,%s,%s' % (metric, str(end_timestamp), str(full_duration), anomaly_json, skyline_app, str(algorithms)))
        try:
            anomalous, ensemble = run_algorithms(timeseries, str(metric), end_timestamp, full_duration, str(anomaly_json), skyline_app, algorithms)
        except:
            logger.error('error :: run_algorithms failed - %s' % str(traceback.print_exc()))

        run_algorithms_end_timestamp = int(time())
        run_algorithms_seconds = run_algorithms_end_timestamp - run_algorithms_start_timestamp

        if settings.ENABLE_CRUCIBLE_DEBUG:
            logger.info('anomalous, ensemble - %s, %s' % (anomalous, str(ensemble)))

        if anomalous:
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('anomalous - %s' % (anomalous))
            nothing_to_do = 'true - for debug only'

        logger.info('run_algorithms took %s seconds' % str(run_algorithms_seconds))

        # Update anomaly file
        crucible_data = 'crucible_tests_run = "%s"\n' \
                        'crucible_triggered_algorithms = %s\n' \
                        'tested_by = "%s"\n' \
            % (str(run_timestamp), str(ensemble), str(this_host))
        crucible_anomaly_file = '%s/%s.txt' % (anomaly_dir, metric)
        with open(crucible_anomaly_file, 'a') as fh:
            fh.write(crucible_data)
        if python_version == 2:
            mode_arg = int('0644')
        if python_version == 3:
            mode_arg = '0o644'
        os.chmod(crucible_anomaly_file, mode_arg)
        logger.info('updated crucible anomaly file - %s/%s.txt' % (anomaly_dir, metric))

        # gzip the json timeseries data after analysis
        if os.path.isfile(anomaly_json):
            if not os.path.isfile(anomaly_json_gz):
                try:
                    f_in = open(anomaly_json)
                    f_out = gzip.open(anomaly_json_gz, 'wb')
                    f_out.writelines(f_in)
                    f_out.close()
                    f_in.close()
                    os.remove(anomaly_json)
                    if python_version == 2:
                        mode_arg = int('0644')
                    if python_version == 3:
                        mode_arg = '0o644'
                    os.chmod(anomaly_json_gz, mode_arg)
                    logger.info('gzipped - %s' % (anomaly_json_gz))
                except:
                    logger.error('error :: Failed to gzip data file - %s' % str(traceback.print_exc()))
            else:
                os.remove(anomaly_json)

        if run_script:
            if os.path.isfile(run_script):
                logger.info('running - %s' % (run_script))
                os.system('%s %s' % (str(run_script), str(crucible_anomaly_file)))

        # Remove metric check file
        try:
            os.remove(metric_check_file)
            logger.info('complete removed check file - %s' % (metric_check_file))
        except OSError:
            pass

    def run(self):
        """
        Called when the process intializes.
        """

        # Log management to prevent overwriting
        # Allow the bin/<skyline_app>.d to manage the log
        if os.path.isfile(skyline_app_logwait):
            try:
                os.remove(skyline_app_logwait)
            except OSError:
                logger.error('error - failed to remove %s, continuing' % skyline_app_logwait)
                pass

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
                pass
        else:
            logger.info('bin/%s.d log management done' % skyline_app)

        logger.info('process intialized')

        while 1:
            now = time()
            if settings.ENABLE_CRUCIBLE_DEBUG:
                logger.info('process started - %s' % int(now))

            # Make sure check_dir exists and has not been removed
            try:
                if settings.ENABLE_CRUCIBLE_DEBUG:
                    logger.info('checking check dir exists - %s' % settings.CRUCIBLE_CHECK_PATH)
                os.path.exists(settings.CRUCIBLE_CHECK_PATH)
            except:
                logger.error('error :: check dir did not exist - %s' % settings.CRUCIBLE_CHECK_PATH)
                if python_version == 2:
                    mode_arg = int('0755')
                if python_version == 3:
                    mode_arg = mode=0o755
                os.makedirs(settings.CRUCIBLE_CHECK_PATH, mode_arg)
                logger.info('check dir created - %s' % settings.CRUCIBLE_CHECK_PATH)
                os.path.exists(settings.CRUCIBLE_CHECK_PATH)
                # continue

            # Make sure Redis is up
            try:
                self.redis_conn.ping()
                logger.info('connected to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
            except:
                logger.info('skyline can not connect to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                sleep(10)
                logger.info('connecting to redis at socket path %s' % settings.REDIS_SOCKET_PATH)
                self.redis_conn = StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
                continue

            """
            Determine if any metric has been added to test
            """
            while True:

                # Report app up
                self.redis_conn.setex(skyline_app, 120, now)

                metric_var_files = [f for f in listdir(settings.CRUCIBLE_CHECK_PATH) if isfile(join(settings.CRUCIBLE_CHECK_PATH, f))]
#                if len(metric_var_files) == 0:
                if not metric_var_files:
                    logger.info('sleeping 10 no metric check files')
                    sleep(10)

                # Discover metric to analyze
                metric_var_files = ''
                metric_var_files = [f for f in listdir(settings.CRUCIBLE_CHECK_PATH) if isfile(join(settings.CRUCIBLE_CHECK_PATH, f))]
#                if len(metric_var_files) > 0:
                if metric_var_files:
                    break

            metric_var_files_sorted = sorted(metric_var_files)
            metric_check_file = settings.CRUCIBLE_CHECK_PATH + "/" + str(metric_var_files_sorted[0])

            logger.info('assigning check for processing - %s' % str(metric_var_files_sorted[0]))

            # Reset process_list
            try:
                self.process_list[:] = []
            except:
                logger.error('error :: failed to reset self.process_list')

            # Spawn processes
            pids = []
            spawned_pids = []
            pid_count = 0
            run_timestamp = int(now)
            for i in range(1, settings.CRUCIBLE_PROCESSES + 1):
                p = Process(target=self.spin_process, args=(i, run_timestamp, str(metric_check_file)))
                pids.append(p)
                pid_count += 1
                logger.info('starting %s of %s spin_process/es' % (str(pid_count), str(settings.CRUCIBLE_PROCESSES)))
                p.start()
                spawned_pids.append(p.pid)

            # Send wait signal to zombie processes
            # for p in pids:
            #     p.join()
            # Self monitor processes and terminate if any spin_process has run
            # for longer than CRUCIBLE_TESTS_TIMEOUT
            p_starts = time()
            while time() - p_starts <= settings.CRUCIBLE_TESTS_TIMEOUT:
                if any(p.is_alive() for p in pids):
                    # Just to avoid hogging the CPU
                    sleep(.1)
                else:
                    # All the processes are done, break now.
                    time_to_run = time() - p_starts
                    logger.info('%s :: %s spin_process/es completed in %.2f seconds' % (skyline_app, str(settings.CRUCIBLE_PROCESSES), time_to_run))
                    break
            else:
                # We only enter this if we didn't 'break' above.
                logger.info('%s :: timed out, killing all spin_process processes' % (skyline_app))
                for p in pids:
                    p.terminate()
                    p.join()

            while os.path.isfile(metric_check_file):
                sleep(1)
