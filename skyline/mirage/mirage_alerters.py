from __future__ import division, print_function
import logging
from smtplib import SMTP
import mirage_alerters
try:
    import urllib2
except ImportError:
    import urllib.request
    import urllib.error
from ast import literal_eval
from requests.utils import quote

# Added for graphs showing Redis data
import traceback
import redis
from msgpack import Unpacker
import datetime as dt
# @added 20180809 - Bug #2498: Incorrect scale in some graphs
from time import time
# @modified 20160820 - Issue #23 Test dependency updates
# Use Agg for matplotlib==1.5.2 upgrade, backwards compatibile
import matplotlib
matplotlib.use('Agg')
# @modified 20161228 - Feature #1828: ionosphere - mirage Redis data features
# Handle flake8 E402
if True:
    import matplotlib.pyplot as plt
    from matplotlib.pylab import rcParams
    from matplotlib.dates import DateFormatter
    import io
    import numpy as np
    import pandas as pd
    import syslog
    import os.path
    import sys
    import resource

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
sys.path.insert(0, os.path.dirname(__file__))

python_version = int(sys.version_info[0])
if python_version == 2:
    from email.MIMEMultipart import MIMEMultipart
    from email.MIMEText import MIMEText
    from email.MIMEImage import MIMEImage
    from email import charset
if python_version == 3:
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.image import MIMEImage

# @modified 20161228 - Feature #1828: ionosphere - mirage Redis data features
# Handle flake8 E402
if True:
    import settings
    import skyline_version
    from skyline_functions import (
        write_data_to_file, mkdir_p,
        # @added 20170603 - Feature #2034: analyse_derivatives
        nonNegativeDerivative, in_list)

skyline_app = 'mirage'
skyline_app_logger = '%sLog' % skyline_app
logger = logging.getLogger(skyline_app_logger)
skyline_app_logfile = '%s/%s.log' % (settings.LOG_PATH, skyline_app)

skyline_version = skyline_version.__absolute_version__

"""
Create any alerter you want here. The function will be invoked from trigger_alert.
Four arguments will be passed, two of them tuples: alert and metric.

alert: the tuple specified in your settings:\n
    alert[0]: The matched substring of the anomalous metric\n
    alert[1]: the name of the strategy being used to alert\n
    alert[2]: The timeout of the alert that was triggered\n
    alert[3]: The SECOND_ORDER_RESOLUTION_HOURS\n
metric: information about the anomaly itself\n
    metric[0]: the anomalous value\n
    metric[1]: The full name of the anomalous metric\n
    metric[2]: anomaly timestamp\n

second_order_resolution_seconds: int

context: the app name

"""

# FULL_DURATION to hours so that Mirage can surface the relevant timeseries data
# for Analyzer comparison in the graph
try:
    full_duration_seconds = int(settings.FULL_DURATION)
except:
    full_duration_seconds = 86400
full_duration_in_hours = full_duration_seconds / 60 / 60


def alert_smtp(alert, metric, second_order_resolution_seconds, context):
    """
    Called by :func:`~trigger_alert` and sends an alert via smtp to the
    recipients that are configured for the metric.

    """
    LOCAL_DEBUG = False
    logger = logging.getLogger(skyline_app_logger)
    if settings.ENABLE_DEBUG or LOCAL_DEBUG:
        logger.info('debug :: alert_smtp - sending smtp alert')
        logger.info('debug :: alert_smtp - Memory usage at start: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

    # SECOND_ORDER_RESOLUTION_SECONDS to hours so that Mirage surfaces the
    # relevant timeseries data in the graph
    second_order_resolution_in_hours = int(second_order_resolution_seconds) / 3600

    # @added 20161229 - Feature #1830: Ionosphere alerts
    # Added Ionosphere variables
    base_name = str(metric[1]).replace(settings.FULL_NAMESPACE, '', 1)
    if settings.IONOSPHERE_ENABLED:
        timeseries_dir = base_name.replace('.', '/')
        training_data_dir = '%s/%s/%s' % (
            settings.IONOSPHERE_DATA_FOLDER, str(int(metric[2])),
            timeseries_dir)
        graphite_image_file = '%s/%s.%s.graphite.%sh.png' % (
            training_data_dir, base_name, skyline_app,
            str(int(second_order_resolution_in_hours)))
        json_file = '%s/%s.%s.redis.%sh.json' % (
            training_data_dir, base_name, skyline_app,
            str(int(full_duration_in_hours)))
        training_data_redis_image = '%s/%s.%s.redis.plot.%sh.png' % (
            training_data_dir, base_name, skyline_app,
            str(int(full_duration_in_hours)))

    # For backwards compatibility
    if '@' in alert[1]:
        sender = settings.ALERT_SENDER
        recipient = alert[1]
        logger.info('alert_smtp - recipient for %s are %s' % (str(alert[0]), str(recipient)))
    else:
        sender = settings.SMTP_OPTS['sender']
        # @modified 20160806 - Added default_recipient
        try:
            recipients = settings.SMTP_OPTS['recipients'][alert[0]]
            use_default_recipient = False
            logger.info('alert_smtp - recipients for %s are %s' % (str(alert[0]), str(recipients)))
            logger.info('alert_smtp - recipients are:')
            try:
                for recip in recipients:
                    logger.info('alert_smtp :: recipient - %s' % str(recip))
            except:
                logger.error(traceback.format_exc())
                logger.error('alert_smtp :: error :: could not iterate recipients list')
        except:
            use_default_recipient = True
        if use_default_recipient:
            try:
                recipients = settings.SMTP_OPTS['default_recipient']
                logger.info(
                    'alert_smtp - using default_recipient as no recipients are configured for %s' %
                    str(alert[0]))
            except:
                logger.error(
                    'error :: alert_smtp - no known recipient for %s' %
                    str(alert[0]))
                return False

    # Backwards compatibility
    if type(recipients) is str:
        logger.info('alert_smtp :: recipients is a string')
        recipients = [recipients]
    else:
        logger.info('alert_smtp :: recipients is not a string, OK')

    # @added 20180524 - Task #2384: Change alerters to cc other recipients
    # The alerters did send an individual email to each recipient. This would be
    # more useful if one email was sent with the first smtp recipient being the
    # to recipient and the subsequent recipients were add in cc.
    if recipients:
        primary_recipient = False
        cc_recipients = False
        for i_recipient in recipients:
            if not primary_recipient:
                primary_recipient = str(i_recipient)
            if primary_recipient != i_recipient:
                if not cc_recipients:
                    cc_recipients = str(i_recipient)
                else:
                    new_cc_recipients = '%s,%s' % (str(cc_recipients), str(i_recipient))
                    cc_recipients = str(new_cc_recipients)
        logger.info(
            'alert_smtp - will send to primary_recipient :: %s, cc_recipients :: %s' %
            (str(primary_recipient), str(cc_recipients)))

    # @modified 20161228 - Feature #1830: Ionosphere alerts
    # Ionosphere alerts
    unencoded_graph_title = 'Skyline %s - ALERT at %s hours - %s' % (
        context, str(int(second_order_resolution_in_hours)), str(metric[0]))
    # @modified 20170603 - Feature #2034: analyse_derivatives
    # Added deriative functions to convert the values of metrics strictly
    # increasing monotonically to their deriative products in alert graphs and
    # specify it in the graph_title
    known_derivative_metric = False
    try:
        # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
        if settings.REDIS_PASSWORD:
            REDIS_ALERTER_CONN = redis.StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
        else:
            REDIS_ALERTER_CONN = redis.StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
    except:
        logger.error('error :: alert_smtp - redis connection failed')
    try:
        derivative_metrics = list(REDIS_ALERTER_CONN.smembers('derivative_metrics'))
    except:
        derivative_metrics = []
    redis_metric_name = '%s%s' % (settings.FULL_NAMESPACE, str(base_name))
    if redis_metric_name in derivative_metrics:
        known_derivative_metric = True
    if known_derivative_metric:
        try:
            non_derivative_monotonic_metrics = settings.NON_DERIVATIVE_MONOTONIC_METRICS
        except:
            non_derivative_monotonic_metrics = []
        skip_derivative = in_list(redis_metric_name, non_derivative_monotonic_metrics)
        if skip_derivative:
            known_derivative_metric = False
    if known_derivative_metric:
        unencoded_graph_title = 'Skyline %s - ALERT at %s hours - derivative graph - %s' % (
            context, str(int(second_order_resolution_in_hours)), str(metric[0]))

    if settings.ENABLE_DEBUG or LOCAL_DEBUG:
        logger.info('debug :: alert_smtp - unencoded_graph_title: %s' % unencoded_graph_title)
    graph_title_string = quote(unencoded_graph_title, safe='')
    graph_title = '&title=%s' % graph_title_string

    graphite_port = '80'
    if settings.GRAPHITE_PORT != '':
        graphite_port = str(settings.GRAPHITE_PORT)

    # @added 20180809 - Bug #2498: Incorrect scale in some graphs
    # If -xhours is used the scale is incorrect if x hours > than first
    # retention period, passing from and until renders the graph with the
    # correct scale.
    # @modified 20181009 - Feature #2618: alert_slack
    #                      Bug #2498: Incorrect scale in some graphs
    # Corrected the from_timestamp and until_timestamp as they were incorrectly
    # order, however Graphite still rendered the correct graph as it plotted
    # reverse, which is the same.  Also using the metric[0] value instead of
    # time()
    # from_timestamp = int(time())
    # until_timestamp = from_timestamp - int(second_order_resolution_seconds)
    until_timestamp = int(metric[2])
    from_timestamp = until_timestamp - int(second_order_resolution_seconds)

    graphite_from = dt.datetime.fromtimestamp(int(from_timestamp)).strftime('%H:%M_%Y%m%d')
    logger.info('graphite_from - %s' % str(graphite_from))
    graphite_until = dt.datetime.fromtimestamp(int(until_timestamp)).strftime('%H:%M_%Y%m%d')
    logger.info('graphite_until - %s' % str(graphite_until))

    # @modified 20180809 - Bug #2498: Incorrect scale in some graphs
    # link = '%s://%s:%s/render/?from=-%shours&target=cactiStyle(%s)%s%s&colorList=orange' % (
    #     settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
    #     graphite_port, str(int(second_order_resolution_in_hours)),
    #     metric[1], settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
    link = '%s://%s:%s/render/?from=%s&until=%s&target=cactiStyle(%s)%s%s&colorList=orange' % (
        settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST, graphite_port,
        str(graphite_from), str(graphite_until), metric[1],
        settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
    # @added 20170603 - Feature #2034: analyse_derivatives
    if known_derivative_metric:
        # @modified 20180809 - Bug #2498: Incorrect scale in some graphs
        # link = '%s://%s:%s/render/?from=-%shours&target=cactiStyle(nonNegativeDerivative(%s))%s%s&colorList=orange' % (
        #     settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
        #     graphite_port, str(int(second_order_resolution_in_hours)),
        #     metric[1], settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
        link = '%s://%s:%s/render/?from=%s&until=%s&target=cactiStyle(nonNegativeDerivative(%s))%s%s&colorList=orange' % (
            settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST, graphite_port,
            str(graphite_from), str(graphite_until), metric[1],
            settings.GRAPHITE_GRAPH_SETTINGS, graph_title)

    content_id = metric[1]
    image_data = None
    if settings.SMTP_OPTS.get('embed-images'):
        # @added 20161229 - Feature #1830: Ionosphere alerts
        # Use existing data if files exist
        if os.path.isfile(graphite_image_file):
            try:
                with open(graphite_image_file, 'r') as f:
                    image_data = f.read()
                logger.info('alert_smtp - using existing png - %s' % graphite_image_file)
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: alert_smtp - failed to read image data from existing png - %s' % graphite_image_file)
                logger.error('error :: alert_smtp - %s' % str(link))
                image_data = None

        if image_data is None:
            try:
                # @modified 20170913 - Task #2160: Test skyline with bandit
                # Added nosec to exclude from bandit tests
                image_data = urllib2.urlopen(link).read()  # nosec
                if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                    logger.info('debug :: alert_smtp - image data OK')
            except urllib2.URLError:
                logger.error(traceback.format_exc())
                logger.error('error :: alert_smtp - failed to get image graph')
                logger.error('error :: alert_smtp - %s' % str(link))
                image_data = None
                if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                    logger.info('debug :: alert_smtp - image data None')

    # If we failed to get the image or if it was explicitly disabled,
    # use the image URL instead of the content.
    if image_data is None:
        img_tag = '<img src="%s"/>' % link
    else:
        img_tag = '<img src="cid:%s"/>' % content_id
        if settings.ENABLE_DEBUG or LOCAL_DEBUG:
            logger.info('debug :: alert_smtp - img_tag: %s' % img_tag)

        if settings.IONOSPHERE_ENABLED:
            # Create Ionosphere Graphite image
            # @modified 20161229 - Feature #1830: Ionosphere alerts
            # Only write the data to the file if it does not exist
            if not os.path.isfile(graphite_image_file):
                try:
                    write_data_to_file(skyline_app, graphite_image_file, 'w', image_data)
                    logger.info(
                        'added %s Ionosphere Graphite image :: %s' % (
                            skyline_app, graphite_image_file))
                except:
                    logger.info(traceback.format_exc())
                    logger.error(
                        'error :: failed to add %s Ionosphere Graphite image' % (
                            skyline_app, graphite_image_file))
            else:
                logger.info(
                    '%s Ionosphere Graphite image already exists :: %s' % (
                        skyline_app, graphite_image_file))

    redis_image_data = None
    try:
        plot_redis_data = settings.PLOT_REDIS_DATA
    except:
        plot_redis_data = False

    if settings.SMTP_OPTS.get('embed-images') and plot_redis_data:
        # Create graph from Redis data
        redis_metric_key = '%s%s' % (settings.FULL_NAMESPACE, metric[1])
        try:
            raw_series = REDIS_ALERTER_CONN.get(redis_metric_key)
            if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                logger.info('debug :: alert_smtp - raw_series: %s' % 'OK')
        except:
            if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                logger.info('debug :: alert_smtp - raw_series: %s' % 'FAIL')

        try:
            unpacker = Unpacker(use_list=True)
            unpacker.feed(raw_series)
            timeseries_x = [float(item[0]) for item in unpacker]
            unpacker = Unpacker(use_list=True)
            unpacker.feed(raw_series)
            timeseries_y = [item[1] for item in unpacker]

            unpacker = Unpacker(use_list=False)
            unpacker.feed(raw_series)
            timeseries = list(unpacker)
        except:
            logger.error('error :: alert_smtp - unpack timeseries failed')
            timeseries = None

        # @added 20170603 - Feature #2034: analyse_derivatives
        if known_derivative_metric:
            try:
                derivative_timeseries = nonNegativeDerivative(timeseries)
                timeseries = derivative_timeseries
            except:
                logger.error('error :: alert_smtp - nonNegativeDerivative failed')

        if settings.IONOSPHERE_ENABLED and timeseries:
            '''
            .. todo: this is possibly to be used to allow the user to submit the
                FULL_DURATION duration data set for the features profile to be
                created against IF it is a Mirage metric.  This would allow for
                additional granularity in Mirage metrics, thereby maintaining
                their seasonality, but allow user and Skyline to analyze the
                anomaly at a FULL_DURATION resolution as well.  Not sure how to
                code that in Ionosphere context yet but could just be additonal
                flag in the Ionosphere record.  In the Ionosphere frontend, the
                user would be given an option to either create the features
                profile on the Mirage timeseries or the redis FULL_DURATION
                timeseries.  It is a little complicated, but doable.
                # @modified 20161229 - Feature #1828: ionosphere - mirage Redis data features
                However that ^^ is UNDESIRABLE in the Mirage/Ionosphere context
                at the moment.  Ionosphere must only profile SECOND_ORDER_RESOLUTION_HOURS
                currently so as to not pollute the seasonality aspect of Mirage
            '''
            # Create Ionosphere redis timeseries json if is does not exist
            # @modified 20161229 - Feature #1830: Ionosphere alerts
            # Only write the data to the file if it does not exist and replace
            # the timeseries object if a json file exists

            # @added 20170920 - Bug #2168: Strange Redis derivative graph
            using_original_redis_json = False

            if not os.path.isfile(json_file):
                timeseries_json = str(timeseries).replace('[', '(').replace(']', ')')
                try:
                    write_data_to_file(skyline_app, json_file, 'w', timeseries_json)
                    logger.info('added %s Ionosphere Redis data timeseries json file :: %s' % (skyline_app, json_file))
                except:
                    logger.info(traceback.format_exc())
                    logger.error('error :: failed to add %s Ionosphere Redis data timeseries json file' % (skyline_app, json_file))
            else:
                # Replace the timeseries object
                logger.info('%s Ionosphere Redis data timeseries json file already exists, using :: %s' % (skyline_app, json_file))
                anomaly_json = json_file
                try:
                    # Read the timeseries json file
                    with open(anomaly_json, 'r') as f:
                        raw_timeseries = f.read()
                    timeseries_array_str = str(raw_timeseries).replace('(', '[').replace(')', ']')
                    timeseries = literal_eval(timeseries_array_str)
                    logger.info('%s Redis timeseries replaced with timeseries from :: %s' % (skyline_app, anomaly_json))
                    timeseries_x = [float(item[0]) for item in timeseries]
                    timeseries_y = [item[1] for item in timeseries]
                    # @added 20170920 - Bug #2168: Strange Redis derivative graph
                    # This already has nonNegativeDerivative applied to it
                    using_original_redis_json = True
                except:
                    logger.error(traceback.format_exc())
                    logger.error(
                        'error :: %s failed to read timeseries data from %s' % (skyline_app, anomaly_json))
                    timeseries = None

        # @added 20170823 - Feature #2034: analyse_derivatives
        # Originally patterned and added to analyzer/alerters.py on 20170603
        if known_derivative_metric:

            # @added 20170920 - Bug #2168: Strange Redis derivative graph
            # If this is the Mirage Redis json it already has
            # nonNegativeDerivative applied to it
            if not using_original_redis_json:
                logger.info('alert_smtp - nonNegativeDerivative being applied')

                try:
                    derivative_timeseries = nonNegativeDerivative(timeseries)
                    timeseries = derivative_timeseries
                    # @added 20170920 - Bug #2168: Strange Redis derivative graph
                    logger.info('alert_smtp - nonNegativeDerivative applied')
                except:
                    logger.error('error :: alert_smtp - nonNegativeDerivative failed')
            else:
                logger.info('alert_smtp - nonNegativeDerivative not being applied, as it will have been applied in the original json')

        # @added 21070823 - Bug #2068: Analyzer smtp alert error on Redis plot with derivative metrics
        # Originally patterned and added to analyzer/alerters.py on 20170726
        # If the nonNegativeDerivative has been calculated we need to reset the
        # x and y as nonNegativeDerivative has to discard the first value as it
        # has no delta for it so the timeseries is 1 item less.
        timeseries_x = [float(item[0]) for item in timeseries]
        timeseries_y = [item[1] for item in timeseries]

        pd_series_values = None
        original_anomalous_datapoint = metric[0]
        if timeseries:
            try:
                values = pd.Series([x[1] for x in timeseries])
                # Because the truth value of a Series is ambiguous
                pd_series_values = True
            except:
                logger.error('error :: alert_smtp - pandas value series on timeseries failed')

            # @added 20170307 - Feature #1960: ionosphere_layers
            # To display the original anomalous datapoint value in the Redis plot
            try:
                original_anomalous_datapoint = float(timeseries[-1][1])
            except:
                logger.error('error :: alert_smtp - falied to determine the original_anomalous_datapoint from the timeseries')

        if pd_series_values:
            try:
                array_median = np.median(values)
                if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                    logger.info('debug :: alert_smtp - values median: %s' % str(array_median))

                array_amax = np.amax(values)
                if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                    logger.info('debug :: alert_smtp - array_amax: %s' % str(array_amax))
                array_amin = np.amin(values)
                if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                    logger.info('debug :: alert_smtp - array_amin: %s' % str(array_amin))
                mean = values.mean()
                if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                    logger.info('debug :: alert_smtp - mean: %s' % str(mean))
                stdDev = values.std()
                if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                    logger.info('debug :: alert_smtp - stdDev: %s' % str(stdDev))

                sigma3 = 3 * stdDev
                if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                    logger.info('debug :: alert_smtp - sigma3: %s' % str(sigma3))

                # sigma3_series = [sigma3] * len(values)

                sigma3_upper_bound = mean + sigma3
                try:
                    sigma3_lower_bound = mean - sigma3
                except:
                    sigma3_lower_bound = 0

                sigma3_upper_series = [sigma3_upper_bound] * len(values)
                sigma3_lower_series = [sigma3_lower_bound] * len(values)
                amax_series = [array_amax] * len(values)
                amin_series = [array_amin] * len(values)
                mean_series = [mean] * len(values)
            except:
                logger.error('error :: alert_smtp - numpy ops on series failed')
                mean_series = None

        if mean_series:
            # @modified 20170307 - Feature #1960: ionosphere_layers
            # To display the original anomalous datapoint value in the Redis plot
            # graph_title = 'Skyline %s - ALERT - at %s hours - Redis data\n%s - anomalous value: %s' % (context, str(int(full_duration_in_hours)), metric[1], str(metric[0]))
            graph_title = 'Skyline %s - ALERT - at %s hours - Redis data\n%s - anomalous value: %s' % (context, str(int(full_duration_in_hours)), metric[1], str(original_anomalous_datapoint))
            # @added 20170603 - Feature #2034: analyse_derivatives
            if known_derivative_metric:
                graph_title = 'Skyline %s - ALERT - at %s hours - Redis data (derivative graph)\n%s - anomalous value: %s' % (context, str(int(full_duration_in_hours)), metric[1], str(original_anomalous_datapoint))

            if python_version == 3:
                buf = io.StringIO()
            else:
                buf = io.BytesIO()

            # Too big
            # rcParams['figure.figsize'] = 12, 6
            rcParams['figure.figsize'] = 8, 4
            try:
                # fig = plt.figure()
                fig = plt.figure(frameon=False)
                ax = fig.add_subplot(111)
                ax.set_title(graph_title, fontsize='small')
                # @modified 20180417 - Bug #2358: set_axis_bgcolor method removed from Matplotlib - Luminosity
                #                      IssueID #49 'AxesSubplot' object has no attribute 'set_axis_bgcolor'
                # ax.set_axis_bgcolor('black')
                if hasattr(ax, 'set_facecolor'):
                    ax.set_facecolor('black')
                else:
                    ax.set_axis_bgcolor('black')

                try:
                    datetimes = [dt.datetime.utcfromtimestamp(ts) for ts in timeseries_x]
                    if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                        logger.info('debug :: alert_smtp - datetimes: %s' % 'OK')
                except:
                    logger.error('error :: alert_smtp - datetimes: %s' % 'FAIL')

                plt.xticks(rotation=0, horizontalalignment='center')
                xfmt = DateFormatter('%a %H:%M')
                plt.gca().xaxis.set_major_formatter(xfmt)

                ax.xaxis.set_major_formatter(xfmt)

                ax.plot(datetimes, timeseries_y, color='orange', lw=0.6, zorder=3)
                ax.tick_params(axis='both', labelsize='xx-small')

                max_value_label = 'max - %s' % str(array_amax)
                ax.plot(datetimes, amax_series, lw=1, label=max_value_label, color='m', ls='--', zorder=4)
                min_value_label = 'min - %s' % str(array_amin)
                ax.plot(datetimes, amin_series, lw=1, label=min_value_label, color='b', ls='--', zorder=4)
                mean_value_label = 'mean - %s' % str(mean)
                ax.plot(datetimes, mean_series, lw=1.5, label=mean_value_label, color='g', ls='--', zorder=4)

                sigma3_text = (r'3$\sigma$')
                # sigma3_label = '%s - %s' % (str(sigma3_text), str(sigma3))

                sigma3_upper_label = '%s upper - %s' % (str(sigma3_text), str(sigma3_upper_bound))
                ax.plot(datetimes, sigma3_upper_series, lw=1, label=sigma3_upper_label, color='r', ls='solid', zorder=4)

                if sigma3_lower_bound > 0:
                    sigma3_lower_label = '%s lower - %s' % (str(sigma3_text), str(sigma3_lower_bound))
                    ax.plot(datetimes, sigma3_lower_series, lw=1, label=sigma3_lower_label, color='r', ls='solid', zorder=4)

                ax.get_yaxis().get_major_formatter().set_useOffset(False)
                ax.get_yaxis().get_major_formatter().set_scientific(False)

                # Shrink current axis's height by 10% on the bottom
                box = ax.get_position()
                ax.set_position([box.x0, box.y0 + box.height * 0.1,
                                 box.width, box.height * 0.9])

                # Put a legend below current axis
                ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.05),
                          fancybox=True, shadow=True, ncol=4, fontsize='x-small')
                plt.rc('lines', lw=2, color='w')

                plt.grid(True)

                ax.grid(b=True, which='both', axis='both', color='lightgray',
                        linestyle='solid', alpha=0.5, linewidth=0.6)
                # @modified 20180417 - Bug #2358: set_axis_bgcolor method removed from Matplotlib - Luminosity
                #                      IssueID #49 'AxesSubplot' object has no attribute 'set_axis_bgcolor'
                # ax.set_axis_bgcolor('black')
                if hasattr(ax, 'set_facecolor'):
                    ax.set_facecolor('black')
                else:
                    ax.set_axis_bgcolor('black')

                rcParams['xtick.direction'] = 'out'
                rcParams['ytick.direction'] = 'out'
                ax.margins(y=.02, x=.03)
                # tight_layout removes the legend box
                # fig.tight_layout()

                if settings.IONOSPHERE_ENABLED:
                    if not os.path.exists(training_data_dir):
                        mkdir_p(training_data_dir)
                        logger.info('created dir - %s' % training_data_dir)

                    if not os.path.isfile(training_data_redis_image):
                        try:
                            plt.savefig(training_data_redis_image, format='png')
                            logger.info(
                                'alert_smtp - save Redis training data image - %s' % (
                                    training_data_redis_image))
                        except:
                            logger.info(traceback.format_exc())
                            logger.error(
                                'error :: alert_smtp - could not save - %s' % (
                                    training_data_redis_image))
                    else:
                        logger.info(
                            'alert_smtp - Redis training data image already exists - %s' % (
                                training_data_redis_image))

                try:
                    plt.savefig(buf, format='png')
                    # @added 20160814 - Bug #1558: Memory leak in Analyzer
                    # As per http://www.mail-archive.com/matplotlib-users@lists.sourceforge.net/msg13222.html
                    # savefig in the parent process was causing the memory leak
                    # the below fig.clf() and plt.close() did not resolve this
                    # however spawing a multiprocessing process for alert_smtp
                    # does solve this as issue as all memory is freed when the
                    # process terminates.
                    fig.clf()
                    plt.close(fig)
                    redis_graph_content_id = 'redis.%s' % metric[1]
                    redis_image_data = True
                    if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                        logger.info('debug :: alert_smtp - savefig: %s' % 'OK')
                except:
                    logger.info(traceback.format_exc())
                    logger.error('error :: alert_smtp - plt.savefig: %s' % 'FAIL')
            except:
                logger.error(traceback.format_exc())
                logger.error('error :: alert_smtp - could not build plot')

    if redis_image_data:
        redis_img_tag = '<img src="cid:%s"/>' % redis_graph_content_id
        if settings.ENABLE_DEBUG or LOCAL_DEBUG:
            logger.info('debug :: alert_smtp - redis_img_tag: %s' % str(redis_img_tag))
    else:
        # @modified 20161229 - Feature #1830: Ionosphere alerts
        # @modified 20170108 - Feature #1852: Ionosphere - features_profile matched graphite graphs
        # Restored the previous redis_img_tag method as some smtp alerts were
        # coming without a Redis graph, not all but some and for some reason,
        # I am pretty certain retrospectively that it was done that way from
        # testing I just wanted to try and be cleaner.
        # The redis_img_tag was changed at
        # https://github.com/earthgecko/skyline/commit/31bcacf3f90f0953ebed0d57260cb937e01f887c#diff-520bf2a218f65074ffead4d8184c138dR489
        redis_img_tag = '<img src="%s"/>' % 'none'
        # redis_img_tag = '<img src="none"/>'

    # @added 20170806 - Feature #1830: Ionosphere alerts
    # Show a human date in alerts
    alerted_at = str(dt.datetime.utcfromtimestamp(int(metric[2])))

    try:
        body = '<h3><font color="#dd3023">Sky</font><font color="#6698FF">line</font><font color="black"> %s alert</font></h3><br>' % context
        body += '<font color="black">metric: <b>%s</b></font><br>' % metric[1]
        body += '<font color="black">Anomalous value: %s (Mirage)</font><br>' % str(metric[0])
        body += '<font color="black">Original anomalous value: %s (Analyzer)</font><br>' % str(original_anomalous_datapoint)
        body += '<font color="black">Anomaly timestamp: %s</font><br>' % str(int(metric[2]))
        # @added 20170806 - Feature #1830: Ionosphere alerts
        # Show a human date in alerts
        body += '<font color="black">Anomalous at: %s</font><br>' % alerted_at
        body += '<font color="black">At hours: %s</font><br>' % str(int(second_order_resolution_in_hours))
        body += '<font color="black">Next alert in: %s seconds</font><br>' % str(alert[2])
        # @added 20170603 - Feature #2034: analyse_derivatives
        if known_derivative_metric:
            body += '<font color="black">Derivative graph: True</font><br>'

        more_body = ''
        if settings.IONOSPHERE_ENABLED:
            # @modified 20170823 - Bug #2142: 7bit SMTP encoding breaking long urls
            # Broke body into body and more_body to workaround the 990 character
            # limit per line for SMTP
            more_body += '<h3><font color="#dd3023">Ionosphere :: </font><font color="#6698FF">training data</font><font color="black"></font></h3>'
            ionosphere_link = '%s/ionosphere?timestamp=%s&metric=%s' % (
                settings.SKYLINE_URL, str(int(metric[2])), str(metric[1]))
            more_body += '<font color="black">To use this timeseries to train Skyline that this is not anomalous manage this training data at:<br>'
            more_body += '<a href="%s">%s</a></font>' % (ionosphere_link, ionosphere_link)
        if image_data:
            more_body += '<h3><font color="black">Graphite data at SECOND_ORDER_RESOLUTION_HOURS (aggregated)</font></h3>'
            more_body += '<div dir="ltr"><a href="%s">%s</a><br></div><br>' % (link, img_tag)
            more_body += '<font color="black">Clicking on the above graph will open to the Graphite graph with current data</font><br>'
        if redis_image_data:
            more_body += '<font color="black">min: %s  | max: %s   | mean: %s <br>' % (
                str(array_amin), str(array_amax), str(mean))
            more_body += '3-sigma: %s <br>' % str(sigma3)
            more_body += '3-sigma upper bound: %s   | 3-sigma lower bound: %s <br></font>' % (
                str(sigma3_upper_bound), str(sigma3_lower_bound))
            more_body += '<h3><font color="black">Redis data at FULL_DURATION</font></h3><br>'
            more_body += '<div dir="ltr">:%s<br></div>' % redis_img_tag
            more_body += '<font color="black">To disable the Redis data graph view, set PLOT_REDIS_DATA to False in your settings.py, if the Graphite graph is sufficient for you,<br>'
            more_body += 'however do note that will remove the 3-sigma and mean value too.</font>'
        more_body += '<br>'
        more_body += '<div dir="ltr" align="right"><font color="#dd3023">Sky</font><font color="#6698FF">line</font><font color="black"> version :: %s</font></div><br>' % str(skyline_version)
    except:
        logger.error('error :: alert_smtp - could not build body')
        logger.info(traceback.format_exc())

    # @modified 20180524 - Task #2384: Change alerters to cc other recipients
    # Do not send to each recipient, send to primary_recipient and cc the other
    # recipients, thereby sending only one email
    # for recipient in recipients:
    #     logger.info('alert_smtp - sending alert to %s' % (str(recipient)))
    if primary_recipient:
        logger.info(
            'alert_smtp - will send to primary_recipient :: %s, cc_recipients :: %s' %
            (str(primary_recipient), str(cc_recipients)))
        try:
            # @modified 20170823 - Bug #2142: 7bit SMTP encoding breaking long urls
            # Broke body into body and more_body to workaround the 990 character
            # limit per line for SMTP, using mixed as alternative indicates that
            # the client should select one of the parts for display and ignore
            # the rest (tripleee - https://stackoverflow.com/a/35115938)
            # msg = MIMEMultipart('alternative')
            msg = MIMEMultipart('mixed')

            # @added 20170812 - Bug #2142: 7bit SMTP encoding breaking long urls
            # set email charset and email encodings
            cs_ = charset.Charset('utf-8')
            cs_.header_encoding = charset.QP
            cs_.body_encoding = charset.QP
            msg.set_charset(cs_)

            msg['Subject'] = '[Skyline alert] - %s ALERT - %s' % (context, metric[1])
            msg['From'] = sender
            # @modified 20180524 - Task #2384: Change alerters to cc other recipients
            # msg['To'] = recipient
            msg['To'] = primary_recipient

            # @added 20180524 - Task #2384: Change alerters to cc other recipients
            # Added Cc
            if cc_recipients:
                msg['Cc'] = cc_recipients

            msg.attach(MIMEText(body, 'html'))
            # @added 20170823 - Bug #2142: 7bit SMTP encoding breaking long urls
            # Broke body into body and more_body to workaround the 990 character
            # limit per line for SMTP
            msg.attach(MIMEText(more_body, 'html'))
            msg.replace_header('content-transfer-encoding', 'quoted-printable')

            if image_data is not None:
                try:
                    msg_attachment = MIMEImage(image_data)
                    msg_attachment.add_header('Content-ID', '<%s>' % content_id)
                    msg.attach(msg_attachment)
                    if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                        logger.info('debug :: alert_smtp - msg_attachment - Graphite img source done')
                except:
                    logger.error('error :: alert_smtp - msg_attachment')
                    logger.info(traceback.format_exc())
            if redis_image_data:
                try:
                    buf.seek(0)
                    msg_plot_attachment = MIMEImage(buf.read())
                    msg_plot_attachment.add_header('Content-ID', '<%s>' % redis_graph_content_id)
                    msg.attach(msg_plot_attachment)
                    if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                        logger.info('debug :: alert_smtp - msg_plot_attachment - redis data done')
                except:
                    logger.error('error :: alert_smtp - msg_plot_attachment')
                    logger.info(traceback.format_exc())
        except:
            logger.error('error :: alert_smtp - could not attach')
            logger.info(traceback.format_exc())

        s = SMTP('127.0.0.1')
        try:
            # @modified 20180524 - Task #2384: Change alerters to cc other recipients
            # Send to primary_recipient and cc_recipients
            # s.sendmail(sender, recipient, msg.as_string())
            if cc_recipients:
                s.sendmail(sender, [primary_recipient, cc_recipients], msg.as_string())
            else:
                s.sendmail(sender, primary_recipient, msg.as_string())
            if settings.ENABLE_DEBUG or LOCAL_DEBUG:
                # logger.info('debug :: alert_smtp - message sent to %s OK' % str(recipient))
                logger.info(
                    'debug :: alert_smtp - message sent OK to primary_recipient :: %s, cc_recipients :: %s' %
                    (str(primary_recipient), str(cc_recipients)))
        except:
            logger.info(traceback.format_exc())
            # logger.error('error :: alert_smtp - could not send email to %s' % str(recipient))
            logger.error(
                'error :: alert_smtp - could not send email to primary_recipient :: %s, cc_recipients :: %s' %
                (str(primary_recipient), str(cc_recipients)))

        s.quit()

        if LOCAL_DEBUG:
            logger.info('debug :: alert_smtp - Memory usage after email: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

    return


def alert_pagerduty(alert, metric, second_order_resolution_seconds, context):
    """
    Called by :func:`~trigger_alert` and sends an alert via PagerDuty
    """
    if settings.PAGERDUTY_ENABLED:
        import pygerduty
        pager = pygerduty.PagerDuty(settings.PAGERDUTY_OPTS['subdomain'], settings.PAGERDUTY_OPTS['auth_token'])
        pager.trigger_incident(settings.PAGERDUTY_OPTS['key'], '%s alert - %s - %s' % (context, str(metric[0]), metric[1]))
    else:
        return False


def alert_hipchat(alert, metric, second_order_resolution_seconds, context):
    """
    Called by :func:`~trigger_alert` and sends an alert the hipchat room that is
    configured in settings.py.
    """

    # SECOND_ORDER_RESOLUTION_SECONDS to hours so that Mirage surfaces the
    # relevant timeseries data in the graph
    second_order_resolution_in_hours = int(second_order_resolution_seconds) / 3600

    if settings.HIPCHAT_ENABLED:
        sender = settings.HIPCHAT_OPTS['sender']
        import hipchat
        hipster = hipchat.HipChat(token=settings.HIPCHAT_OPTS['auth_token'])
        rooms = settings.HIPCHAT_OPTS['rooms'][alert[0]]
        unencoded_graph_title = 'Skyline %s - ALERT at %s hours - %s' % (
            context, str(int(second_order_resolution_in_hours)), str(metric[0]))
        graph_title_string = quote(unencoded_graph_title, safe='')
        graph_title = '&title=%s' % graph_title_string

        # @added 20180809 - Bug #2498: Incorrect scale in some graphs
        # If -xhours is used the scale is incorrect if x hours > than first
        # retention period, passing from and until renders the graph with the
        # correct scale.
        # @modified 20181009 - Feature #2618: alert_slack
        #                      Bug #2498: Incorrect scale in some graphs
        # Corrected the from_timestamp and until_timestamp as they were incorrectly
        # order, however Graphite still rendered the correct graph as it plotted
        # reverse, which is the same.  Also using the metric[0] value instead of
        # time()
        # from_timestamp = int(time())
        # until_timestamp = from_timestamp - int(second_order_resolution_seconds)
        until_timestamp = int(metric[2])
        from_timestamp = until_timestamp - int(second_order_resolution_seconds)

        graphite_from = dt.datetime.fromtimestamp(int(from_timestamp)).strftime('%H:%M_%Y%m%d')
        logger.info('graphite_from - %s' % str(graphite_from))
        graphite_until = dt.datetime.fromtimestamp(int(until_timestamp)).strftime('%H:%M_%Y%m%d')
        logger.info('graphite_until - %s' % str(graphite_until))

        if settings.GRAPHITE_PORT != '':
            # @modified 20180809 - Bug #2498: Incorrect scale in some graphs
            # link = '%s://%s:%s/render/?from=-%shour&target=cactiStyle(%s)%s%s&colorList=orange' % (settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST, settings.GRAPHITE_PORT, str(int(second_order_resolution_in_hours)), metric[1], settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
            link = '%s://%s:%s/render/?from=%s&until=%s&target=cactiStyle(%s)%s%s&colorList=orange' % (settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST, settings.GRAPHITE_PORT, str(graphite_from), str(graphite_until), metric[1], settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
        else:
            # @modified 20180809 - Bug #2498: Incorrect scale in some graphs
            # link = '%s://%s/render/?from=-%shour&target=cactiStyle(%s)%s%s&colorList=orange' % (settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST, str(int(second_order_resolution_in_hours)), metric[1], settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
            link = '%s://%s/render/?from=%s&until=%s&target=cactiStyle(%s)%s%s&colorList=orange' % (settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST, str(graphite_from), str(graphite_until), metric[1], settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
        embed_graph = "<a href='" + link + "'><img height='308' src='" + link + "'>" + metric[1] + "</a>"

        for room in rooms:
            hipster.method('rooms/message', method='POST', parameters={'room_id': room, 'from': 'skyline', 'color': settings.HIPCHAT_OPTS['color'], 'message': '%s - %s - Anomalous metric: %s (value: %s) at %s hours %s' % (sender, context, metric[1], str(metric[0]), str(int(second_order_resolution_in_hours)), embed_graph)})
    else:
        return False


def alert_syslog(alert, metric, second_order_resolution_seconds, context):
    if settings.SYSLOG_ENABLED:
        syslog_ident = settings.SYSLOG_OPTS['ident']
        message = '%s - Anomalous metric: %s (value: %s)' % (context, metric[1], str(metric[0]))
        if sys.version_info[:2] == (2, 6):
            syslog.openlog(syslog_ident, syslog.LOG_PID, syslog.LOG_LOCAL4)
        elif sys.version_info[:2] == (2, 7):
            syslog.openlog(ident="skyline", logoption=syslog.LOG_PID, facility=syslog.LOG_LOCAL4)
        elif sys.version_info[:1] == (3):
            syslog.openlog(ident="skyline", logoption=syslog.LOG_PID, facility=syslog.LOG_LOCAL4)
        else:
            syslog.openlog(syslog_ident, syslog.LOG_PID, syslog.LOG_LOCAL4)
        syslog.syslog(4, message)
    else:
        return False


# @added 20181006 - Feature #2618: alert_slack
def alert_slack(alert, metric, second_order_resolution_seconds, context):

    if not settings.SLACK_ENABLED:
        return False

    from slackclient import SlackClient
    import simplejson as json
    logger.info('alert_slack - anomalous metric :: alert: %s, metric: %s' % (str(alert), str(metric)))
    base_name = str(metric[1]).replace(settings.FULL_NAMESPACE, '', 1)

    full_duration_in_hours = int(second_order_resolution_seconds) / 3600
    second_order_resolution_in_hours = int(second_order_resolution_seconds) / 3600

    # The known_derivative_metric state is determine in case we need to surface
    # the png image from Graphite if the Ionosphere image is not available for
    # some reason.  This will result in Skyline at least still sending an alert
    # to slack, even if some gear fails in Ionosphere or slack alerting is used
    # without Ionosphere enabled. Yes not DRY but multiprocessing and spawn
    # safe.
    known_derivative_metric = False
    try:
        if settings.REDIS_PASSWORD:
            REDIS_ALERTER_CONN = redis.StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
        else:
            REDIS_ALERTER_CONN = redis.StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
    except:
        logger.error('error :: alert_slack - redis connection failed')
    try:
        derivative_metrics = list(REDIS_ALERTER_CONN.smembers('derivative_metrics'))
    except:
        derivative_metrics = []
    redis_metric_name = '%s%s' % (settings.FULL_NAMESPACE, str(base_name))
    if redis_metric_name in derivative_metrics:
        known_derivative_metric = True
    if known_derivative_metric:
        try:
            non_derivative_monotonic_metrics = settings.NON_DERIVATIVE_MONOTONIC_METRICS
        except:
            non_derivative_monotonic_metrics = []
        skip_derivative = in_list(redis_metric_name, non_derivative_monotonic_metrics)
        if skip_derivative:
            known_derivative_metric = False

    if known_derivative_metric:
        unencoded_graph_title = 'Skyline %s - ALERT at %s hours - derivative graph - %s' % (
            context, str(int(second_order_resolution_in_hours)), str(metric[0]))
        slack_title = '*Skyline %s - ALERT* on %s at %s hours - derivative graph - %s' % (
            context, str(metric[1]), str(int(second_order_resolution_in_hours)),
            str(metric[0]))
    else:
        unencoded_graph_title = 'Skyline %s - ALERT at %s hours - %s' % (
            context, str(int(second_order_resolution_in_hours)),
            str(metric[0]))
        slack_title = '*Skyline %s - ALERT* on %s at %s hours - %s' % (
            context, str(metric[1]), str(int(second_order_resolution_in_hours)),
            str(metric[0]))

    graph_title_string = quote(unencoded_graph_title, safe='')
    graph_title = '&title=%s' % graph_title_string
    until_timestamp = int(metric[2])
    from_timestamp = until_timestamp - int(second_order_resolution_seconds)
    graphite_from = dt.datetime.fromtimestamp(int(from_timestamp)).strftime('%H:%M_%Y%m%d')
    logger.info('graphite_from - %s' % str(graphite_from))
    graphite_until = dt.datetime.fromtimestamp(int(until_timestamp)).strftime('%H:%M_%Y%m%d')
    logger.info('graphite_until - %s' % str(graphite_until))

    if settings.GRAPHITE_PORT != '':
        if known_derivative_metric:
            link = '%s://%s:%s/render/?from=%s&until=%s&target=cactiStyle(nonNegativeDerivative(%s))%s%s&colorList=orange' % (
                settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
                settings.GRAPHITE_PORT, str(graphite_from), str(graphite_until),
                metric[1], settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
        else:
            link = '%s://%s:%s/render/?from=%s&until=%s&target=cactiStyle(%s)%s%s&colorList=orange' % (
                settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
                settings.GRAPHITE_PORT, str(graphite_from), str(graphite_until),
                metric[1], settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
    else:
        if known_derivative_metric:
            link = '%s://%s/render/?from=%s&until=%s&target=cactiStyle(nonNegativeDerivative(%s))%s%s&colorList=orange' % (
                settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
                str(graphite_from), str(graphite_until), metric[1],
                settings.GRAPHITE_GRAPH_SETTINGS, graph_title)
        else:
            link = '%s://%s/render/?from=%s&until=%s&target=cactiStyle(%s)%s%s&colorList=orange' % (
                settings.GRAPHITE_PROTOCOL, settings.GRAPHITE_HOST,
                str(graphite_from), str(graphite_until), metric[1],
                settings.GRAPHITE_GRAPH_SETTINGS, graph_title)

    # slack does not allow embedded images, nor will it fetch links behind
    # authentication so Skyline uploads a png graphite image with the message
    image_file = None

    # Use the Ionosphere image if it exists
    if settings.IONOSPHERE_ENABLED:
        timeseries_dir = base_name.replace('.', '/')
        training_data_dir = '%s/%s/%s' % (
            settings.IONOSPHERE_DATA_FOLDER, str(int(metric[2])),
            timeseries_dir)
        graphite_image_file = '%s/%s.%s.graphite.%sh.png' % (
            training_data_dir, base_name, skyline_app,
            str(int(full_duration_in_hours)))
        logger.info('alert_slack - interpolated Ionosphere Graphite image :: %s' % (
            graphite_image_file))
    else:
        graphite_image_file = None
        logger.info('alert_slack - no Ionosphere Graphite image interpolated')

    if graphite_image_file:
        if os.path.isfile(graphite_image_file):
            image_file = graphite_image_file
            logger.info('alert_slack - interpolated Ionosphere Graphite image file exists :: %s' % (
                graphite_image_file))
        else:
            logger.error('error :: alert_slack - interpolated Ionosphere Graphite image file not found :: %s' % (
                graphite_image_file))

    if not image_file:
        # Fetch the png from Graphite
        try:
            image_data = urllib2.urlopen(link).read()  # nosec
        except urllib2.URLError:
            logger.error(traceback.format_exc())
            logger.error('error :: alert_slack - failed to get image graph')
            logger.error('error :: alert_slack - %s' % str(link))
            image_data = None
        if image_data:
            image_file = '%s/%s.%s.graphite.%sh.png' % (
                settings.SKYLINE_TMP_DIR, base_name, skyline_app,
                str(int(full_duration_in_hours)))
            try:
                write_data_to_file(skyline_app, image_file, 'w', image_data)
                logger.info('alert_slack - added Graphite image :: %s' % (
                    image_file))
            except:
                logger.info(traceback.format_exc())
                logger.error(
                    'error :: alert_slack - failed to add %s Graphite image' % (
                        image_file))
                image_file = None
    try:
        filename = os.path.basename(image_file)
    except:
        filename = None

    try:
        bot_user_oauth_access_token = settings.SLACK_OPTS['bot_user_oauth_access_token']
    except:
        logger.error('error :: alert_slack - could not determine bot_user_oauth_access_token')
        return False
    try:
        channels = settings.SLACK_OPTS['channels'][alert[0]]
    except:
        logger.error('error :: alert_slack - could not determine channel')
        return False

    try:
        icon_emoji = settings.SLACK_OPTS['icon_emoji']
    except:
        icon_emoji = ':chart_with_upwards_trend:'

    try:
        sc = SlackClient(bot_user_oauth_access_token)
    except:
        logger.info(traceback.format_exc())
        logger.error('error :: alert_slack - could not initiate SlackClient')
        return False

    ionosphere_link = '%s/ionosphere?timestamp=%s&metric=%s' % (
        settings.SKYLINE_URL, str(int(metric[2])), str(metric[1]))

    # This block is not used but left heve as it is the pattern for sending
    # messages using the chat.postMessage methods and could possibly be the
    # failover for a files.upload error or future messages.
    message_payload  = json.dumps([{
        "fallback": slack_title + ' - ' + link,
        "title": slack_title,
        "title_link": link,
        "image_url": link,
        "text": 'Ionosphere training data :: ' + ionosphere_link,
        "color": "#764FA5"
    }])
    send_slack_message = False

    for channel in channels:
        if send_slack_message:
            try:
                send_message = sc.api_call(
                    'chat.postMessage',
                    channel=channel,
                    icon_emoji=icon_emoji,
                    attachments=message_payload)
                if not send_message['ok']:
                    logger.error('error :: alert_slack - failed to send slack message')
                else:
                    logger.info('alert_slack - sent slack message')
            except:
                logger.info(traceback.format_exc())
                logger.error('error :: alert_slack - could not send_message')
                return False

        if settings.IONOSPHERE_ENABLED:
            initial_comment = slack_title + ' :: <' + link  + '|graphite image link>\n*Ionosphere training dir* :: <' + ionosphere_link + '|training data link>'
        else:
            initial_comment = slack_title + ' :: <' + link  + '|graphite image link>'

        try:
            # slack does not allow embedded images, nor links behind authentication
            # or color text, so we have jump through all the API hoops to end up
            # having to upload an image with a very basic message.
            if os.path.isfile(image_file):
                slack_file_upload = sc.api_call(
                    'files.upload', filename=filename, channels=channel,
                    initial_comment=initial_comment, file=open(image_file, 'rb'))
                if not slack_file_upload['ok']:
                    logger.error('error :: alert_slack - failed to send slack message')
            else:
                send_text = initial_comment + '  ::  error :: there was no graph image to upload'
                send_message = sc.api_call(
                    'chat.postMessage',
                    channel=channel,
                    icon_emoji=icon_emoji,
                    text=send_text)
                if not send_message['ok']:
                    logger.error('error :: alert_slack - failed to send slack message')
                else:
                    logger.info('alert_slack - sent slack message')
        except:
            logger.info(traceback.format_exc())
            logger.error('error :: alert_slack - could not upload file')
            return False


def trigger_alert(alert, metric, second_order_resolution_seconds, context):
    """
    Called by :class:`~skyline.skyline.Mirage.run` to trigger an alert,
    Mirage passes two arguments, both of them tuples.  The alerting strategy
    is determined and the approriate alert def is then called and passed the
    tuples.

    :param alert:
        The alert tuple specified in settings.py.\n
        alert[0]: The matched substring of the anomalous metric\n
        alert[1]: the name of the strategy being used to alert\n
        alert[2]: The timeout of the alert that was triggered\n
        alert[3]: The SECOND_ORDER_RESOLUTION_HOURS in seconds\n
    :param meric:
        The metric tuple.\n
        metric[0]: the anomalous value\n
        metric[1]: The full name of the anomalous metric\n
        metric[2]: anomaly timestamp\n
    :param context: app name
    :type context: str

    """

    if '@' in alert[1]:
        strategy = 'alert_smtp'
    else:
        strategy = 'alert_%s' % alert[1]

    try:
        getattr(mirage_alerters, strategy)(alert, metric, second_order_resolution_seconds, context)
    except:
        logger.error('error :: alerters - %s - getattr error' % strategy)
        logger.info(traceback.format_exc())
