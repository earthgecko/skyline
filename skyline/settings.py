"""
Shared settings
"""

REDIS_SOCKET_PATH = '/tmp/redis.sock'
"""
:var REDIS_SOCKET_PATH: The path for the Redis unix socket
:vartype REDIS_SOCKET_PATH: str
"""

LOG_PATH = '/var/log/skyline'
"""
:var LOG_PATH: The Skyline logs directory. Do not include a trailing slash.
:vartype LOG_PATH: str
"""

PID_PATH = '/var/run/skyline'
"""
:var PID_PATH: The Skyline pids directory. Do not include a trailing slash.
:vartype PID_PATH: str
"""

SKYLINE_TMP_DIR = '/tmp/skyline'
"""
:var SKYLINE_TMP_DIR: The Skyline tmp dir. Do not include a trailing slash.
    It is recommended you keep this in the /tmp directory which normally uses
    tmpfs.
:vartype SKYLINE_TMP_DIR: str
"""

FULL_NAMESPACE = 'metrics.'
"""
:var FULL_NAMESPACE: Metrics will be prefixed with this value in Redis.
:vartype FULL_NAMESPACE: str
"""

GRAPHITE_SOURCE = ''
"""
:var GRAPHITE_SOURCE: The data source
:vartype GRAPHITE_SOURCE: str
"""

ENABLE_DEBUG = False
"""
:var ENABLE_DEBUG: Enable additional debug logging - useful for development
    only, this should definitely be set to False on production systems.
:vartype ENABLE_DEBUG: str
"""

MINI_NAMESPACE = 'mini.'
"""
:var MINI_NAMESPACE: The Horizon agent will make T'd writes to both the full
    namespace and the mini namespace. Oculus gets its data from everything in
    the mini namespace.
:vartype MINI_NAMESPACE: str
"""

FULL_DURATION = 86400
"""
:var FULL_DURATION: This is the rolling duration that will be stored in Redis.
    Be sure to pick a value that suits your memory capacity, your CPU capacity
    and your overall metrics count. Longer durations take a longer to analyze,
    but they can help the algorithms reduce the noise and provide more accurate
    anomaly detection.
:vartype FULL_DURATION: str
"""

MINI_DURATION = 3600
"""
:var MINI_DURATION: This is the duration of the 'mini' namespace, if you are
    also using the Oculus service. It is also the duration of data that is
    displayed in the Webapp 'mini' view.
:vartype MINI_DURATION: str
"""

GRAPHITE_HOST = 'YOUR_GRAPHITE_HOST.example.com'
"""
:var GRAPHITE_HOST: If you have a Graphite host set up, set this metric to get
    graphs on Skyline and Horizon. Don't include http:// since this is used for
    carbon host as well.
:vartype GRAPHITE_HOST: str
"""

GRAPHITE_PROTOCOL = 'http'
"""
:var GRAPHITE_PROTOCOL: Graphite host protocol - http or https
:vartype GRAPHITE_PROTOCOL: str
"""

GRAPHITE_PORT = '80'
"""
:var GRAPHITE_PORT: Graphite host port - for a specific port if graphite runs on
    a port other than 80, e.g. '8888'
:vartype GRAPHITE_PORT: str
"""

GRAPHITE_CONNECT_TIMEOUT = 5
"""
:var GRAPHITE_CONNECT_TIMEOUT: Graphite connect timeout - this allows for the
    gracefully failure of any graphite requests so that no graphite related
    functions ever block for too long.
:vartype GRAPHITE_CONNECT_TIMEOUT: int
"""

GRAPHITE_READ_TIMEOUT = 10
"""
:var GRAPHITE_READ_TIMEOUT: Graphite read timeout
:vartype GRAPHITE_READ_TIMEOUT: int
"""

GRAPHITE_GRAPH_SETTINGS = '&width=588&height=308&bgcolor=000000&fontBold=true&fgcolor=C0C0C0'
"""
:var GRAPHITE_GRAPH_SETTINGS: These are graphite settings in terms of alert
    graphs - this is defaulted to a format that is more colourblind friendly
    than the default graphite graphs.
:vartype GRAPHITE_GRAPH_SETTINGS: str
"""

TARGET_HOURS = '7'
"""
:var TARGET_HOURS: The number of hours data to graph in alerts.
:vartype TARGET_HOURS: str
"""

GRAPH_URL = GRAPHITE_PROTOCOL + '://' + GRAPHITE_HOST + ':' + GRAPHITE_PORT + '/render/?width=1400&from=-' + TARGET_HOURS + 'hour&target='
"""
:var GRAPH_URL: The graphite URL for alert graphs will be appended with the
    relevant metric name in each alert.
:vartype GRAPH_URL: str

.. note:: There is probably no neeed to change this unless you what a different
    size graph sent with alerts.
"""

CARBON_PORT = 2003
"""
:var CARBON_PORT: If you have a Graphite host set up, set its Carbon port.
:vartype CARBON_PORT: int
"""

OCULUS_HOST = ''
"""
:var OCULUS_HOST: If you have Oculus set up, set this to ``http://<OCULUS_HOST>``
:vartype OCULUS_HOST: str

- If you do not want to use Oculus, leave this empty. However if you comment
  this out, Skyline will not work!  Speed improvements will occur when Oculus
  support is disabled.
"""

SERVER_METRICS_NAME = 'YOUR_HOSTNAME'
"""
:var SERVER_METRICS_NAME: The hostname of the Skyline.
:vartype SERVER_METRICS_NAME: str

- This is to allow for multiple Skyline nodes to send metrics to a Graphite
  instance on the Skyline namespace sharded by this setting, like carbon.relays.
  If you want multiple Skyline hosts, set the hostname of the skyline here and
  metrics will be as e.g. ``skyline.analyzer.skyline-01.run_time``
"""

MIRAGE_CHECK_PATH = '/opt/skyline/mirage/check'
"""
:var MIRAGE_CHECK_PATH: This is the location the Skyline analyzer will write
    the second order resolution anomalies to check to a file on disk - absolute
    path
:vartype MIRAGE_CHECK_PATH: str
"""

CRUCIBLE_CHECK_PATH = '/opt/skyline/crucible/check'
"""
:var CRUCIBLE_CHECK_PATH: This is the location the Skyline apps will write the
    anomalies to for crucible to check to a file on disk - absolute path
:vartype CRUCIBLE_CHECK_PATH: str
"""

PANORAMA_CHECK_PATH = '/opt/skyline/panaroma/check'
"""
:var PANORAMA_CHECK_PATH: This is the location the Skyline apps will write the
    anomalies to for Panorama to check to a file on disk - absolute path
:vartype PANORAMA_CHECK_PATH: str
"""

PANDAS_VERSION = '0.18.1'
"""
:var PANDAS_VERSION: Pandas version in use
:vartype PANDAS_VERSION: str

- Declaring the version of pandas in use reduces a large amount of interpolating
  in all the skyline modules.  There are some differences from pandas >= 0.18.0
  however the original Skyline could run on lower versions of pandas.
"""

ALERTERS_SETTINGS = True
"""
.. note:: Alerters can be enabled alerters due to that fact that not everyone
    will necessarily want all 3rd party alerters.  Enabled what 3rd alerters you
    require here.   This enables only the alerters that are required to be
    imported and means that not all alerter related modules in
    ``requirements.txt`` have to be installed, only those you require.
"""

SYSLOG_ENABLED = True
"""
:var SYSLOG_ENABLED: Alerter - enables Skyline apps to submit anomalous metric
    details to syslog.
:vartype SYSLOG_ENABLED: boolean
"""

HIPCHAT_ENABLED = False
"""
:var HIPCHAT_ENABLED: Enables the Hipchat alerter
:vartype HIPCHAT_ENABLED: boolean
"""

PAGERDUTY_ENABLED = False
"""
:var PAGERDUTY_ENABLED: Enables the Pagerduty alerter
:vartype PAGERDUTY_ENABLED: boolean
"""

SLACK_ENABLED = False
"""
:var SLACK_ENABLED: Enables the Slack alerter`
:vartype SLACK_ENABLED: boolean
"""


"""
Analyzer settings
"""

ANOMALY_DUMP = 'webapp/static/dump/anomalies.json'
"""
:var ANOMALY_DUMP: This is the location the Skyline agent will write the
    anomalies file to disk.  It needs to be in a location accessible to the webapp.
:vartype ANOMALY_DUMP: str
"""

ANALYZER_PROCESSES = 1
"""
:var ANALYZER_PROCESSES: This is the number of processes that the Skyline
    Analyzer will spawn.
:vartype ANALYZER_PROCESSES: int

- Analysis is a very CPU-intensive procedure. You will see optimal results if
  you set ANALYZER_PROCESSES to several less than the total number of CPUs on
  your server. Be sure to leave some CPU room for the Horizon workers and for
  Redis.
- IMPORTANTLY bear in mind that your Analyzer run should be able to analyze all
  your metrics in the same resoluton as your metrics. So for example if you have
  1000 metrics at a resolution of 60 seconds (e.g. one datapoint per 60
  seconds), you are aiming to try and analyze all of those within 60 seconds.
  If you do not the anomaly detection begins to lag and it is no longer really
  near realtime.  That stated, bear in mind if you are not processing 10s of
  1000s of metrics, you may only need one Analyzer process.  To determine your
  optimal settings take note of 'seconds to run' values in the Analyzer log.
"""

ANALYZER_OPTIMUM_RUN_DURATION = 60
"""
:var ANALYZER_OPTIMUM_RUN_DURATION: This is how many seconds it would be
    optimum for Analyzer to be able to analyze all your metrics in.
:vartype ANALYZER_OPTIMUM_RUN_DURATION: int

.. note:: In the original Skyline this was hardcorded to 5.
"""

MAX_ANALYZER_PROCESS_RUNTIME = 180
"""
:var MAX_ANALYZER_PROCESS_RUNTIME: What is the maximum number of seconds an
    Analyzer process should run analysing a set of ``assigned_metrics``
:vartype MAX_ANALYZER_PROCESS_RUNTIME: int

- How many seconds This is for Analyzer to self monitor its own analysis threads
  and terminate any threads that have run longer than this.  Although Analyzer
  and mutliprocessing are very stable, there are edge cases in real world
  operations which can very infrequently cause a process to hang.
"""

STALE_PERIOD = 500
"""
:var STALE_PERIOD: This is the duration, in seconds, for a metric to become
    'stale' and for the analyzer to ignore it until new datapoints are added.
    'Staleness' means that a datapoint has not been added for STALE_PERIOD
    seconds.
:vartype STALE_PERIOD: int
"""

MIN_TOLERABLE_LENGTH = 1
"""
:var MIN_TOLERABLE_LENGTH: This is the minimum length of a timeseries, in
    datapoints, for the analyzer to recognize it as a complete series.
:vartype MIN_TOLERABLE_LENGTH: int
"""

MAX_TOLERABLE_BOREDOM = 100
"""
:var MAX_TOLERABLE_BOREDOM: Sometimes a metric will continually transmit the
    same number. There's no need to analyze metrics that remain boring like
    this, so this setting determines the amount of boring datapoints that will
    be allowed to accumulate before the analyzer skips over the metric. If the
    metric becomes noisy again, the analyzer will stop ignoring it.
:vartype MAX_TOLERABLE_BOREDOM: int
"""

BOREDOM_SET_SIZE = 1
"""
:var BOREDOM_SET_SIZE: By default, the analyzer skips a metric if it it has
    transmitted a single number :mod:`settings.MAX_TOLERABLE_BOREDOM` times.
:vartype BOREDOM_SET_SIZE: int

- Change this setting if you wish the size of the ignored set to be higher (ie,
  ignore the metric if there have only been two different values for the past
  :mod:`settings.MAX_TOLERABLE_BOREDOM` datapoints).  This is useful for
  timeseries that often oscillate between two values.
"""

CANARY_METRIC = 'statsd.numStats'
"""
:var CANARY_METRIC: The metric name to use as the CANARY_METRIC
:vartype CANARY_METRIC: str

- The canary metric should be a metric with a very high, reliable resolution
  that you can use to gauge the status of the system as a whole.  Like the
  ``statsd.numStats`` or a metric in the ``carbon.`` namespace
"""

ALGORITHMS = [
    'histogram_bins',
    'first_hour_average',
    'stddev_from_average',
    'grubbs',
    'ks_test',
    'mean_subtraction_cumulation',
    'median_absolute_deviation',
    'stddev_from_moving_average',
    'least_squares',
]
"""
:var ALGORITHMS: These are the algorithms that the Analyzer will run. To add a
    new algorithm, you must both define the algorithm in algorithms.py and add
    it's name here.
:vartype ALGORITHMS: array
"""

CONSENSUS = 6
"""
:var CONSENSUS: This is the number of algorithms that must return True before a
    metric is classified as anomalous by Analyzer.
:vartype CONSENSUS: int
"""

RUN_OPTIMIZED_WORKFLOW = True
"""
:var RUN_OPTIMIZED_WORKFLOW: This sets Analyzer to run in an optimized manner.
:vartype RUN_OPTIMIZED_WORKFLOW: boolean

- This sets Analyzer to run in an optimized manner in terms of using the
  CONSENSUS setting to dynamically determine in what order and how many
  algorithms need to be run be able to achieve CONSENSUS.  This reduces the
  amount of work that Analyzer has to do per run.  It is recommended that
  this be set to True in most circumstances to ensure that Analyzer is run as
  efficiently as possible, UNLESS you are working on algorithm development then
  you may want this to be False
"""

ENABLE_ALGORITHM_RUN_METRICS = True
"""
:var ENABLE_ALGORITHM_RUN_METRICS: This enables algorithm timing metrics to
    Graphite
:vartype ENABLE_ALGORITHM_RUN_METRICS: boolean

- This will send additional metrics to the graphite namespaces of:
  ``skyline.analyzer.<hostname>.algorithm_breakdown.<algorithm_name>.timings.median_time``
  ``skyline.analyzer.<hostname>.algorithm_breakdown.<algorithm_name>.timings.times_run``
  ``skyline.analyzer.<hostname>.algorithm_breakdown.<algorithm_name>.timings.total_time``
  These are related to the RUN_OPTIMIZED_WORKFLOW performance tuning.
"""

ENABLE_ALL_ALGORITHMS_RUN_METRICS = False
"""
:var ENABLE_ALL_ALGORITHMS_RUN_METRICS: DEVELOPMENT only - run and time all
:vartype ENABLE_ALL_ALGORITHMS_RUN_METRICS: boolean

.. warning:: If set to ``True``, Analyzer will revert to it's original unoptimized
    workflow and will run and time all algorithms against all timeseries.
"""

ENABLE_SECOND_ORDER = False
"""
:var ENABLE_SECOND_ORDER: This is to enable second order anomalies.
:vartype ENABLE_SECOND_ORDER: boolean

.. warning:: EXPERIMENTAL - This is an experimental feature, so it's turned off
    by default.
"""

ENABLE_ALERTS = True
"""
:var ENABLE_ALERTS: This enables Analyzer alerting.
:vartype ENABLE_ALERTS: boolean
"""

ENABLE_MIRAGE = False
"""
:var ENABLE_MIRAGE: This enables Analyzer to output to Mirage
:vartype ENABLE_MIRAGE: boolean
"""

ENABLE_FULL_DURATION_ALERTS = True
"""
:var ENABLE_FULL_DURATION_ALERTS: This enables Analyzer to alert on all
    FULL_DURATION anomalies.
:vartype ENABLE_FULL_DURATION_ALERTS: boolean

- This enables FULL_DURATION alerting for Analyzer, if ``True`` Analyzer will
  send ALL alerts on any alert tuple that have a ``SECOND_ORDER_RESOLUTION_HOURS``
  value defined for Mirage in their alert tuple.  If ``False`` Analyzer will only
  add a Mirage check and allow Mirage to do the alerting.

.. note:: If you have Mirage enabled and have defined ``SECOND_ORDER_RESOLUTION_HOURS``
    values in the desired metric alert tuples, you want this set to ``False``
"""

ANALYZER_CRUCIBLE_ENABLED = False
"""
:var ANALYZER_CRUCIBLE_ENABLED: This enables Analyzer to output to Crucible
:vartype ANALYZER_CRUCIBLE_ENABLED: boolean

- This enables Analyzer to send Crucible data, if this is set to ``True`` ensure
  that :mod:`settings.CRUCIBLE_ENABLED` is also set to ``True`` in the Crucible
  settings block.

.. warning:: Not recommended from production, will make a LOT of data files in
    the :mod:`settings.CRUCIBLE_DATA_FOLDER`
"""

ALERTS = (
    ('skyline', 'smtp', 1800),
    ('skyline_test.alerters.test', 'smtp', 1800),
    ('skyline_test.alerters.test', 'hipchat', 1800),
    ('skyline_test.alerters.test', 'pagerduty', 1800),
)
"""
:var ALERTS: This enables analyzer alerting.
:vartype ALERTS: tuples

This is the config for which metrics to alert on and which strategy to use for
each.  Alerts will not fire twice within ``EXPIRATION_TIME``, even if they
trigger again.

- **Tuple schema example**::

    ALERTS = (
        # ('<metric_namespace>', '<alerter>', EXPIRATION_TIME, SECOND_ORDER_RESOLUTION_HOURS),
        # With SECOND_ORDER_RESOLUTION_HOURS being optional for Mirage
        ('metric1', 'smtp', 1800),
        ('important_metric.total', 'smtp', 600),
        ('important_metric.total', 'pagerduty', 1800),
        ('metric3', 'hipchat', 600),
        # Log all anomalies to syslog
        ('stats.', 'syslog', 1),
        # Wildcard namespaces can be used as well
        ('metric4.thing.*.requests', 'stmp', 900),
        # mirage - SECOND_ORDER_RESOLUTION_HOURS - if added and Mirage is enabled
        ('metric5.thing.*.rpm', 'smtp', 900, 168),
    )

- Alert tuple parameters are:

:param metric: metric name.
:param alerter: alerter name.
:param EXPIRATION_TIME: Alerts will not fire twice within this amount of
    seconds, even if they trigger again.
:param SECOND_ORDER_RESOLUTION_HOURS: (optional) The number of hours that Mirage
    should surface the metric timeseries for
:type metric: str
:type alerter: str
:type EXPIRATION_TIME: int
:type SECOND_ORDER_RESOLUTION_HOURS: int

.. note:: Consider using the default skyline_test.alerters.test for testing
    alerts with.

"""

PLOT_REDIS_DATA = True
"""
:var PLOT_REDIS_DATA: Plot graph using Redis timeseries data on with
    Analyzer alerts.
:vartype PLOT_REDIS_DATA: boolean

- There are times when Analyzer alerts have no data in the Graphite graphs
  and/or the data in the Graphite graph is skewed due to retentions aggregation.
  This mitigates that by creating a graph using the Redis timeseries data and
  embedding the image in the Analyzer alerts as well.

.. note:: The Redis data plot has the following additional information as well,
    the 3sigma upper (and if applicable lower) bounds and the mean are plotted
    and reported too.  Although less is more effective, in this case getting a
    visualisation of the 3sigma boundaries is informative.

"""

# Each alert module requires additional information.
SMTP_OPTS = {
    # This specifies the sender of email alerts.
    'sender': 'skyline@your_domain.com',
    # recipients is a dictionary mapping metric names
    # (exactly matching those listed in ALERTS) to an array of e-mail addresses
    'recipients': {
        'skyline': ['you@your_domain.com', 'them@your_domain.com'],
        'skyline_test.alerters.test': ['you@your_domain.com'],
    },
    'embed-images': True,
}
"""
:var SMTP_OPTS: Your SMTP settings.
:vartype SMTP_OPTS: dictionary
"""

HIPCHAT_OPTS = {
    'auth_token': 'hipchat_auth_token',
    'sender': 'hostname or identifier',
    # list of hipchat room_ids to notify about each anomaly
    # (similar to SMTP_OPTS['recipients'])
    'rooms': {
        'skyline': (12345,),
        'skyline_test.alerters.test': (12345,),
    },
    # Background color of hipchat messages
    # (One of 'yellow', 'red', 'green', 'purple', 'gray', or 'random'.)
    'color': 'purple',
}
"""
:var HIPCHAT_OPTS: Your Hipchat settings.
:vartype HIPCHAT_OPTS: dictionary

HipChat alerts require python-simple-hipchat
"""

PAGERDUTY_OPTS = {
    # Your pagerduty subdomain and auth token
    'subdomain': 'example',
    'auth_token': 'your_pagerduty_auth_token',
    # Service API key (shown on the detail page of a 'Generic API' service)
    'key': 'your_pagerduty_service_api_key',
}
"""
:var PAGERDUTY_OPTS: Your SMTP settings.
:vartype PAGERDUTY_OPTS: dictionary

PagerDuty alerts require pygerduty
"""

SYSLOG_OPTS = {
    'ident': 'skyline',
}
"""
:var SYSLOG_OPTS: Your SMTP settings.
:vartype SYSLOG_OPTS: dictionary

syslog alerts requires an ident this adds a LOG_WARNING message to the
LOG_LOCAL4 which will ship to any syslog or rsyslog down the line.  The
``EXPIRATION_TIME`` for the syslog alert method should be set to 1 to fire
every anomaly into the syslog.
"""

"""
Horizon settings
"""

WORKER_PROCESSES = 2
"""
:var WORKER_PROCESSES: This is the number of worker processes that will consume
    from the Horizon queue.
:vartype WORKER_PROCESSES: int
"""

HORIZON_IP = '0.0.0.0'
"""
:var HORIZON_IP: The IP address for Horizon to bind to.  Defaults to
    ``gethostname()``
:vartype HORIZON_IP: str
"""

PICKLE_PORT = 2024
"""
:var PICKLE_PORT: This is the port that listens for Graphite pickles over TCP,
    sent by Graphite's carbon-relay agent.
:vartype PICKLE_PORT: str
"""

UDP_PORT = 2025
"""
:var UDP_PORT: This is the port that listens for Messagepack-encoded UDP packets.
:vartype UDP_PORT: str
"""

CHUNK_SIZE = 10
"""
:var CHUNK_SIZE: This is how big a 'chunk' of metrics will be before they are
    added onto the shared queue for processing into Redis.
:vartype CHUNK_SIZE: int

- If you are noticing that Horizon is having trouble consuming metrics, try
  setting this value a bit higher.
"""

MAX_QUEUE_SIZE = 500
"""
:var MAX_QUEUE_SIZE: Maximum allowable length of the processing queue
:vartype MAX_QUEUE_SIZE: int

This is the maximum allowable length of the processing queue before new chunks
are prevented from being added. If you consistently fill up the processing
queue, a higher MAX_QUEUE_SIZE will not save you. It most likely means that the
workers do not have enough CPU alotted in order to process the queue on time.
Try increasing :mod:`settings.CHUNK_SIZE` and decreasing
:mod:`settings.ANALYZER_PROCESSES` or decreasing :mod:`settings.ROOMBA_PROCESSES`
"""

ROOMBA_PROCESSES = 1
"""
:var ROOMBA_PROCESSES: This is the number of Roomba processes that will be
    spawned to trim timeseries in order to keep them at
    :mod:`settings.FULL_DURATION`. Keep this number small, as it is not
    important that metrics be exactly :mod:`settings.FULL_DURATION` all the time.
:vartype ROOMBA_PROCESSES: int
"""

ROOMBA_GRACE_TIME = 600
"""
:var ROOMBA_GRACE_TIME: Seconds grace
:vartype ROOMBA_GRACE_TIME:

Normally Roomba will clean up everything that is older than
:mod:`settings.FULL_DURATION` if you have metrics that are not coming in every
second, it can happen that you'll end up with INCOMPLETE metrics. With this
setting Roomba will clean up evertyhing that is older than
:mod:`settings.FULL_DURATION` + :mod:`settings.ROOMBA_GRACE_TIME`
"""

ROOMBA_TIMEOUT = 100
"""
:var ROOMBA_TIMEOUT: Timeout in seconds
:vartype ROOMBA_TIMEOUT: int

This is the number seconds that a Roomba process can be expected to run before
it is terminated.  Roomba should really be expected to have run within 100
seconds in general.  Roomba is done in a multiprocessing subprocess, however
there are certain conditions that could arise that could cause Roomba to
stall, I/O wait being one such edge case.  Although 99.999% of the time Roomba
is fine, this ensures that no Roombas hang around longer than expected.
"""

MAX_RESOLUTION = 1000
"""
:var MAX_RESOLUTION: The Horizon agent will ignore incoming datapoints if their
    timestamp is older than MAX_RESOLUTION seconds ago.
:vartype MAX_RESOLUTION: int
"""

SKIP_LIST = [
    # Skip anomaly_breakdown
    'anomaly_breakdown.',
    'example.statsd.metric',
    'another.example.metric',
    # if you use statsd, these can result in many near-equal series
    # '_90',
    # '.lower',
    # '.upper',
    # '.median',
    # '.count_ps',
    # '.sum',
]
"""
:var SKIP_LIST: Metrics to skip
:vartype SKIP_LIST: array

These are metrics that, for whatever reason, you do not want to store in
Skyline. The Worker will check to see if each incoming metrics contains
anything in the skip list. It is generally wise to skip entire namespaces by
adding a '.' at the end of the skipped item - otherwise you might skip things
you do not intend to.  For example the default
``skyline.analyzer.anomaly_breakdown.`` which MUST be skipped to prevent crazy
feedback.
"""

"""
Panorama settings
"""

PANORAMA_ENABLED = True
"""
:var PANORAMA_ENABLED: Enable Panorama
:vartype PANORAMA_ENABLED: boolean
"""

PANORAMA_PROCESSES = 1
"""
:var PANORAMA_PROCESSES: Number of processes to assign to Panorama, should never
    need more than 1
:vartype PANORAMA_ENABLED: int
"""

ENABLE_PANORAMA_DEBUG = False
"""
:var ENABLE_PANORAMA_DEBUG: DEVELOPMENT only - enables additional debug logging
    useful for development only, this should definitely be set to ``False`` on
    production system as LOTS of output
:vartype ENABLE_PANORAMA_DEBUG: boolean
"""

PANORAMA_DATABASE = 'skyline'
"""
:var PANORAMA_DATABASE: The database schema name
:vartype PANORAMA_DATABASE: str
"""

PANORAMA_DBHOST = '127.0.0.1'
"""
:var PANORAMA_DBHOST: The IP address or FQDN of the database server
:vartype PANORAMA_DBHOST: str
"""

PANORAMA_DBPORT = '3306'
"""
:var PANORAMA_DBPORT: The port to connet to the database server on
:vartype PANORAMA_DBPORT: str
"""

PANORAMA_DBUSER = 'skyline'

"""
:var PANORAMA_DBUSER: The database user
:vartype PANORAMA_DBUSER: str
"""

PANORAMA_DBUSERPASS = 'the_user_password'
"""
:var PANORAMA_DBUSERPASS: The database user password
:vartype PANORAMA_DBUSERPASS: str
"""

NUMBER_OF_ANOMALIES_TO_STORE_IN_PANORAMA = 0
"""
:var NUMBER_OF_ANOMALIES_TO_STORE_IN_PANORAMA: The number of anomalies to store
    in the Panaroma database, the default is 0 which means UNLIMITED.  This does
    nothing currently.
:vartype NUMBER_OF_ANOMALIES_TO_STORE_IN_PANORAMA: int
"""

PANORAMA_EXPIRY_TIME = 900
"""
:var PANORAMA_EXPIRY_TIME: Panorama will only store one anomaly for a metric \
    every PANORAMA_EXPIRY_TIME seconds.
:vartype PANORAMA_EXPIRY_TIME:

- This is the Panorama sample rate.  Please bear in mind Panorama does not use
  the ALERTS time expiry keys or matching, Panorama records every anomaly, even
  if the metric is not in an alert tuple.  Consider that a metric could and does
  often fire as anomalous every minute, until it no longer is.
"""


"""
Mirage settings
"""

MIRAGE_DATA_FOLDER = '/opt/skyline/mirage/data'
"""
:var MIRAGE_DATA_FOLDER: This is the path for the Mirage data folder where
    timeseries data that has been surfaced will be written - absolute path
:vartype MIRAGE_DATA_FOLDER: str
"""

MIRAGE_ALGORITHMS = [
    'first_hour_average',
    'mean_subtraction_cumulation',
    'stddev_from_average',
    'stddev_from_moving_average',
    'least_squares',
    'grubbs',
    'histogram_bins',
    'median_absolute_deviation',
    'ks_test',
]
"""
:var MIRAGE_ALGORITHMS: These are the algorithms that the Mirage will run.
:vartype MIRAGE_ALGORITHMS: array

To add a new algorithm, you must both define the algorithm in
``mirage/mirage_algorithms.py`` and add it's name here.
"""

MIRAGE_STALE_SECONDS = 120
"""
:var MIRAGE_STALE_SECONDS: The number of seconds after which a check is
    considered stale and discarded.
:vartype MIRAGE_STALE_SECONDS: int
"""

MIRAGE_CONSENSUS = 6
"""
:var MIRAGE_CONSENSUS: This is the number of algorithms that must return ``True``
    before a metric is classified as anomalous.
:vartype MIRAGE_CONSENSUS: int
"""

MIRAGE_ENABLE_SECOND_ORDER = False
"""
:var MIRAGE_ENABLE_SECOND_ORDER: This is to enable second order anomalies.
:vartype MIRAGE_ENABLE_SECOND_ORDER: boolean

.. warning:: EXPERIMENTAL - This is an experimental feature, so it's turned off
    by default.
"""

MIRAGE_ENABLE_ALERTS = False
"""
:var MIRAGE_ENABLE_ALERTS: This enables Mirage alerting.
:vartype MIRAGE_ENABLE_ALERTS: boolean
"""

NEGATE_ANALYZER_ALERTS = False
"""
:var NEGATE_ANALYZER_ALERTS: DEVELOPMENT only - negates Analyzer alerts
:vartype NEGATE_ANALYZER_ALERTS: boolean

This is to enables Mirage to negate Analyzer alerts.  Mirage will send out an
alert for every anomaly that Analyzer sends to Mirage that is NOT anomalous at
the ``SECOND_ORDER_RESOLUTION_HOURS`` with a ``SECOND_ORDER_RESOLUTION_HOURS``
graph and the Analyzer :mod:`settings.FULL_DURATION` graph embedded. Mostly for
testing and comparison of analysis at different time ranges and/or algorithms.
"""

MIRAGE_CRUCIBLE_ENABLED = False
"""
:var MIRAGE_CRUCIBLE_ENABLED: This enables Mirage to output to Crucible
:vartype MIRAGE_CRUCIBLE_ENABLED: boolean

This enables Mirage to send Crucible data, if this is set to ``True`` ensure
that :mod:`settings.CRUCIBLE_ENABLED` is also set to ``True`` in the Crucible
settings block.

.. warning:: Not recommended from production, will make a LOT of data files in
    the :mod:`settings.CRUCIBLE_DATA_FOLDER`
"""

"""
Boundary settings
"""

BOUNDARY_PROCESSES = 1
"""
:var BOUNDARY_PROCESSES: The number of processes that Boundary should spawn.
:vartype BOUNDARY_PROCESSES: int

Seeing as Boundary analysis is focused at specific metrics this should be less
than the number of :mod:`settings.ANALYZER_PROCESSES`.
"""

BOUNDARY_OPTIMUM_RUN_DURATION = 60
"""
:var BOUNDARY_OPTIMUM_RUN_DURATION: This is how many seconds it would be optimum
    for Boundary to be able to analyze your Boundary defined metrics in.
:vartype BOUNDARY_OPTIMUM_RUN_DURATION:

This largely depends on your metric resolution e.g. 1 datapoint per 60 seconds
and how many metrics you are running through Boundary.
"""

ENABLE_BOUNDARY_DEBUG = False
"""
:var ENABLE_BOUNDARY_DEBUG: Enables Boundary debug logging
:vartype ENABLE_BOUNDARY_DEBUG: boolean

- Enable additional debug logging - useful for development only, this should
  definitely be set to `False` on as production system - LOTS of output
"""

BOUNDARY_ALGORITHMS = [
    'detect_drop_off_cliff',
    'greater_than',
    'less_than',
]
"""
:var BOUNDARY_ALGORITHMS: Algorithms that Boundary can run
:vartype BOUNDARY_ALGORITHMS: array

- These are the algorithms that boundary can run. To add a new algorithm, you
  must both define the algorithm in boundary_algorithms.py and add its name here.
"""

BOUNDARY_ENABLE_ALERTS = False
"""
:var BOUNDARY_ENABLE_ALERTS: Enables Boundary alerting
:vartype BOUNDARY_ENABLE_ALERTS: boolean
"""

BOUNDARY_CRUCIBLE_ENABLED = False
"""
:var BOUNDARY_CRUCIBLE_ENABLED: Enables and disables Boundary pushing data to
    Crucible
:vartype BOUNDARY_CRUCIBLE_ENABLED: boolean

This enables Boundary to send Crucible data, if this is set to ``True`` ensure
that :mod:`settings.CRUCIBLE_ENABLED` is also set to ``True`` in the Crucible
settings block.

.. warning:: Not recommended from production, will make a LOT of data files in
    the :mod:`settings.CRUCIBLE_DATA_FOLDER`
"""

BOUNDARY_METRICS = (
    # ('metric', 'algorithm', EXPIRATION_TIME, MIN_AVERAGE, MIN_AVERAGE_SECONDS, TRIGGER_VALUE, ALERT_THRESHOLD, 'ALERT_VIAS'),
    ('skyline_test.alerters.test', 'greater_than', 1, 0, 0, 0, 1, 'smtp|hipchat|pagerduty'),
    ('metric1', 'detect_drop_off_cliff', 1800, 500, 3600, 0, 2, 'smtp'),
    ('metric2.either', 'less_than', 3600, 0, 0, 15, 2, 'smtp|hipchat'),
    ('nometric.other', 'greater_than', 3600, 0, 0, 100000, 1, 'smtp'),
)
"""
:var BOUNDARY_METRICS: definitions of metrics for Boundary to analyze
:vartype BOUNDARY_METRICS: tuple

This is the config for metrics to analyse with the boundary algorithms.
It is advisable that you only specify high rate metrics and global
metrics here, although the algoritms should work with low rate metrics, the
smaller the range, the smaller a cliff drop of change is, meaning more noise,
however some algorithms are pre-tuned to use different trigger values on
different ranges to pre-filter some noise.

- **Tuple schema**::

    BOUNDARY_METRICS = (
        ('metric1', 'algorithm1', EXPIRATION_TIME, MIN_AVERAGE, MIN_AVERAGE_SECONDS, TRIGGER_VALUE, ALERT_THRESHOLD, 'ALERT_VIAS'),
        ('metric2', 'algorithm2', EXPIRATION_TIME, MIN_AVERAGE, MIN_AVERAGE_SECONDS, TRIGGER_VALUE, ALERT_THRESHOLD, 'ALERT_VIAS'),
        # Wildcard namespaces can be used as well
        ('metric.thing.*.requests', 'algorithm1', EXPIRATION_TIME, MIN_AVERAGE, MIN_AVERAGE_SECONDS, TRIGGER_VALUE, ALERT_THRESHOLD, 'ALERT_VIAS'),
                        )

- Metric parameters (all are required):

:param metric: metric name.
:param algorithm: algorithm name.
:param EXPIRATION_TIME: Alerts will not fire twice within this amount of
    seconds, even if they trigger again.
:param MIN_AVERAGE: the minimum average value to evaluate for
    :func:`.boundary_algorithms.detect_drop_off_cliff`, in the
    :func:`.boundary_algorithms.less_than` and
    :func:`.boundary_algorithms.greater_than` algorithm contexts set
    this to 0.
:param MIN_AVERAGE_SECONDS: the seconds to calculate the minimum average value
    over in :func:`.boundary_algorithms.detect_drop_off_cliff`.  So if
    ``MIN_AVERAGE`` set to 100 and ``MIN_AVERAGE_SECONDS`` to 3600 a metric will
    only be analysed if the average value of the metric over 3600 seconds is
    greater than 100.  For the :func:`.boundary_algorithms.less_than`
    and :func:`.boundary_algorithms.greater_than` algorithms set this
    to 0.
:param TRIGGER_VALUE: then less_than or greater_than trigger value set to 0 for
    :func:`.boundary_algorithms.detect_drop_off_cliff`
:param ALERT_THRESHOLD: alert after detected x times.  This allows you to set
    how many times a timeseries has to be detected by the algorithm as anomalous
    before alerting on it.  The nature of distributed metric collection, storage
    and analysis can have a lag every now and then due to latency, I/O pause,
    etc.  Boundary algorithms can be sensitive to this not unexpectedly. This
    setting should be 1, maybe 2 maximum to ensure that signals are not being
    surpressed.  Try 1 if you are getting the occassional false positive, try 2.
    Note - Any :func:`.boundary_algorithms.greater_than` metrics should have
    this as 1.
:param ALERT_VIAS: pipe separated alerters to send to.
:type metric: str
:type algorithm: str
:type EXPIRATION_TIME: int
:type MIN_AVERAGE: int
:type MIN_AVERAGE_SECONDS: int
:type TRIGGER_VALUE: int
:type ALERT_THRESHOLD: int
:type ALERT_VIAS: str

- Wildcard and absolute metric paths.  Currently the only supported metric
  namespaces are a parent namespace and an absolute metric path e.g.

- **Examples**::

    ('stats_counts.someapp.things', 'detect_drop_off_cliff', 1800, 500, 3600, 0, 2, 'smtp'),
    ('stats_counts.someapp.things.an_important_thing.requests', 'detect_drop_off_cliff', 600, 100, 3600, 0, 2, 'smtp|pagerduty'),
    ('stats_counts.otherapp.things.*.requests', 'detect_drop_off_cliff', 600, 500, 3600, 0, 2, 'smtp|hipchat'),

- In the above all ``stats_counts.someapp.things*`` would be painted with a 1800
  ``EXPIRATION_TIME`` and 500 ``MIN_AVERAGE``, but those values would be
  overridden by 600 and 100 ``stats_counts.someapp.things.an_important_thing.requests``
  and pagerduty added.
"""

BOUNDARY_AUTOAGGRERATION = False
"""
:var BOUNDARY_AUTOAGGRERATION: Enables autoaggregate a timeseries
:vartype BOUNDARY_AUTOAGGRERATION: boolean

This is used to autoaggregate a timeseries with :func:`.autoaggregate_ts`, if a
timeseries dataset has 6 data points per minute but only one data value every
minute then autoaggregate can be used to aggregate the required sample.
"""

"""
:var :
:vartype :
"""

BOUNDARY_AUTOAGGRERATION_METRICS = (
    ('nometrics.either', 60),
)
"""
:var BOUNDARY_AUTOAGGRERATION_METRICS: The namespaces to autoaggregate
:vartype BOUNDARY_AUTOAGGRERATION_METRICS: tuples

- **Tuple schema example**::

    BOUNDARY_AUTOAGGRERATION_METRICS = (
        ('metric1', AGGREGATION_VALUE),
    )

- Metric tuple parameters are:

:param metric: metric name.
:param AGGREGATION_VALUE: alerter name.
:type metric: str
:type AGGREGATION_VALUE: int

Declare the namespace and aggregation value in seconds by which you want the
timeseries aggregated.  To aggregate a timeseries to minutely values use 60 as
the ``AGGREGATION_VALUE``, e.g. sum metric datapoints by minute
"""

BOUNDARY_ALERTER_OPTS = {
    # When an alert is sent as key is set with in the alert namespaces with an
    # expiration value
    'alerter_expiration_time': {
        'smtp': 60,
        'pagerduty': 1800,
        'hipchat': 1800,
    },
    # If alerter keys >= limit in the above alerter_expiration_time do not alert
    # to this channel until keys < limit
    'alerter_limit': {
        'smtp': 100,
        'pagerduty': 15,
        'hipchat': 30,
    },
}
"""
:var BOUNDARY_ALERTER_OPTS: Your Boundary alerter settings.
:vartype BOUNDARY_ALERTER_OPTS: dictionary

.. note:: Boundary Alerting
    Because you may want to alert multiple channels on each metric and algorithm,
    Boundary has its own alerting settings, similar to Analyzer.  However due to
    the nature of Boundary and it algorithms it could be VERY noisy and expensive
    if all your metrics dropped off a cliff.  So Boundary introduces alerting
    the ability to limit overall alerts to an alerter channel.  These limits use
    the same methodology that the alerts use, but each alerter is keyed too.
"""

BOUNDARY_SMTP_OPTS = {
    # This specifies the sender of email alerts.
    'sender': 'skyline-boundary@your_domain.com',
    # recipients is a dictionary mapping metric names
    # (exactly matching those listed in ALERTS) to an array of e-mail addresses
    'recipients': {
        'skyline_test.alerters.test': ['you@your_domain.com'],
        'nometrics': ['you@your_domain.com', 'them@your_domain.com'],
        'nometrics.either': ['you@your_domain.com', 'another@some-company.com'],
    },
    'embed-images': True,
    # Send graphite graphs at the most meaningful resolution if different from
    # FULL_DURATION
    'graphite_previous_hours': 7,
    'graphite_graph_line_color': 'pink',
}
"""
:var BOUNDARY_SMTP_OPTS: Your SMTP settings.
:vartype BOUNDARY_SMTP_OPTS: dictionary
"""

BOUNDARY_HIPCHAT_OPTS = {
    'auth_token': 'hipchat_auth_token',
    'sender': 'hostname or identifier',
    # list of hipchat room_ids to notify about each anomaly
    # (similar to SMTP_OPTS['recipients'])
    # Wildcard metric namespacing is allowed
    'rooms': {
        'skyline_test.alerters.test': (12345,),
        'nometrics': (12345,),
    },
    # Background color of hipchat messages
    # (One of 'yellow', 'red', 'green', 'purple', 'gray', or 'random'.)
    'color': 'purple',
    # Send graphite graphs at the most meaningful resolution if different from
    # FULL_DURATION
    'graphite_previous_hours': 7,
    'graphite_graph_line_color': 'pink',
}
"""
:var BOUNDARY_HIPCHAT_OPTS: Your Hipchat settings.
:vartype BOUNDARY_HIPCHAT_OPTS: dictionary

HipChat alerts require python-simple-hipchat
"""

# PagerDuty alerts require pygerduty
BOUNDARY_PAGERDUTY_OPTS = {
    # Your pagerduty subdomain and auth token
    'subdomain': 'example',
    'auth_token': 'your_pagerduty_auth_token',
    # Service API key (shown on the detail page of a 'Generic API' service)
    'key': 'your_pagerduty_service_api_key',
}
"""
:var BOUNDARY_PAGERDUTY_OPTS: Your SMTP settings.
:vartype BOUNDARY_PAGERDUTY_OPTS: dictionary

PagerDuty alerts require pygerduty
"""

"""
Crucible settings
"""

ENABLE_CRUCIBLE = True
"""
:var ENABLE_CRUCIBLE: Enable Crucible.
:vartype ENABLE_CRUCIBLE: boolean
"""

# This is the number of processes that crucible will spawn.
CRUCIBLE_PROCESSES = 1
"""
:var CRUCIBLE_PROCESSES: The number of processes that Crucible should spawn.
:vartype CRUCIBLE_PROCESSES: int
"""

CRUCIBLE_TESTS_TIMEOUT = 60
"""
:var CRUCIBLE_TESTS_TIMEOUT: # This is the number of seconds that Crucible tests
    can take. 60 is a reasonable default for a run with a
    :mod:`settings.FULL_DURATION` of 86400
:vartype CRUCIBLE_TESTS_TIMEOUT: int
"""

ENABLE_CRUCIBLE_DEBUG = False
"""
:var ENABLE_CRUCIBLE_DEBUG: DEVELOPMENT only - enables additional debug logging
    useful for development only, this should definitely be set to ``False`` on
    production system as LOTS of output
:vartype ENABLE_CRUCIBLE_DEBUG: boolean
"""

CRUCIBLE_DATA_FOLDER = '/opt/skyline/crucible/data'
"""
:var CRUCIBLE_DATA_FOLDER: This is the path for the Crucible data folder where
    anomaly data for timeseries will be stored - absolute path
:vartype CRUCIBLE_DATA_FOLDER: str
"""

"""
Webapp settings
"""

WEBAPP_SERVER = 'gunicorn'
"""
:var WEBAPP_SERVER: Run the Webapp via gunicorn (recommended) or the Flask
    development server, set this to either ``'gunicorn'`` or ``'flask'``
:vartype WEBAPP_SERVER: str
"""

WEBAPP_IP = '127.0.0.1'
"""
:var WEBAPP_IP: The IP address for the Webapp to bind to
:vartype WEBAPP_IP: str
"""

WEBAPP_PORT = 1500
"""
:var WEBAPP_PORT: The port for the Webapp to listen on
:vartype WEBAPP_PORT: int
"""

# These Webapp security settings attempt to ensure that the system is not
# totally open by default, but totally restricted by default.  This adds a bit
# of defence in depth and hopefully will mitigate against unauthorised access in
# the event that there was a firewall misconfiguration.
# THIS DOES NOT replace the need to restrict access to the WEBAPP_PORT with
# proper firewall rules.

WEBAPP_AUTH_ENABLED = True
"""
:var WEBAPP_AUTH_ENABLED: To enable pseudo basic HTTP auth
:vartype WEBAPP_AUTH_ENABLED: boolean
"""

WEBAPP_AUTH_USER = 'admin'
"""
:var WEBAPP_AUTH_USER: The username for pseudo basic HTTP auth
:vartype WEBAPP_AUTH_USER: str
"""

WEBAPP_AUTH_USER_PASSWORD = 'aec9ffb075f9443c8e8f23c4f2d06faa'
"""
:var WEBAPP_AUTH_USER_PASSWORD: The user password for pseudo basic HTTP auth
:vartype WEBAPP_AUTH_USER_PASSWORD: str
"""

WEBAPP_IP_RESTRICTED = True
"""
:var WEBAPP_IP_RESTRICTED: To enable restricted access from IP address declared
    in :mod:`settings.WEBAPP_ALLOWED_IPS`
:vartype WEBAPP_IP_RESTRICTED: boolean
"""

WEBAPP_ALLOWED_IPS = ['127.0.0.1']
"""
:var WEBAPP_ALLOWED_IPS: The allowed IP addresses
:vartype WEBAPP_ALLOWED_IPS: array
"""
