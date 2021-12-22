"""
Shared settings

IMPORTANT NOTE:

These settings are described with docstrings for the purpose of automated
documentation.  You may find reading some of the docstrings easier to understand
or read in the documentation itself.

http://earthgecko-skyline.readthedocs.io/en/latest/skyline.html#module-settings

"""

REDIS_SOCKET_PATH = '/tmp/redis.sock'
"""
:var REDIS_SOCKET_PATH: The path for the Redis unix socket [USER_DEFINED]
:vartype REDIS_SOCKET_PATH: str
"""

# REDIS_PASSWORD = 'DO.PLEASE.set.A.VERY.VERY.LONG.REDIS.password-time(now-1+before_you_forget)'
REDIS_PASSWORD = None
"""
:var REDIS_PASSWORD: The password for Redis, even though Skyline uses socket it
    is still advisable to set a password for Redis.  If this is set to the
    boolean None Skyline will not use Redis AUTH [USER_DEFINED]
:vartype REDIS_PASSWORD: str

.. note:: Please ensure that you do enable Redis authentication by setting the
    requirepass in your redis.conf with a very long Redis password.  See
    https://redis.io/topics/security and http://antirez.com/news/96 for more
    info.

"""

SECRET_KEY = 'your-long_secret-key-to-encrypt_the_redis_password_in_url_parameters'
"""
:var SECRET_KEY: A secret key that is used to encrypt the Redis password in the
    rebrow URL parameters.
:vartype SECRET_KEY: str
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

SKYLINE_DIR = '/opt/skyline'
"""
:var SKYLINE_DIR: The Skyline dir. All other Skyline.
:vartype SKYLINE_DIR: str
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
:var FULL_NAMESPACE: Metrics must be prefixed with this value in Redis.
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
    anomaly detection.  However, Mirage handles longer durations so ideally this
    should be 86400.
:vartype FULL_DURATION: str
"""

MINI_DURATION = 3600
"""
:var MINI_DURATION: This is the duration of the 'mini' namespace, if you are
    also using the Oculus service. It is also the duration of data that is
    displayed in the Webapp 'mini' view.
:vartype MINI_DURATION: str
"""

VERIFY_SSL = True
"""
:var VERIFY_SSL: Whether to verify SSL certificates requestsed endpoints.  By
    defualt this is True, however this can be set to False to allow for the use
    of self signed SSL certificates.
:vartype VERIFY_SSL: boolean
"""

GRAPHITE_AUTH_HEADER = False
"""
:var GRAPHITE_AUTH_HEADER: the Authorization header for Graphite api
:vartype GRAPHITE_AUTH_HEADER: str
"""

GRAPHITE_CUSTOM_HEADERS = {}
"""
:var GRAPHITE_CUSTOM_HEADERS: Dictionary with custom headers
:vartype GRAPHITE_CUSTOM_HEADERS: dict
"""

GRAPHITE_HOST = 'YOUR_GRAPHITE_HOST.example.com'
"""
:var GRAPHITE_HOST: If you have a Graphite host set up, set this variable to get
    graphs on Skyline and Horizon. Don't include http:// since this can be used
    for CARBON_HOST as well.  [USER_DEFINED]
:vartype GRAPHITE_HOST: str
"""

GRAPHITE_PROTOCOL = 'http'
"""
:var GRAPHITE_PROTOCOL: Graphite host protocol - http or https [USER_DEFINED]
:vartype GRAPHITE_PROTOCOL: str
"""

GRAPHITE_PORT = '80'
"""
:var GRAPHITE_PORT: Graphite host port - for a specific port if graphite runs on
    a port other than 80, e.g. '8888' [USER_DEFINED]
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

GRAPHITE_RENDER_URI = 'render'
"""
:var GRAPHITE_RENDER_URI: Base URI for graphite render, this can generally be
    render, api/datasources/1/render,  api/datasources/proxy/1/render, etc,
    depending on how your Graphite (or grafana proxy) is set up.
:vartype GRAPHITE_RENDER_URI: str
"""

GRAPH_URL = GRAPHITE_PROTOCOL + '://' + GRAPHITE_HOST + ':' + GRAPHITE_PORT + '/' + GRAPHITE_RENDER_URI + '?width=1400&from=-' + TARGET_HOURS + 'hour&target='
"""
:var GRAPH_URL: The graphite URL for alert graphs will be appended with the
    relevant metric name in each alert.
:vartype GRAPH_URL: str

.. note:: There is probably no neeed to change this unless you what a different
    size graph sent with alerts.
"""

CARBON_HOST = GRAPHITE_HOST
"""
:var CARBON_HOST: endpoint to send metrics that should reach graphite if
    the CARBON_HOST is a different host to the GRAPHITE_HOST set it here.
:vartype CARBON_HOST: str
"""

CARBON_PORT = 2003
"""
:var CARBON_PORT: If you have a Graphite host set up, set its Carbon port.
    [USER_DEFINED]
:vartype CARBON_PORT: int
"""

SKYLINE_METRICS_CARBON_HOST = GRAPHITE_HOST
"""
:var SKYLINE_METRICS_CARBON_HOST: If you want to send the Skyline metrics to
    a different host other that the GRAPHITE_HOST, declare it here and see the
    SKYLINE_METRICS_CARBON_PORT setting below.
:vartype SKYLINE_METRICS_CARBON_HOST: str
"""

SKYLINE_METRICS_CARBON_PORT = CARBON_PORT
"""
:var SKYLINE_METRICS_CARBON_PORT: If you want to send the Skyline metrics to
    a different SKYLINE_METRICS_CARBON_HOST host other than the GRAPHITE_HOST
    and it has a different port to the CARBON_PORT, declare it here.
:vartype SKYLINE_METRICS_CARBON_PORT: int
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
:var SERVER_METRICS_NAME: The hostname of the Skyline. [USER_DEFINED]
:vartype SERVER_METRICS_NAME: str

- This is to allow for multiple Skyline nodes to send metrics to a Graphite
  instance on the Skyline namespace sharded by this setting, like carbon.relays.
  If you want multiple Skyline hosts, set the hostname of the skyline here and
  metrics will be as e.g. ``skyline.analyzer.skyline-01.run_time``
"""

SKYLINE_FEEDBACK_NAMESPACES = [SERVER_METRICS_NAME, GRAPHITE_HOST]
"""
:var SKYLINE_FEEDBACK_NAMESPACES: This is a list of namespaces that can cause
    feedback in Skyline.  If you are analysing the system metrics of the Skyline
    host (server or container), then if a lot of metrics become anomalous, the
    Skyline host/s are going to be working much more and pulling more data from
    the GRAPHITE_HOST, the Skyline mysql database metrics and Redis queries
    will all change substantially too.  Although Skyline can be trained and
    learn them, when Skyline is in a known busy state, the monitoring of its
    own metrics and related metrics should take 2nd priority.  In fact when the
    ionosphere_busy state is determined, Skyline will rate limit the analysis of
    any metrics in the namespaces declared here.
    This list works in the same way that Horizon SKIP_LIST does, it matches in
    the string or dotted namespace elements. [USER_DEFINED]
    **For example**::

        SKYLINE_FEEDBACK_NAMESPACES = [
            SERVER_METRICS_NAME,
            'stats.skyline-docker-graphite-statsd-1',
            'stats.skyline-mysql']

:vartype SKYLINE_FEEDBACK_NAMESPACES: list
"""

DO_NOT_SKIP_SKYLINE_FEEDBACK_NAMESPACES = []
"""
:var DO_NOT_SKIP_SKYLINE_FEEDBACK_NAMESPACES: This is a list of namespaces or metrics
    that may be in the SKYLINE_FEEDBACK_NAMESPACES that you DO NOT want to skip
    as feedback metrics but always want analysed.  [USER_DEFINED]
    Metrics will be evaluated against namespaces in this list using
    :func:`.matched_or_regexed_in_list` which determines if a pattern is in a
    list as a:
    1) absolute match
    2) match by dotted elements
    3) matched by a regex

    **For example**::

        DO_NOT_SKIP_SKYLINE_FEEDBACK_NAMESPACES = [
            'nginx',
            'disk.used_percent',
            'system.load15'
        ]

:vartype DO_NOT_SKIP_SKYLINE_FEEDBACK_NAMESPACES: list
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

PANORAMA_CHECK_PATH = '/opt/skyline/panorama/check'
"""
:var PANORAMA_CHECK_PATH: This is the location the Skyline apps will write the
    anomalies to for Panorama to check to a file on disk - absolute path
:vartype PANORAMA_CHECK_PATH: str
"""

DATA_UPLOADS_PATH = '/tmp/skyline/data_uploads'
"""
:var DATA_UPLOADS_PATH: The path that webapp writes uploaded data to and flux
    checks for data to process.  Note the parent directory must be writable to
    the user that the Skyline processes are running as.  This is related to the
    :mod:`settings.FLUX_PROCESS_UPLOADS` and
    :mod:`settings.WEBAPP_ACCEPT_DATA_UPLOADS` settings.
:vartype DATA_UPLOADS_PATH: str
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
    will necessarily want all 3rd party alerters.  Enable what 3rd alerters you
    require here.   This enables only the alerters that are required to be
    imported and means that not all alerter related modules in
    ``requirements.txt`` have to be installed, only those you require.
"""

SYSLOG_ENABLED = True
"""
:var SYSLOG_ENABLED: enables Skyline apps to submit anomalous metric
    details to syslog.  Being set to True makes syslog a kind of alerter, like a
    SMTP alert. It also results in all anomalies being recorded in the database
    by Panorama and this is the desired default.
:vartype SYSLOG_ENABLED: boolean
"""

HIPCHAT_ENABLED = False
"""
:var HIPCHAT_ENABLED: [DEPRECATED] Enables the Hipchat alerter
:vartype HIPCHAT_ENABLED: boolean
"""

PAGERDUTY_ENABLED = False
"""
:var PAGERDUTY_ENABLED: Enables the Pagerduty alerter [USER_DEFINED]
:vartype PAGERDUTY_ENABLED: boolean
"""

SLACK_ENABLED = False
"""
:var SLACK_ENABLED: Enables the Slack alerter [USER_DEFINED]
:vartype SLACK_ENABLED: boolean
"""

HTTP_ALERTERS_ENABLED = False
"""
:var HTTP_ALERTERS_ENABLED: Enables the http alerter
:vartype HTTP_ALERTERS_ENABLED: boolean
"""

START_IF_NO_DB = False
"""
:var START_IF_NO_DB: This allows the Skyline apps to start if there is a DB
    issue and/or the DB is not available.  By default the apps will not start if
    the DB is not available, but this is useful for testing and allowing the
    Skyline apps to continue to function and alert, even if it is in a limited
    fashion and defualts to nosiy 3-sigma alerting.
:vartype START_IF_NO_DB: boolean
"""

"""
Analyzer settings
"""

ANALYZER_ENABLED = True
"""
:var ANALYZER_ENABLED: This enables analysis via Analyzer.  For ADVANCED
    configurations only.
    If this is set to False, the Analyzer process can still be started and will
    process the metrics in the pipeline but it will NOT analyse them, therefore
    there will be no alerting, no feeding Mirage, etc.  Analyzer will simply run
    as if there are 0 metrcis.  This allows for an advanced modular set up for
    running multiple distributed Skyline instance.
:vartype ANALYZER_ENABLED: boolean
"""

ANALYZER_VERBOSE_LOGGING = True
"""
:var ANALYZER_VERBOSE_LOGGING: As of Skyline 3.0, apps log notice and errors
    only.  To have addtional info logged set this to True.  Useful for debugging
    but less verbose than LOCAL_DEBUG.
:vartype ANALYZER_VERBOSE_LOGGING: boolean
"""

ANOMALY_DUMP = 'webapp/static/dump/anomalies.json'
"""
:var ANOMALY_DUMP: This is the location the Skyline agent will write the
    anomalies file to disk.  It needs to be in a location accessible to the
    webapp.
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

CUSTOM_STALE_PERIOD = {}
"""
:var CUSTOM_STALE_PERIOD: This enables overriding the :mod:`settings.STALE_PERIOD`
    per metric namespace to become 'stale' and for the analyzer/boundary to
    ignore it until new datapoints are added.  'Staleness' means that a
    datapoint has not been added for the defined number of seconds.  The
    namespaces can be absolute metric names, substrings (dotted elements) of a
    namespace or a regex of a namespace.  First match wins so ensure metrics
    are not defined by multiple entries.
:vartype CUSTOM_STALE_PERIOD: dict

- **Example**::

    CUSTOM_STALE_PERIOD = {
        'cswellsurf.buoys': 7200,
    }

"""

ALERT_ON_STALE_METRICS = True
"""
:var ALERT_ON_STALE_METRICS: Send a digest alert of all metrics that stop
    populating their time series data.
:vartype ALERT_ON_STALE_METRICS: boolean
"""

ALERT_ON_STALE_PERIOD = 300
"""
:var ALERT_ON_STALE_PERIOD: This is the duration, in seconds, after which an
    alert will be sent for a metric if it stops sending data.  The digest alert
    will only occur once while in the window between the ALERT_ON_STALE_PERIOD
    and the STALE_PERIOD.
:vartype ALERT_ON_STALE_PERIOD: int
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

IDENTIFY_AIRGAPS = False
"""
:var IDENTIFY_AIRGAPS: Identify metrics which have air gaps and publish to the
    analyzer.airgapped_metrics Redis set which is exposed via the webapp on
    /api?airgapped_metrics [ADVANCED FEATURE]
:vartype IDENTIFY_AIRGAPS: boolean

- Note that the implementation of this feature has a computational cost and will
  increase analyzer.run_time and CPU usage.  If you are not back filling airgaps
  by via Flux or directly to carbon-relay DO NOT enable this.
- Enabling this also enables the IDENTIFY_UNORDERED_TIMESERIES features which is
  a part of the IDENTIFY_AIRGAPS functionality.
- If you do enable this, consider specifying specific metrics and/or namespaces
  in CHECK_AIRGAPS below to limit only checking metrics which will be back
  filled and not all your metrics.

"""

MAX_AIRGAP_PERIOD = 21600
"""
:var MAX_AIRGAP_PERIOD: If IDENTIFY_AIRGAPS is enabled Analyzer will only flag
    metric that have any air gaps in the last MAX_AIRGAP_PERIOD seconds as
    air gapped.
:vartype MAX_AIRGAP_PERIOD: int
"""

CHECK_AIRGAPS = []
"""
:var CHECK_AIRGAPS: If set to [] ALL metrics will be check.  List metrics and
    namespaces that you explicitly want to identify airgaps in, this is only
    applicable if you have IDENTIFY_AIRGAPS enabled.  If metrics and/or
    namespaces are listed here ONLY these will be checked.
:vartype CHECK_AIRGAPS: list

Seeing as IDENTIFY_AIRGAPS can be computationally expensive, this allows you to
only check specific metrics for airgaps.

The CHECK_AIRGAPS are also matched just dotted namespace elements too, if a match
is not found in the string, then the dotted elements are compared.  For example
if an item such as 'skyline.analyzer.algorithm_breakdown' was added it would
macth any metric that matched all 3 dotted namespace elements, so it would
match:

skyline.analyzer.skyline-1.algorithm_breakdown.histogram_bins.timing.median_time
skyline.analyzer.skyline-1.algorithm_breakdown.histogram_bins.timing.times_run
skyline.analyzer.skyline-1.algorithm_breakdown.ks_test.timing.times_run

Example:

    CHECK_AIRGAPS = [
        'remote_hosts',
        'external_hosts.dc1',
    ]


"""

SKIP_AIRGAPS = []
"""
:var SKIP_AIRGAPS: List metrics that you you do not want to identify airgaps in,
    this is only applicable if you have IDENTIFY_AIRGAPS enabled.
:vartype SKIP_AIRGAPS: list

These are metrics that, for whatever reason, you do not want to check to see if
any airgaps are present.

The SKIP_AIRGAPS are also matched just dotted namespace elements too, if a match
is not found in the string, then the dotted elements are compared.  For example
if an item such as 'skyline.analyzer.algorithm_breakdown' was added it would
macth any metric that matched all 3 dotted namespace elements, so it would
match:

skyline.analyzer.skyline-1.algorithm_breakdown.histogram_bins.timing.median_time
skyline.analyzer.skyline-1.algorithm_breakdown.histogram_bins.timing.times_run
skyline.analyzer.skyline-1.algorithm_breakdown.ks_test.timing.times_run

Example:

    SKIP_AIRGAPS = [
        'carbon',
        'skyline',
        'stats',
    ]

"""

IDENTIFY_UNORDERED_TIMESERIES = False
"""
:var IDENTIFY_UNORDERED_TIMESERIES: Identify metrics that are not ordered
    correctly via time stamps for Analyzer to sort and deduplicate and recreate
    the Redis metric data with the correctly sorted time series data, in a
    manner as to not lose any data.
:vartype IDENTIFY_UNORDERED_TIMESERIES: boolean

- Note that the implementation of this feature has a small computational cost.
  If enabled it uses a small part of the IDENTIFY_AIRGAPS feature described
  above.
- If IDENTIFY_AIRGAPS is enabled this is enabled by default, even if
  IDENTIFY_UNORDERED_TIMESERIES = False
- This was introduced as external sources sending metrics via Flux could send
  metric data out of order, which Graphite will handle but will pickle to
  Horizon and make the metric Redis data unordered.  This definitely occurs if a
  metric is back filled via Flux or directly to carbon-relay.  Although Flux
  identifies these metrics for Analyzer via flux.filled Redis keys, Analyzer can
  undertake this check on its own to handle any cases where for whatever reason
  any metric data becomes unordered, even if IDENTIFY_AIRGAPS is not enabled.
  [ADVANCED FEATURE]

"""

# @added 20201209 - Feature #3870: metrics_manager - check_data_sparsity
CHECK_DATA_SPARSITY = True
"""
:var CHECK_DATA_SPARSITY: ADVANCED FEATURE - in Analyzer metrics_manager
    determine how many metrics are fully populated (good), becoming increasingly
    sparsely populated (bad, not receiving data), becoming more densely populated
    (good) and the average data sparsity for the entire metric population.
:vartype CHECK_DATA_SPARSITY: boolen
"""

SKIP_CHECK_DATA_SPARSITY_NAMESPACES = []
"""
:var SKIP_CHECK_DATA_SPARSITY_NAMESPACES: ADVANCED FEATURE - if there are
    metrics in population that you expect to not send data all the time you can
    declare the namespaces if you do not want them influencing the data sparsity
    measurements.  This is a list of absolute metric names, substrings (dotted
    elements) of a namespace or a regex of a namespace
:vartype SKIP_CHECK_DATA_SPARSITY_NAMESPACES: list
"""

FULLY_POPULATED_PERCENTAGE = 94.0
"""
:var FULLY_POPULATED_PERCENTAGE: ADVANCED FEATURE - the percent of data
    points required in a time series for it to be considered as fully populated
    at :mod:`settings.FULL_DURATION`.  Any time series with more than this is
    considered fully populated.  Skyline calculates this value based on the
    metric resolution/frequency from the time series data that Skyline
    automatically calculates.
:vartype FULLY_POPULATED_PERCENTAGE: float
"""

SPARSELY_POPULATED_PERCENTAGE = 40.0
"""
:var SPARSELY_POPULATED_PERCENTAGE: ADVANCED FEATURE - the percent of data
    points required in a time series for it to be considered as sparse populated
    at :mod:`settings.FULL_DURATION`.  Any time series with less than this is
    considered sparsely populate.  Skyline calculates this value based on the
    metric resolution/frequency from the time series data that Skyline
    automatically calculates.
:vartype SPARSELY_POPULATED_PERCENTAGE: float
"""

# @added 20201212 - Feature #3884: ANALYZER_CHECK_LAST_TIMESTAMP
ANALYZER_CHECK_LAST_TIMESTAMP = False
"""
:var ANALYZER_CHECK_LAST_TIMESTAMP: ADVANCED FEATURE - whether to make Analyzer
    record the last analyzed timestamp per metric and only submit the metric to
    be analysed if it has a new timestamp for the last (or is stale).  Where
    high frequency metrics are used, this is generally not required, however it
    is useful and can substantially reduce the amount of analysis run if you
    have lots of lower frequency metrics.
:vartype BATCH_PROCESSING: boolen
"""

# @added 20200411 - Feature #3480: batch_processing
BATCH_PROCESSING = None
"""
:var BATCH_PROCESSING: Whether to apply batch processing to metrics which are
    recieved in batches.  In general this should not be enabled for all metrics
    as it significantly increases the computational footprint and increases
    memory use and calls.  It should only be enabled if you have metrics that
    are receieved in infrequent batches, metrics feed per minute do not require
    batch processing.  For example if a metric/s are sent to Skyline every 15
    minutes with a data point for each minute in the period, Analyzer's default
    analysis would only analyse the latest data point against the metric time
    series data.  With batch processing, Analyzer identifies batched metrics and
    when a batch of data is received Analyzer sends the metric/s to analyzer_batch
    to analyse.  To ensure that this can be achieved as computationally cheap as
    possible the BATCH_PROCESSING_NAMESPACES list can be applied, to reduce the
    footprint of this functionality.  ADVANCED FEATURE
:vartype BATCH_PROCESSING: boolen
"""

BATCH_PROCESSING_STALE_PERIOD = 86400
"""
:var BATCH_PROCESSING_STALE_PERIOD: This is the duration, in seconds, for a
    metric to be deemed as stale and for the analyzer_batch to ignore it until
    new datapoints are added.  ADVANCED FEATURE
:vartype BATCH_PROCESSING_STALE_PERIOD: int
"""

BATCH_PROCESSING_DEBUG = None
"""
:var BATCH_PROCESSING_DEBUG: Whether to log batch processing info from Analyzer.
:vartype BATCH_PROCESSING_DEBUG: boolen
"""

BATCH_PROCESSING_NAMESPACES = []
"""
:var BATCH_PROCESSING_NAMESPACES: If BATCH_PROCESSING is eanbled to reduce the
    computational footprint of batch processing metric time series data, a list
    of metric namespaces which can be expected to be batch processed can be
    defined so that BATCH_PROCESSING keys, checks and resources are not applied
    to all metrics.  This list works in the same way that SKIP_LIST does, it
    matches in the string or dotted namespace elements. ADVANCED FEATURE
:vartype BATCH_PROCESSING_NAMESPACES: list
"""

METRICS_INACTIVE_AFTER = FULL_DURATION - 3600
"""
:var METRICS_INACTIVE_AFTER: Identify metrics as inactive after the defined
    seconds.
:vartype METRICS_INACTIVE_AFTER: int
"""

CANARY_METRIC = 'statsd.numStats'
"""
:var CANARY_METRIC: The metric name to use as the CANARY_METRIC [USER_DEFINED]
:vartype CANARY_METRIC: str

- The canary metric should be a metric with a very high, reliable resolution
  that you can use to gauge the status of the system as a whole.  Like the
  ``statsd.numStats`` or a metric in the ``carbon.`` namespace or a Skyline
  metric like:
  CANARY_METRIC = 'skyline.%s.analyzer.run_time' % SERVER_METRICS_NAME
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
:vartype ALGORITHMS: list
"""

CONSENSUS = 6
"""
:var CONSENSUS: This is the number of algorithms that must return True before a
    metric is classified as anomalous by Analyzer.
:vartype CONSENSUS: int
"""

ANALYZER_ANALYZE_LOW_PRIORITY_METRICS = True
"""
:var ANALYZER_ANALYZE_LOW_PRIORITY_METRICS: By default all low priority metrics
    are analysed normally like high priority metrics.  For any type of analysis
    to be run against low priority metrics this must be set to True.  Setting
    this value to False disables ALL analysis of low priority metrics, they are
    simply skipped (except in Luminosity correlations).  To configure low
    priority metrics with any of the below LOW_PRIORITY_METRICS settings, this
    value must be set to True.  ADVANCED FEATURE.  See
    https://earthgecko-skyline.readthedocs.io/en/latest/analyzer.html#high-and-low-priority-metrics
:vartype ANALYZER_ANALYZE_LOW_PRIORITY_METRICS: boolean
"""

ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS = False
"""
:var ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS: ADVANCED FEATURE. This
    mode will attempt to dynamically analyse as many low priority metrics as
    possible in the available time, looping through the metrics on a best effort
    basis to analyse low priority metrics as frequently as possible without
    causing lag on the analysis of high priority metrics.  ADVANCED FEATURE
:vartype ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS: boolean
"""

ANALYZER_MAD_LOW_PRIORITY_METRICS = 0
"""
:var ANALYZER_MAD_LOW_PRIORITY_METRICS: ADVANCED FEATURE. This is the number of
    data points on which to calculate MAD against to determine if a low priority
    metric should be analysed via the three-sigma algorithms.  The default value
    of 0 disables this feature.  If set, this should not be greater than 15 at
    the most as it will result in a performance loss, see
    https://earthgecko-skyline.readthedocs.io/en/latest/analyzer.html#ANALYZER_MAD_LOW_PRIORITY_METRICS
    Note if ANALYZER_DYNAMICALLY_ANALYZE_LOW_PRIORITY_METRICS is set to True,
    ANALYZER_MAD_LOW_PRIORITY_METRICS will be automatically set to 10 if it is
    set to 0 here.  ADVANCED FEATURE
:vartype ANALYZER_MAD_LOW_PRIORITY_METRICS: int
"""

ANALYZER_SKIP = []
"""
:var ANALYZER_SKIP: Namespaces to not analyse.  These are metrics that you do
    not want Analyzer to analyse.  It allows for disabling the analysis on
    certain namespaces.  Works in the same way as SKIP_LIST works, it matches in
    the string or dotted namespace elements.  If you never want to analyse a
    namespace rather add it to SKIP_LIST.  ANALYZER_SKIP is more suited to allow
    for temporarily disabling analysis on a namespace but the metrics time
    series data still gets submitted to Redis.  Whereas metrics in SKIP_LIST
    just get dropped.
:vartype ANALYZER_SKIP: list
"""

CUSTOM_ALGORITHMS = {}
"""
:var CUSTOM_ALGORITHMS: Custom algorithms to run.  An empty dict {} disables
    this feature. ADVANCED FEATURE.
:vartype CUSTOM_ALGORITHMS: dict

- For full documentation see https://earthgecko-skyline.readthedocs.io/en/latest/algorithms/custom-algorithms.html
- **CUSTOM_ALGORITHMS example**::

    CUSTOM_ALGORITHMS = {
        'abs_stddev_from_median': {
            'namespaces': ['telegraf.cpu-total.cpu.usage_system'],
            'algorithm_source': '/opt/skyline/github/skyline/skyline/custom_algorithms/abs_stddev_from_median.py',
            'algorithm_parameters': {},
            'max_execution_time': 0.09,
            'consensus': 6,
            'algorithms_allowed_in_consensus': [],
            'run_3sigma_algorithms': True,
            'run_before_3sigma': True,
            'run_only_if_consensus': False,
            'trigger_history_override': 0,
            'use_with': ['analyzer', 'analyzer_batch', 'mirage', 'crucible'],
            'debug_logging': False,
        },
        'last_same_hours': {
            'namespaces': ['telegraf.cpu-total.cpu.usage_user'],
            'algorithm_source': '/opt/skyline/github/skyline/skyline/custom_algorithms/last_same_hours.py',
            # Pass the argument 1209600 for the sample_period parameter and
            # enable debug_logging in the algorithm itself
            'algorithm_parameters': {
              'sample_period': 604800,
              'debug_logging': True
            },
            'max_execution_time': 0.3,
            'consensus': 6,
            'algorithms_allowed_in_consensus': [],
            'run_3sigma_algorithms': True,
            'run_before_3sigma': True,
            'run_only_if_consensus': False,
            'trigger_history_override': 0,
            # This does not run on analyzer as it is weekly data
            'use_with': ['mirage', 'crucible'],
            'debug_logging': False,
        },
        'detect_significant_change': {
            'namespaces': ['swell.buoy.*.Hm0'],
            # Algorithm source not in the Skyline code directory
            'algorithm_source': '/opt/skyline_custom_algorithms/detect_significant_change/detect_significant_change.py',
            'algorithm_parameters': {},
            'max_execution_time': 0.002,
            'consensus': 1,
            'algorithms_allowed_in_consensus': ['detect_significant_change'],
            'run_3sigma_algorithms': False,
            'run_before_3sigma': True,
            'run_only_if_consensus': False,
            'trigger_history_override': 0,
            'use_with': ['analyzer', 'crucible'],
            'debug_logging': True,
        },
        'skyline_matrixprofile': {
            'namespaces': ["telegraf",],
            'algorithm_source': '/opt/skyline/github/skyline/skyline/custom_algorithms/skyline_matrixprofile.py',
            'algorithm_parameters': {'windows': 5, 'k_discords': 20},
            'max_execution_time': 5.0,
            'consensus': 1,
            'algorithms_allowed_in_consensus': ["skyline_matrixprofile"],
            'run_3sigma_algorithms': True,
            'run_before_3sigma': False,
            'run_only_if_consensus': True,
            'trigger_history_override': 4,
            'use_with': ["mirage"],
            'debug_logging': False,
        },
    }

- Each dictionary item needs to be named the same as the algorithm to be run
- CUSTOM_ALGORITHMS dictionary keys and values are:

:param namespaces: a list of absolute metric names, substrings (dotted elements)
    of a namespace or a regex of a namespace
:param algorithm_source: the full path to the custom algorithm Python file
:param algorithm_parameters: a dictionary of any parameters to pass to the
    custom algorithm
:param max_execution_time: the maximum time the algorithm should run, any longer
    than this value and the algorithm process will be timed out and terminated.
    Bear in mind that algorithms have to run FAST, otherwise analysis stops
    being real time and the Skyline apps will terminate their own spawned
    processes.  Consider that Skyline's 3 sigma algorithms take on average
    0.0023 seconds to run and all 9 are run on a metric in about 0.0207 seconds.
:param consensus: The number of algorithms that need to trigger, including this
    one for a data point to be classed as anomalous, you can declare the same as
    the :mod:`settings.CONSENSUS` value or that +1 or simply 1 if you want as
    anomaly triggered because the custom algorithm triggered.
:param algorithms_allowed_in_consensus: this is not implemented yet and is
    optional
:param run_3sigma_algorithms: a boolean stating whether to run normal 3 sigma
    algorithms, this is optional and defaults to ``True`` if it is not passed
    in the dictionary.  Read the full documentation referred to above to
    determine the effects of passing this as ``False``.
: param run_before_3sigma: a boolean stating whether to run the custom algorithm
    before the normal three-sigma algorithms, this defaults to ``True``.  If you
    want your custom algorithm to run after the three-sigma algorithms set this to
    ``False``.  Read the full documentation referred to above to determine the
    effects of passing this as ``False``.
:param run_only_if_consensus: a boolean stating whether to run the custom
    algorithm only if CONSENSUS or MIRAGE_CONSENSUS is achieved, it defaults to
    ``False``.  This only applies to custom algorithms that are run after
    three-sigma algorithms, e.g. with the parameter ``run_before_3sigma: False``
    Currently this parameter only uses the CONSENSUS or MIRAGE_CONSENSUS setting
    and does not apply the consensus parameter above.
:param trigger_history_override: If the 3-sigma algorithms have reached
    CONSENSUS this many times in a row, override a custom algorithm result of
    not anomalous. int
:param use_with: a list of Skyline apps which should apply the algorithm if
    they handle the metric, it is only applied if the app handles the metric,
    generally set this to ``['analyzer', 'analyzer_batch', 'mirage', 'crucible']``
:param debug_logging: whether to run debug logging on the custom algorithm,
    normally set this to ``False`` but for development and testing ``True`` is
    useful.
:type namespaces: list
:type algorithm_source: str
:type algorithm_parameters: dict
:type consensus: int
:type algorithms_allowed_in_consensus: list
:type run_before_3sigma: boolean
:type run_3sigma_algorithms: boolean
:type run_only_if_consensus: boolean
:type trigger_history_override: int
:type use_with: list
:type debug_logging: boolean

"""

DEBUG_CUSTOM_ALGORITHMS = False
"""
:var DEBUG_CUSTOM_ALGORITHMS: a boolean to enable debugging.
:vartype DEBUG_CUSTOM_ALGORITHMS: boolean
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
:var ENABLE_MIRAGE: This enables Analyzer to output to Mirage [USER_DEFINED]
:vartype ENABLE_MIRAGE: boolean
"""

ENABLE_FULL_DURATION_ALERTS = False
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
    ('skyline_test.alerters.test', 'smtp|http_alerter-mock_api_alerter_receiver', 1),
    ('horizon.test.udp', 'smtp|http_alerter-mock_api_alerter_receiver', 1),
    ('horizon.test.pickle', 'smtp|http_alerter-mock_api_alerter_receiver', 1),
    ('skyline_test.alerters.test', 'slack', 1),  # slack alerts MUST be declared afer smtp
    ('skyline_test.alerters.test', 'pagerduty', 1),
    # ('stats', 'http_alerter-external_endpoint', 30),
    # ('carbon', 'http_alerter-otherapp', 60),
)
"""
:var ALERTS: This enables analyzer alerting. [USER_DEFINED]
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
        # Log all anomalies to syslog
        ('stats.', 'syslog', 1),
        # Wildcard namespaces can be used as well
        ('metric4.thing.*.requests', 'stmp', 900),
        # However beware of wildcards as the above wildcard should really be
        ('metric4.thing\\..*.\\.requests', 'stmp', 900),
        # mirage - SECOND_ORDER_RESOLUTION_HOURS - if added and Mirage is enabled
        ('metric5.thing.*.rpm', 'smtp', 900, 168),
        ('org_website.status_code.500', 'smtp', 1800),
        # NOTE: all other alert tuples MUST be declared AFTER smtp alert tuples
        ('metric3', 'slack', 600),
        ('stats.', 'http_alerter_external_endpoint', 30),
        # Send SMS alert via AWS SNS and to slack
        ('org_website.status_code.500', 'sms', 1800),
        ('org_website.status_code.500', 'slack', 1800),
    )

- Alert tuple parameters are:

:param metric: metric name.
:param alerter: the alerter name e.g. smtp, syslog, slack, pagerduty,
    http_alerter_<name> or sms.
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

.. note:: All other alerts must be declared AFTER smtp alerts as other alerts
    rely on the smtp resources.

"""

EXTERNAL_ALERTS = {}
"""
:var EXTERNAL_ALERTS: ADVANCED FEATURE - Skyline can get json alert configs from
    external sources.
:vartype EXTERNAL_ALERTS: dict

See the External alerts documentation for the elements the are required in a
json alert config and how external alerts are applied.

- **Example**::

    EXTERNAL_ALERTS = {
        'test_alert_config': {
            'url': 'http://127.0.0.1:1500/mock_api?test_alert_config',
            'method': 'GET',
        },
    }

"""

DO_NOT_ALERT_ON_STALE_METRICS = [
    # DO NOT alert if these metrics go stale
    'donot_alert_on_stale.metric_known_to_go_stale',
]
"""
:var DO_NOT_ALERT_ON_STALE_METRICS: Metrics to not digest alert on if they
    are becoming stale.
:vartype DO_NOT_ALERT_ON_STALE_METRICS: list

These are metrics that you do not want a Skyline stale digest alert on.  Works
in the same way that SKIP_LIST does, it matches in the string or dotted
namespace elements.

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

NON_DERIVATIVE_MONOTONIC_METRICS = [
    'the_namespace_of_the_monotonic_metric_to_not_calculate_the_derivative_for',
]
"""
:var NON_DERIVATIVE_MONOTONIC_METRICS: Strictly increasing monotonically metrics
    to **not** calculate the derivative values for
:vartype NON_DERIVATIVE_MONOTONIC_METRICS: list

Skyline by default automatically converts strictly increasingly monotonically
metric values to their derivative values by calculating the delta between
subsequent datapoints.  The function ignores datapoints that trend down.  This
is useful for metrics that increase over time and then reset.

Any strictly increasing monotonically metrics that you do not want Skyline to
convert to the derivative values are declared here.  This list works in the same
way that Horizon SKIP_LIST does, it matches in the string or dotted namespace
elements.

"""

# Each alert module requires additional information.
SMTP_OPTS = {
    # This specifies the sender of email alerts.
    'sender': 'skyline@your_domain.com',
    # recipients is a dictionary mapping metric names
    # (exactly matching those listed in ALERTS) to an array of e-mail addresses
    'recipients': {
        'horizon.test': ['you@your_domain.com'],
        'skyline': ['you@your_domain.com', 'them@your_domain.com'],
        'skyline_test.alerters.test': ['you@your_domain.com'],
    },
    # This is the default recipient which acts as a catchall for alert tuples
    # that do not have a matching namespace defined in recipients
    'default_recipient': ['you@your_domain.com'],
    'embed-images': True,
}
"""
:var SMTP_OPTS: Your SMTP settings. [USER_DEFINED]
:vartype SMTP_OPTS: dictionary

It is possible to set the email addresses to no_email if you do not wish to
receive SMTP alerts, but smtp alerters are required see
https://earthgecko-skyline.readthedocs.io/en/latest/alerts.html

.. note:: For each alert tuple defined in :mod:`settings.ALERTS` you need a
    recipient defined that matches the namespace.  The default_recipient acts
    as a catchall for any alert tuple that does not have a matching recipients
    defined.
"""

HIPCHAT_OPTS = {
    'auth_token': 'hipchat_auth_token',
    'sender': 'hostname or identifier',
    # list of hipchat room_ids to notify about each anomaly
    # (similar to SMTP_OPTS['recipients'])
    'rooms': {
        'skyline': (12345,),
        'skyline_test.alerters.test': (12345,),
        'horizon.udp.test': (12345,),
    },
    # Background color of hipchat messages
    # (One of 'yellow', 'red', 'green', 'purple', 'gray', or 'random'.)
    'color': 'purple',
}
"""
:var HIPCHAT_OPTS: [DEPRECATED] Your Hipchat settings.
:vartype HIPCHAT_OPTS: dictionary

HipChat alerts require python-simple-hipchat
"""

SLACK_OPTS = {
    # Bot User OAuth Access Token
    'bot_user_oauth_access_token': 'YOUR_slack_bot_user_oauth_access_token',
    # list of slack channels to notify about each anomaly
    # (similar to SMTP_OPTS['recipients'])
    # channel names - you can either pass the channel name (#general) or encoded
    # Note that even if you map multiple channels to a metric namespace, only ONE, the
    # first channel, will get slack updates to the thread.
    # Also note the channels are parsed in order and the first match will be the
    # channel/s were alerts are sent.
    # ID (C024BE91L)
    'channels': {
        'skyline': ('#skyline',),
        'skyline_test.alerters.test': ('#skyline',),
        'horizon.udp.test': ('#skyline', '#testing'),
    },
    # For every channel defined above a channel_id is required.
    'channel_ids': {
        '#skyline': 'YOUR_slack_channel_id',
        '#testing': 'YOUR_slack_other_channel_id',
    },
    'icon_emoji': ':chart_with_upwards_trend:',
    # Your default slack Skyline channel name e.g. '#skyline'
    'default_channel': 'YOUR_default_slack_channel',
    # Your default slack Skyline channel id e.g. 'C0XXXXXX'
    'default_channel_id': 'YOUR_default_slack_channel_id',
    # Whether to update slack message threads on any of the below events
    'thread_updates': True,
    # You can disable or enable each message_on event individually
    'message_on_training_data_viewed': True,
    'message_on_training_data_viewed_reaction_emoji': 'eyes',
    'message_on_features_profile_created': True,
    'message_on_features_profile_created_reaction_emoji': 'thumbsup',
    'message_on_features_profile_learnt': True,
    'message_on_features_profile_learnt_reaction_emoji': 'heavy_check_mark',
    'message_on_features_profile_disabled': True,
    'message_on_features_profile_disabled_reaction_emoji': 'x',
    'message_on_validated_features_profiles': True,
}
"""
:var SLACK_OPTS: Your slack settings. [USER_DEFINED]
:vartype SLACK_OPTS: dictionary

slack alerts require slackclient
"""

PAGERDUTY_OPTS = {
    # Your pagerduty subdomain and auth token
    'subdomain': 'example',
    'auth_token': 'your_pagerduty_auth_token',
    # Service API key (shown on the detail page of a 'Generic API' service)
    'key': 'your_pagerduty_service_api_key',
}
"""
:var PAGERDUTY_OPTS: Your SMTP settings. [USER_DEFINED]
:vartype PAGERDUTY_OPTS: dictionary

PagerDuty alerts require pygerduty
"""

SYSLOG_OPTS = {
    'ident': 'skyline',
}
"""
:var SYSLOG_OPTS: Your syslog settings.
:vartype SYSLOG_OPTS: dictionary

syslog alerts requires an ident this adds a LOG_WARNING message to the
LOG_LOCAL4 which will ship to any syslog or rsyslog down the line.  The
``EXPIRATION_TIME`` for the syslog alert method should be set to 1 to fire
every anomaly into the syslog.
"""

HTTP_ALERTERS_OPTS = {}
"""
:var HTTP_ALERTERS_OPTS: External alert endpoints.
:vartype HTTP_ALERTERS_OPTS: dictionary

- **Dictionary example**::

    HTTP_ALERTERS_OPTS = {
        'http_alerter-mock_api_alerter_receiver': {
            'enabled': True,
            'endpoint': 'http://127.0.0.1:1500/mock_api?alert_reciever',
            'token': None
        },
        'http_alerter-otherapp': {
            'enabled': False,
            'endpoint': 'https://other-http-alerter.example.org/alerts',
            'token': '1234567890abcdefg'
        },
    }

All http_alerter alert names must be prefixed with `http_alerter` followed by
the name you want to assign to it.  HTTP_ALERTERS_OPTS is used by Analyzer (in
:mod:`settings.ALERTS`) and Boundary (in :mod:`settings.BOUNDARY_METRICS`) in
conjunction with http_alerter defines.

"""

AWS_SNS_SMS_ALERTS_ENABLED = False
"""
:var AWS_SNS_SMS_ALERTS_ENABLED: Enables SMS alerting via AWS SNS.  If this is
    set to True :mod:`settings.AWS_OPTS` and boto3 must be configured [USER_DEFINED]
:vartype AWS_SNS_SMS_ALERTS_ENABLED: boolean

By default Skyline by default just uses the boto3
"""

AWS_OPTS = {
    # Just use boto3 credentials method (recommended)
    'use_boto3_defaults': True,
    # Optionally you can set use_boto3_defaults to False and specific the AWS
    # credentials and region - TODO
    # 'aws_access_key_id': 'YOUR_AWS_ACCESS_KEY_ID',
    # 'aws_secret_access_key': 'YOUR_AWS_SECRET_ACCESS_KEY',
    # 'region_name': 'YOUR_AWS_REGION',
}
"""
:var AWS_OPTS: Your AWS settings. [USER_DEFINED]
:vartype AWS_OPTS: dictionary

For SMS alerts via AWS SNS using boto3.  Skyline by default just uses the boto3
configuration which should be configured as normal.  Internally boto3 uses
~/.aws/credentials and ~/.aws/config.  If you run Skyline as the skyline
user that has /opt/skyline as their $HOME directory then boto3 will expect
these files /opt/skyline/.aws/credentials and /opt/skyline/.aws/config
For documentation on configuring AWS SNS to send SMS and AWS users/IAM user and
permissions see AWS docs.

"""

SMS_ALERT_OPTS = {
    'recipients': {},
    'namespaces': {},
}
"""
:var SMS_ALERT_OPTS: Define recipients and namespaces to route SMS alerts to.
    Both :mod:`settings.AWS_SNS_SMS_ALERTS_ENABLED` and :mod:`settings.AWS_OPTS`
    must be enabled and defined for SMS alerting. [USER_DEFINED]
:vartype SMS_ALERT_OPTS: dict

SMS alerts requires AWS_OPTS to be set and are routed via AWS SNS.

- **Example**::

    SMS_ALERT_OPTS = {
        'recipients': {
            'alice': '+1098765432',
            'bob': '+12345678901',
            'pager': '+11111111111',
        },
        'namespaces': {
            'org_website.status_code.500': ['pager'],
            'disk.used_percent': ['pager', 'alice'],
            'skyline.analyzer.runtime': ['pager', 'bob']
        }
    }

"""

CUSTOM_ALERT_OPTS = {
    'main_alert_title': 'Skyline',
    'analyzer_alert_heading': 'Analyzer',
    'mirage_alert_heading': 'Mirage',
    'boundary_alert_heading': 'Boundary',
    'ionosphere_alert_heading': 'Ionosphere',
    'ionosphere_link_path': 'ionosphere',
    'append_environment': '',
}
"""
:var CUSTOM_ALERT_OPTS: Any custom alert headings you want to use
:vartype CUSTOM_ALERT_OPTS: dictionary

Here you can specify any custom alert titles and headings you want for each
alerting app.  You also can use the append_environment option to append the
environment from which the alert originated.
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

HORIZON_IP = 'YOUR_SKYLINE_INSTANCE_IP_ADDRESS'
"""
:var HORIZON_IP: The IP address for Horizon to bind to.  Skyline receives data
    from Graphite on this IP address.  This previously defaulted to
    ``gethostname()`` but has been change to be specifically specified by the
    user. [USER_DEFINED]
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

MAX_QUEUE_SIZE = 50000
"""
:var MAX_QUEUE_SIZE: Maximum allowable length of the processing queue
:vartype MAX_QUEUE_SIZE: int

This is the maximum allowable length of the processing queue before new chunks
are prevented from being added. If you consistently fill up the processing
queue, a higher MAX_QUEUE_SIZE will not save you. It most likely means that the
workers do not have enough CPU alotted in order to process the queue on time or
there is too much I/O wait on the system.  Try increasing
:mod:`settings.CHUNK_SIZE` and decreasing :mod:`settings.ANALYZER_PROCESSES` or
decreasing :mod:`settings.ROOMBA_PROCESSES`
"""

ROOMBA_PROCESSES = 1
"""
:var ROOMBA_PROCESSES: This is the number of Roomba processes that will be
    spawned by Horizon to trim timeseries in order to keep them at
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

ROOMBA_OPTIMUM_RUN_DURATION = 60
"""
:var ROOMBA_OPTIMUM_RUN_DURATION: Timeout in seconds
:vartype ROOMBA_OPTIMUM_RUN_DURATION: int

This is how often Horizon should run roomba to run and prune the time series
data in Redis to :mod:`settings.FULL_DURATION`.  Thus allows roomba to be tuned
under heavy iowait conditions.  Under heavy iowait conditions, the default 60
seconds can results in sustained CPU and IO on Redis and the horizon thread.
Being able to adjust this to 300 allows for a reduction in IO under these
conditions.
Changing value can have an impact on Ionosphere echo features profiles.
ADVANCED FEATURE if you need to use this you have a problem on your host.
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

ROOMBA_DO_NOT_PROCESS_BATCH_METRICS = False
"""
:var ROOMBA_DO_NOT_PROCESS_BATCH_METRICS: Whether Horizon roomba should
    euthanize batch processing metrics.
:vartype ROOMBA_DO_NOT_PROCESS_BATCH_METRICS: boolean

This should be left as False unless you are backfilling batch metrics and do not
want roomba removing data points before analyzer_batch has analyzed them.  If
this is set to True, analyzer_batch euthanizes batch metrics.
"""

ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS = []
"""
:var ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS: A list of lists of namespaces and
    and custom durations for batch metrics.  Advanced feature for development
    and testing.
:vartype ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS: list

This allows for testing metrics via analyzer_batch with a different
FULL_DURATION.  It is only applied if
:mod:`settings.ROOMBA_DO_NOT_PROCESS_BATCH_METRICS` is set to True.  It allows
for a metric to be feed to Skyline with historical data that is not aligned with
the 1 data point per 60 seconds paradigm and a greater duration than
:mod:`settings.FULL_DURATION`, 1 data point per 10 mins for example, allowing
analyzer_batch to fake Mirage analysed for historical data.  analyzer_batch
roomba will use this setting for the euthanize older than value if the metric
name is in a metric namespace found in a list.

- **List example**::

    ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS = [
        ['test.app6.requests.10minutely', 604800],
    ]

"""

BATCH_METRICS_CUSTOM_FULL_DURATIONS = {}
"""
:var BATCH_METRICS_CUSTOM_FULL_DURATIONS: This is ONLY applicable to metrics
    declared in :mod:`settings.ROOMBA_BATCH_METRICS_CUSTOM_DURATIONS` that are
    being back filled.  Metrics that are being backfilled and not managed by
    roomba, but managed by analyzer_batch may have a very long custom duration
    period, but may have a shorter FULL_DURATION period.  In order to backfill
    and analyse at the correct full duration for the metric, the metric full
    duration for analysis (not for analyzer batch roomba) can be declared here.
:vartype BATCH_METRICS_CUSTOM_FULL_DURATIONS: dict

- **dict example**::

    BATCH_METRICS_CUSTOM_FULL_DURATIONS = {
        'test.app6.requests.10minutely': 259200,
    }

"""

MAX_RESOLUTION = 1000
"""
:var MAX_RESOLUTION: The Horizon agent will ignore incoming datapoints if their
    timestamp is older than MAX_RESOLUTION seconds ago.
:vartype MAX_RESOLUTION: int
"""

HORIZON_SHARDS = {}
"""
:var HORIZON_SHARDS: ADVANCED FEATURE - A dictionary of Skyline hostnames and
    there assigned shard value.
:vartype HORIZON_SHARDS: dict

This setting is only applicable to running Skyline Horizon services on multiple
servers (and Graphite instances) in a replicated fashion.  This allows for all
the Skyline servers to receiver all metrics but only analyze those metrics that
are assigned to the specific server (shard).  This enables all Skyline servers
that are running Horizon to receiver the entire metric population stream from
mulitple Graphite carbon-relays and drop (not submit to their Redis instance)
any metrics that do not belong to their shard.  Related settings are
:mod:`settings.REMOTE_SKYLINE_INSTANCES` and :mod:`settings.SYNC_CLUSTER_FILES`

- **Example**::

    HORIZON_SHARDS = {
        'skyline-server-1': 0,
        'skyline-server-2': 1,
        'skyline-server-3': 2,
    }

Shards are 0 indexed.

"""

HORIZON_SHARD_PICKLE_PORT = 2026
"""
:var HORIZON_SHARD_PICKLE_PORT: ADVANCED FEATURE - This is the port that listens
    for Graphite pickles over TCP, sent by Graphite's carbon-relay-b agent.
    When running Skyline clustered with multiple Horizon instances, an
    additional Graphite carbon-relay-b instances are required to be run to on
    the remote Graphite servers to forward metrics on to the remote Horizons.
    See https://earthgecko-skyline.readthedocs.io/en/latest/horizon.html#horizon-shards
:vartype PICKLE_PORT: str
"""

HORIZON_SHARD_DEBUG = False
"""
:var HORIZON_SHARD_DEBUG: For development only to log some sharding debug info
    not for general use.
:vartype HORIZON_SHARD_DEBUG: boolean
"""

SYNC_CLUSTER_FILES = False
"""
:var SYNC_CLUSTER_FILES: ADVANCED FEATURE - If Skyline is running in a clustered
    configuration the :mod:`settings.REMOTE_SKYLINE_INSTANCES` can sync
    Ionosphere training data and features_profiles dirs and files between each
    other.  This allows for the relevant Ionosphere data to be distributed to
    each instance in the cluster, so that if an instance fails or is removed,
    all the nodes have that nodes data.  This also allows for an instance to be
    added to the cluster and it will self populate.  This self populating is
    rate limited so that a new instance will not thunder against the other
    instances in the cluster to populate itself as quickly as possible, but
    rather eventual consistency is achieve. Related settings are
    :mod:`settings.REMOTE_SKYLINE_INSTANCES`, :mod:`settings.HORIZON_SHARDS`,
    :mod:`settings.IONOSPHERE_DATA_FOLDER` and
    :mod:`settings.IONOSPHERE_PROFILES_FOLDER`
:vartype SYNC_CLUSTER_FILES: boolean
"""

SKIP_LIST = [
    # Skip the skyline namespaces, except horizon.  This prevents Skyline
    # populating a lot of anomalies related to timings, algorithm_breakdowns,
    # etc.
    'skyline.analyzer.',
    'skyline.boundary.',
    'skyline.ionosphere.',
    'skyline.mirage.',
    # If you use statsd, these can result in many near-equal series, however
    # be careful that you do not skip any of your own application namespaces
    # if they include these strings.
    # '_90',
    # '.lower',
    # '.upper',
    # '.median',
    # '.count_ps',
    # '.sum',
]
"""
:var SKIP_LIST: Metrics to skip
:vartype SKIP_LIST: list

These are metrics that, for whatever reason, you do not want to analyze in
Skyline. The Worker will check to see if each incoming metrics contains
anything in the skip list. It is generally wise to skip entire namespaces by
adding a '.' at the end of the skipped item - otherwise you might skip things
you do not intend to.  For example the default
``skyline.analyzer.anomaly_breakdown.`` which MUST be skipped to prevent crazy
feedback.

These SKIP_LIST are also matched just dotted namespace elements too, if a match
is not found in the string, then the dotted elements are compared.  For example
if an item such as 'skyline.analyzer.algorithm_breakdown' was added it would
macth any metric that matched all 3 dotted namespace elements, so it would
match:

skyline.analyzer.skyline-1.algorithm_breakdown.histogram_bins.timing.median_time
skyline.analyzer.skyline-1.algorithm_breakdown.histogram_bins.timing.times_run
skyline.analyzer.skyline-1.algorithm_breakdown.ks_test.timing.times_run

"""

DO_NOT_SKIP_LIST = [
    # DO NOT skip these metrics even if they matched in the SKIP_LIST.
    # These are the key indicator metrics for Skyline, if you want to monitor
    # Skyline for anomalies, etc. Due to the other Skyline apps only analysing
    # when required there metrics are more irratic, however all are sent to
    # Graphite and available under the 'skyline.' namespace
    'skyline.analyzer.run_time',
    'skyline.boundary.run_time',
    'skyline.analyzer.ionosphere_metrics',
    'skyline.analyzer.mirage_metrics',
    'skyline.analyzer.total_analyzed',
    'skyline.analyzer.total_anomalies',
    'skyline.analyzer.metrics_sparsity',
    'skyline.exceptions',
    'skyline.mirage.checks',
]
"""
:var DO_NOT_SKIP_LIST: Metrics to skip
:vartype DO_NOT_SKIP_LIST: list

These are metrics that you want Skyline in analyze even if they match a
namespace in the SKIP_LIST.  Works in the same way that SKIP_LIST does, it
matches in the string or dotted namespace elements.

"""

"""
Thunder settings
"""

THUNDER_ENABLED = True
"""
:var THUNDER_ENABLED: Enable Thunder.  Thunder monitors the internals and
    operational changes on the Skyline apps and metrics.  Although the user can
    monitor Skyline metrics with Skyline, thunder is for convenience so that
    Skyline has some default monitoring of important Skyline metrics and
    operations without having to add specific alert tuples on Skyline metrics.
    However this does not stop users from adding their on alerts on THEIR own
    Skyline and other metrics once they have the system up and running an get to
    know it.  WORK IN PROGRESS
:vartype THUNDER_ENABLED: boolean
"""

THUNDER_CHECKS = {
    # If not alert_via_ items are specified on a check it defaults to SMTP
    'analyzer': {
        'up': {  # Notifies if Analyzer changes UP states
            'run': True, 'expiry': 900, 'alert_via_smtp': True,
            'alert_via_slack': False, 'alert_via_pagerduty': False,
        },
        'run_time': {
            'run': True, 'expiry': 900, 'after_overruns': 5,
            'alert_via_smtp': True,
            'alert_via_slack': False, 'alert_via_pagerduty': False,
        },
        # 'total_metrics': {  # Notifies if the Analyzer total_metrics significantly changes
        #     'run': False, 'expiry': 900, 'alert_via_smtp': True,
        #     'alert_via_slack': False, 'alert_via_pagerduty': False,
        # },
        # 'total_analyzed': {  # Notifies if the Analyzer total_analyzed significantly changes
        #     'run': False, 'expiry': 900, 'alert_via_smtp': True,
        #     'alert_via_slack': False, 'alert_via_pagerduty': False,
        # },
    },
    'redis': {
        'up': {  # Notifies if Redis changes UP states
            'run': True, 'expiry': 900, 'alert_via_smtp': True,
            'alert_via_slack': False, 'alert_via_pagerduty': False,
        },
    },
    'horizon': {
        'up': {
            'run': True, 'expiry': 900, 'alert_via_smtp': True,
            'alert_via_slack': False, 'alert_via_pagerduty': False,
        },
        # 'worker.metrics_received': {
        #     'algorithm_source': '/opt/skyline/github/skyline/skyline/custom_algorithms/significant_change_window_percent_sustained.py',
        #     'run': False, 'expiry': 900, 'significant_change_percentage': 10,
        #     'significant_change_window': 600, 'significant_change_over': 3600,
        #     'times_in_a_row': 10, 'alert_via_smtp': True,
        #     'alert_via_slack': False, 'alert_via_pagerduty': False,
        # },
    },
    'mirage': {
        'up': {
            'run': True, 'expiry': 900, 'alert_via_smtp': True,
            'alert_via_slack': False, 'alert_via_pagerduty': False,
        },
    },
    'panorama': {
        'up': {
            'run': True, 'expiry': 900, 'alert_via_smtp': True,
            'alert_via_slack': False, 'alert_via_pagerduty': False,
        },
    },
    'ionosphere': {
        'up': {
            'run': True, 'expiry': 900, 'alert_via_smtp': True,
            'alert_via_slack': False, 'alert_via_pagerduty': False,
        },
    },
    'luminosity': {
        'up': {
            'run': True, 'expiry': 900, 'alert_via_smtp': True,
            'alert_via_slack': False, 'alert_via_pagerduty': False,
        },
    },
    'webapp': {
        'up': {
            'run': True, 'expiry': 900, 'alert_via_smtp': True,
            'alert_via_slack': False, 'alert_via_pagerduty': False,
        },
    },
}
"""
:var THUNDER_CHECKS: Enable/disable the checks, configure alert expiry times and
    define alert_via channels per Skyline app and check.  If no alert_via_ items
    are defined the default is to alert_via_smtp for all thunder checks.  Think
    of Thunder checks as built-in Skyline ALERTS and BOUNDARY_METRICS checks
    specifically for your Skyline instance metrics and external applications,
    without you have to know what Skyline metrics and things to watch and know
    the values of to configure.  WORK IN PROGRESS [USER_DEFINED]
:vartype THUNDER_CHECKS: dict
"""

THUNDER_OPTS = {
    # SMTP ALERTS
    'alert_via_smtp': True,
    # This is a list of email addresses to send thunder alerts to, it can
    # contain a single email in the list.
    'smtp_recipients': ['you@your_domain.com', 'them@your_domain.com'],
    # 'smtp_recipients': [],
    # SLACK ALERTS (uses settings.SLACK_OPTS)
    'alert_via_slack': False,
    'slack_channel': '#skyline',
    # PAGERDUTY ALERTS (uses settings.PAGERDUTY_OPTS)
    'alert_via_pagerduty': False,
}
"""
:var THUNDER_OPTS: Thunder can alert via any combination of the following routes
    SMTP, Slack and Pagerduty.  These settings are very similar to Analyzer and
    Boundary alert related settings and the values may be the same. However
    Thunder alerts are meant for the Skyline administrator/s whereas Analyzer
    and Boundary related alerts can be routed to many different parties.
    [USER_DEFINED]
:vartype THUNDER_ENABLED: boolean
"""

"""
Panorama settings
"""

PANORAMA_ENABLED = True
"""
:var PANORAMA_ENABLED: Enable Panorama [USER_DEFINED]
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
    [USER_DEFINED]
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
:var PANORAMA_DBUSERPASS: The database user password [USER_DEFINED]
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
:vartype PANORAMA_EXPIRY_TIME: int

- This is the Panorama sample rate.  Please bear in mind Panorama does not use
  the ALERTS time expiry keys or matching, Panorama records every anomaly, even
  if the metric is not in an alert tuple.  Consider that a metric could and does
  often fire as anomalous every minute, until it no longer is.
"""

PANORAMA_CHECK_MAX_AGE = 300
"""
:var PANORAMA_CHECK_MAX_AGE: Panorama will only process a check file if it is
    not older than PANORAMA_CHECK_MAX_AGE seconds.  If it is set to 0 it does
    all.  This setting just ensures if Panorama stalls for some hours and is
    restarted, the user can choose to discard older checks and miss anomalies
    being recorded if they so choose to, to prevent Panorama stampeding against
    MySQL if something went down and Panorama comes back online with lots of
    checks.
:vartype PANORAMA_CHECK_MAX_AGE: int
"""

PANORAMA_CHECK_INTERVAL = 20
"""
:var PANORAMA_CHECK_INTERVAL: How often (in seconds) Panorama will check for
    anomalie1s to add to the database.  This allows you to configure Panorama to
    insert anomalies into the database every second if you so wish to, however
    in most cases every 20 seconds is sufficient.
    However, NOTE, if SNAB_ENABLE is True this is automatically overridden to
    the value of 1 as snab needs anomaly ids available asap.  This can increase
    disk I/O until such as time as Panorama changes from check files to Redis.
:vartype PANORAMA_CHECK_INTERVAL: int
"""

PANORAMA_INSERT_METRICS_IMMEDIATELY = False
"""
:var PANORAMA_INSERT_METRICS_IMMEDIATELY: Panorama will only insert a metric
    into the metrics database table when an anomaly is registered for the metric
    by default.  To have Panorama insert every metric into the database as soon
    as it appears in unique_metrics, set this to True.  This functionality is
    for development and testing purposes.
:vartype PANORAMA_CHECK_INTERVAL: boolean
"""

"""
Mirage settings
"""

MIRAGE_PROCESSES = 1
"""
:var MIRAGE_PROCESSES: This is the number of processes that the Skyline Mirage
    will spawn to process checks.  If you find the mirage checks.pending metric
    and checks.stale_discarded are above 0 too often, increase this accordingly.
    Adding additional processes can increase CPU and load on both Skyline and
    Graphite.
:vartype MIRAGE_PROCESSES: int
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
:var MIRAGE_ENABLE_ALERTS: This enables Mirage alerting. [USER_DEFINED]
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

MIRAGE_PERIODIC_CHECK = True
"""
:var MIRAGE_PERIODIC_CHECK: This enables Mirage to periodically check metrics
    matching the namespaces in :mod:`settings.MIRAGE_PERIODIC_CHECK_NAMESPACES`
    at every :mod:`settings.MIRAGE_PERIODIC_CHECK_INTERVAL`.  Mirage should only
    be configured to periodically analyse key metrics.  For further in depth
    details regarding Mirage periodic check and their impact, please see the
    Mirage Periodic Checks documentation at:
    https://earthgecko-skyline.readthedocs.io/en/latest/mirage.html#periodic-checks
    Further :mod:`settings.MIRAGE_ONLY_METRICS` are handled by
:vartype MIRAGE_PERIODIC_CHECK: boolean
"""

MIRAGE_PERIODIC_CHECK_INTERVAL = 3600
"""
:var MIRAGE_PERIODIC_CHECK_INTERVAL: This is the interval in seconds at which
    Mirage should analyse metrics matching the namespaces in
    :mod:`settings.MIRAGE_PERIODIC_CHECK_NAMESPACES`
:vartype MIRAGE_PERIODIC_CHECK_INTERVAL: int
"""

MIRAGE_PERIODIC_CHECK_NAMESPACES = [
    # Check these Mirage metric namespaces periodically with Mirage
]
"""
:var MIRAGE_PERIODIC_CHECK_NAMESPACES: Mirage metric namespaces to periodically
    check with Mirage, even if Analyzer does not find them anomalous, Analyzer
    will ensure that these Mirage metric namespaces are analyzed by Mirage every
    :mod:`settings.MIRAGE_PERIODIC_CHECK_INTERVAL` seconds.  This works in the
    same way that :mod:`settings.SKIP_LIST` does, it matches in the string or
    the dotted namespace elements.
:vartype MIRAGE_PERIODIC_CHECK_NAMESPACES: list
"""

MIRAGE_ALWAYS_METRICS = []
"""
:var MIRAGE_ALWAYS_METRICS: These are metrics you want to always be checked by
    Mirage, every minute and not just by Analyzer.  For this to be in effect,
    you must ensure that MIRAGE_PERIODIC_CHECK is set to ``True``.  This allows
    for a use case where you want to apply a specific
    :mod:`settings.CUSTOM_ALGORITHMS` algorithm on a metric, all the time.  The
    metrics declared here must be absolute metric names, no element matching or
    regex is applied.
:vartype MIRAGE_ALWAYS_METRICS: list
"""

MIRAGE_AUTOFILL_TOOSHORT = False
"""
:var MIRAGE_AUTOFILL_TOOSHORT: This is a convenience feature that allows
    Analyzer to send metrics that are classed TooShort to Mirage and have Mirage
    attempt to fetch the data from Graphite and populate Redis with the time
    series data.  This is useful if for some reason Graphite stopped pickling
    data to Horizon or when Skyline is first run against a populated Graphite
    that already has metric data.  Mirage will only try to fetch data from
    Graphite and populate Redis with data for a metric once in FULL_DURATION,
    so that any remote metrics from Vista are not attempt to be fetched over and
    over as this feature only works with the GRAPHITE_HOST, not remote data
    sources.
:vartype MIRAGE_AUTOFILL_TOOSHORT: boolean
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
    ('skyline_test.alerters.test', 'greater_than', 1, 0, 0, 0, 1, 'smtp|http_alerter-mock_api_alerter_receiver'),
    ('skyline_test.alerters.test', 'detect_drop_off_cliff', 1800, 500, 3600, 0, 2, 'smtp|http_alerter-mock_api_alerter_receiver'),
    ('skyline_test.alerters.test', 'less_than', 3600, 0, 0, 15, 2, 'smtp|http_alerter-mock_api_alerter_receiver'),
    ('metric1', 'detect_drop_off_cliff', 1800, 500, 3600, 0, 2, 'smtp|slack|pagerduty'),
    ('metric2.either', 'less_than', 3600, 0, 0, 15, 2, 'smtp|hipchat'),
    ('nometric.other', 'greater_than', 3600, 0, 0, 100000, 1, 'smtp'),
    ('nometric.another', 'greater_than', 3600, 0, 0, 100000, 1, 'http_alerter-external_endpoint'),
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
:param ALERT_VIAS: pipe separated alerters to send to, valid options smtp,
    pagerduty, http_alerter_<name> and slack.  This could therefore be 'smtp',
    'smtp|slack', 'pagerduty|slack', etc
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
    ('stats_counts.otherapp.things.*.requests', 'detect_drop_off_cliff', 600, 500, 3600, 0, 2, 'smtp|slack'),

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

BOUNDARY_AUTOAGGRERATION_METRICS = (
    ('nometrics.either', 60),
)
"""
:var BOUNDARY_AUTOAGGRERATION_METRICS: The namespaces to autoaggregate
:vartype BOUNDARY_AUTOAGGRERATION_METRICS: tuples

- **Tuple schema example**::

    BOUNDARY_AUTOAGGRERATION_METRICS = (
        ('stats_counts', AGGREGATION_VALUE),
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
        'slack': 1800,
        # 'http_alerter_external_endpoint': 1,
    },
    # If alerter keys >= limit in the above alerter_expiration_time do not alert
    # to this channel until keys < limit
    'alerter_limit': {
        'smtp': 100,
        'pagerduty': 15,
        'slack': 30,
        # 'http_alerter_external_endpoint': 10000,
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
    # This is the default recipient which acts as a catchall for alert tuples
    # that do not have a matching namespace defined in recipients
    'default_recipient': ['you@your_domain.com'],
    'embed-images': True,
    # Send graphite graphs at the most meaningful resolution if different from
    # FULL_DURATION
    'graphite_previous_hours': 7,
    'graphite_graph_line_color': 'pink',
}
"""
:var BOUNDARY_SMTP_OPTS: Your SMTP settings. [USER_DEFINED]
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
:var BOUNDARY_HIPCHAT_OPTS: [DEPRECATED] Your Hipchat settings.
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
:var BOUNDARY_PAGERDUTY_OPTS: Your PagerDuty settings. [USER_DEFINED]
:vartype BOUNDARY_PAGERDUTY_OPTS: dictionary

PagerDuty alerts require pygerduty
"""

BOUNDARY_SLACK_OPTS = {
    # Bot User OAuth Access Token
    'bot_user_oauth_access_token': 'YOUR_slack_bot_user_oauth_access_token',
    # list of slack channels to notify about each anomaly
    # (similar to BOUNDARY_SMTP_OPTS['recipients'])
    # channel names - you can either pass the channel name (#general) or encoded
    # ID (C024BE91L)
    'channels': {
        'skyline': ('#general',),
        'skyline_test.alerters.test': ('#general',),
    },
    'icon_emoji': ':chart_with_upwards_trend:',
}
"""
:var BOUNDARY_SLACK_OPTS: Your slack settings. [USER_DEFINED]
:vartype BOUNDARY_SLACK_OPTS: dictionary

slack alerts require slackclient
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
    development server, set this to either ``'gunicorn'`` or ``'flask'``.
    Flask is no longer supported.
:vartype WEBAPP_SERVER: str
"""

WEBAPP_GUNICORN_WORKERS = 4
"""
:var WEBAPP_GUNICORN_WORKERS: How many gunicorn workers to run for the webapp.
    The normal recommended value for gunicorn is generally be between 2-4
    workers per core, however on a machine with lots of cores this is probably
    over provisioning for the webapp, depending on the load on the server.
:vartype WEBAPP_GUNICORN_WORKERS: int
"""

WEBAPP_GUNICORN_BACKLOG = 254
"""
:var WEBAPP_GUNICORN_BACKLOG: The maximum number of pending connections.  This
    refers to the number of clients that can be waiting to be served. Exceeding
    this number results in the client getting an error when attempting to
    connect. It should only affect servers under significant load.  It must be a
    positive integer. Generally set in the 64-2048 range.
:vartype WEBAPP_GUNICORN_BACKLOG: int
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
:var WEBAPP_AUTH_USER: The username for pseudo basic HTTP auth [USER_DEFINED]
:vartype WEBAPP_AUTH_USER: str
"""

WEBAPP_AUTH_USER_PASSWORD = 'aec9ffb075f9443c8e8f23c4f2d06faa'
"""
:var WEBAPP_AUTH_USER_PASSWORD: The user password for pseudo basic HTTP auth
    [USER_DEFINED]
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

WEBAPP_USER_TIMEZONE = True
"""
:var WEBAPP_USER_TIMEZONE: This determines the user's timezone and
    renders graphs with the user's date values.  If this is set to ``False`` the
    timezone in :mod:`settings.WEBAPP_FIXED_TIMEZONE` is used.
:vartype WEBAPP_USER_TIMEZONE: boolean
"""

WEBAPP_FIXED_TIMEZONE = 'Etc/GMT+0'
"""
:var WEBAPP_FIXED_TIMEZONE: You can specific a timezone you want the client
    browser to render graph date and times in.  This setting is only used if the
    :mod:`settings.WEBAPP_USER_TIMEZONE` is set to ``False``.
    This must be a valid momentjs timezone name, see:
    https://github.com/moment/moment-timezone/blob/develop/data/packed/latest.json
:vartype WEBAPP_FIXED_TIMEZONE: str

.. note:: Timezones, UTC and javascript Date
    You only need to use the first element of the momentjs timezone string, some
    examples, 'Europe/London', 'Etc/UTC', 'America/Los_Angeles'.
    Because the Webapp is graphing using data UTC timestamps, you may may want
    to display the graphs to users with a fixed timezone and not use the browser
    timezone so that the Webapp graphs are the same in any location.

"""

WEBAPP_ACCEPT_DATA_UPLOADS = False
"""
:var WEBAPP_ACCEPT_DATA_UPLOADS: Enables the webapp to accept data uploads for
    Flux to process.  This is related to :mod:`settings.FLUX_PROCESS_UPLOADS`
    and uploads are saved to :mod:`settings.DATA_UPLOADS_PATH`
:vartype WEBAPP_ACCEPT_DATA_UPLOADS: boolean
"""

WEBAPP_JAVASCRIPT_DEBUG = False
"""
:var WEBAPP_JAVASCRIPT_DEBUG: Enables some javascript console.log when enabled.
:vartype WEBAPP_JAVASCRIPT_DEBUG: boolean
"""

ENABLE_WEBAPP_DEBUG = False
"""
:var ENABLE_WEBAPP_DEBUG: Enables some app specific debugging to log.
:vartype ENABLE_WEBAPP_DEBUG: boolean
"""

WEBAPP_PREPROCESS_TIMESERIES = False
"""
:var WEBAPP_PREPROCESS_TIMESERIES: Allow for the time series to be aggregated by
    median or sum per minute so that webapp can return a reasonable number of
    data points for dyngraph to load and display in the browser without causing
    lag.  This is achieved by aggregating the time series using either the
    median of values or the sum as defined by
    settings.WEBAPP_PREPROCESS_AGGREGATE_BY.  At the interval defined by
    settings.  Not implemented - UNDER DEVELOPMENT
:vartype WEBAPP_PREPROCESS_TIMESERIES: boolean
"""

WEBAPP_PREPROCESS_AGGREGATE_BY = 'median'
"""
:var WEBAPP_PREPROCESS_AGGREGATE_BY: The method by which to aggregate the time
    series by.  Valid strings here are 'median' and 'sum'.
    settings.WEBAPP_PREPROCESS_AGGREGATE_BY. Not implemented - UNDER DEVELOPMENT
:vartype WEBAPP_PREPROCESS_AGGREGATE_BY: str
"""

"""
Ionosphere settings
"""

IONOSPHERE_CHECK_PATH = '/opt/skyline/ionosphere/check'
"""
:var IONOSPHERE_CHECK_PATH: This is the location the Skyline apps will write the
    anomalies to for Ionosphere to check to a file on disk - absolute path
:vartype IONOSPHERE_CHECK_PATH: str
"""

IONOSPHERE_ENABLED = True
"""
:var IONOSPHERE_ENABLED: Enable Ionosphere
:vartype IONOSPHERE_ENABLED: boolean
"""

IONOSPHERE_VERBOSE_LOGGING = False
"""
:var IONOSPHERE_VERBOSE_LOGGING: As of Skyline 3.0, apps log notice and errors
    only.  To have addtional info logged set this to True.  Useful for debugging
    but less verbose than LOCAL_DEBUG.
:vartype IONOSPHERE_VERBOSE_LOGGING: boolean
"""

IONOSPHERE_PROCESSES = 1
"""
:var IONOSPHERE_PROCESSES: Number of processes to assign to Ionosphere, however
    Ionosphere should never need more than 1 and is effectively hard coded as
    such currently.  This variable is only declared for the purpose of
    maintaining a standard set up in each module and to possibly enable more
    than one processor on Ionosphere in the future, should there be a
    requirement for Ionosphere to analyse the metrics quicker.  Running
    Ionosphere with more than one process is untested and currently it is
    hard coded to be 1 (https://github.com/earthgecko/skyline/issues/69)
:vartype IONOSPHERE_ENABLED: int
"""

IONOSPHERE_MAX_RUNTIME = 120
"""
:var IONOSPHERE_MAX_RUNTIME: The maximum number of seconds an Ionosphere check
    should run for.
:vartype IONOSPHERE_MAX_RUNTIME: int
"""

ENABLE_IONOSPHERE_DEBUG = False
"""
:var ENABLE_IONOSPHERE_DEBUG: DEVELOPMENT only - enables additional debug logging
    useful for development only, this should definitely be set to ``False`` on
    production system as LOTS of output
:vartype ENABLE_IONOSPHERE_DEBUG: boolean
"""

IONOSPHERE_DATA_FOLDER = '/opt/skyline/ionosphere/data'
"""
:var IONOSPHERE_DATA_FOLDER: This is the path for the Ionosphere data folder
    where anomaly data for timeseries and training will be stored - absolute path
:vartype IONOSPHERE_DATA_FOLDER: str
"""

IONOSPHERE_HISTORICAL_DATA_FOLDER = '/opt/skyline/ionosphere/historical_data'
"""
:var IONOSPHERE_HISTORICAL_DATA_FOLDER: The absolute path for the Ionosphere
    historical data folder where anomaly data for timeseries and training will
    moved to when it reaches it purge age as defined for the namespace in
    :mod:`settings.IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR`. Unless you
    are feeding in and analysing historical data, this can generally be ignored
    and is an ADVANCED FEATURE.  Note if you do use this and
    :mod:`settings.IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR` be advised
    that there is NO purge of this directory, it must be done MANUALLY when
    you have completed your historical analysis and training.
:vartype IONOSPHERE_HISTORICAL_DATA_FOLDER: str
"""

IONOSPHERE_PROFILES_FOLDER = '/opt/skyline/ionosphere/features_profiles'
"""
:var IONOSPHERE_PROFILES_FOLDER: This is the path for the Ionosphere data folder
    where anomaly data for timeseries will be stored - absolute path
:vartype IONOSPHERE_DATA_FOLDER: str
"""

IONOSPHERE_LEARN_FOLDER = '/opt/skyline/ionosphere/learn'
"""
:var IONOSPHERE_LEARN_FOLDER: This is the path for the Ionosphere learning data
    folder where learning data for timeseries will be processed - absolute path
:vartype IONOSPHERE_LEARN_FOLDER: str
"""

IONOSPHERE_CHECK_MAX_AGE = 300
"""
:var IONOSPHERE_CHECK_MAX_AGE: Ionosphere will only process a check file if it is
    not older than IONOSPHERE_CHECK_MAX_AGE seconds.  If it is set to 0 it does
    all.  This setting just ensures if Ionosphere stalls for some hours and is
    restarted, the user can choose to discard older checks and miss anomalies
    being recorded if they so choose to, to prevent Ionosphere stampeding.
:vartype IONOSPHERE_CHECK_MAX_AGE: int
"""

IONOSPHERE_KEEP_TRAINING_TIMESERIES_FOR = 86400
"""
:var IONOSPHERE_KEEP_TRAINING_TIMESERIES_FOR: Ionosphere will keep timeseries
    data files for this long, for the operator to review and train on.
:vartype IONOSPHERE_KEEP_TRAINING_TIMESERIES_FOR: int
"""

IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR = []
"""
:var IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR: After :mod:`settings.IONOSPHERE_KEEP_TRAINING_TIMESERIES_FOR`
    has elapsed Ionosphere will move training data for metric namespaces declared
    in this list to the :mod:`settings.ONOSPHERE_HISTORICAL_DATA_FOLDER`. The
    metric namespaces defined here are only matched on a simple substring match,
    NOT on elements and/or a regex.  If the substring is in the metric name
    it will be moved.  Unless you are feeding in and analysed historical data,
    this can generally be ignored and is an advanced setting.
:vartype IONOSPHERE_CUSTOM_KEEP_TRAINING_TIMESERIES_FOR: list
"""

IONOSPHERE_MANAGE_PURGE = True
"""
:var IONOSPHERE_MANAGE_PURGE: Ionosphere will manage purging the training_data
    and learn data directories.  Under normal running conditions with a SSD hard
    drive this is perfectly acceptable, however if for some reason your file
    system is not purely SSD or you are using a distributed file system that is
    not extreme fast, managing purging via Ionosphere can cause Ionosphere
    analysis to lag.  This can be set to False and you can manage purging with
    your own script or method and Ionosphere will not purge data.
:vartype IONOSPHERE_MANAGE_PURGE: boolean
"""

IONOSPHERE_GRAPHITE_NOW_GRAPHS_OVERRIDE = False
"""
:var IONOSPHERE_GRAPHITE_NOW_GRAPHS_OVERRIDE: By defualt Graphite NOW graphs in
    training_data are retrieved from Graphite when the training data is viewed.
    Graphite NOW graphs are then generated using the current timestamp and
    plotting graphs at now - 7h, now - 24h, now - 7days and now - 30days, this
    can cause the actual anomalous period to not be shown in the Graphite NOW
    graphs if the training data is only loaded days later.  This is especially
    true if metrics are batch processed with historic data.  If this settings is
    set to True, Graphite NOW graphs will be loaded as Graphite THEN graphs,
    using the anomaly timestamp as now.
:vartype IONOSPHERE_GRAPHITE_NOW_OVERRIDE: boolean
"""

SKYLINE_URL = 'https://skyline.example.com'
"""
:var SKYLINE_URL: The http or https URL (and port if required) to access your
    Skyline on (no trailing slash).  For example if you were not using SSL
    termination and listening on a specific port it could be like
    http://skyline.example.com:8080' [USER_DEFINED]

:vartype SKYLINE_URL: str
"""

# No longer declared in the settings.py handled outside user config in
# skyline/tsfresh_feature_names.py
# TSFRESH_VERSION = '0.4.0'
# """
# :var TSFRESH_VERSION: The version of tsfresh installed by pip, this is important
#    in terms of feature extraction baselines
# :vartype TSFRESH_VERSION: str
# """
#
# TSFRESH_BASELINE_VERSION = '0.4.0'
# """
# :var TSFRESH_BASELINE_VERSION: The version of tsfresh installed by pip, this is important
#     in terms of feature extraction baselines
# :vartype TSFRESH_BASELINE_VERSION: str
# """

SERVER_PYTZ_TIMEZONE = 'UTC'
"""
:var SERVER_PYTZ_TIMEZONE: You must specify a pytz timezone you want Ionosphere
    to use for the creation of features profiles and converting datetimes to
    UTC.  This must be a valid pytz timezone name, see:
    https://github.com/earthgecko/skyline/blob/ionosphere/docs/development/pytz.rst
    http://earthgecko-skyline.readthedocs.io/en/ionosphere/development/pytz.html#timezones-list-for-pytz-version
    [USER_DEFINED]
:vartype SERVER_PYTZ_TIMEZONE: str
"""

IONOSPHERE_FEATURES_PERCENT_SIMILAR = 1.0
"""
:var IONOSPHERE_FEATURES_PERCENT_SIMILAR: The percentage difference between a
    features profile sum and a calculated profile sum to result in a match.
:vartype IONOSPHERE_FEATURES_PERCENT_SIMILAR: float
"""

IONOSPHERE_MINMAX_SCALING_ENABLED = True
"""
:var IONOSPHERE_MINMAX_SCALING_ENABLED: Implement Min-Max scaling on features
    profile time series and an anomalous time series if the features profile
    sums do not match.  This adds a form of standardization that significantly
    improves the Ionosphere features sum comparison technique of high range
    metrics within the IONOSPHERE_MINMAX_SCALING_RANGE_TOLERANCE boundaries.
:vartype IONOSPHERE_MINMAX_SCALING_ENABLED: boolean
"""

IONOSPHERE_MINMAX_SCALING_RANGE_TOLERANCE = 0.15
"""
:var IONOSPHERE_MINMAX_SCALING_RANGE_TOLERANCE: Min-Max scaling will only be
    implemented if the lower and upper ranges of both the features profile time
    series and the anomalous time series are within these margins.  The default
    being 0.15 (or 15 percent).  This prevents Ionosphere from Min-Max scaling
    and comparing time series that are in significantly different ranges and
    only applying Min-Max scaling comparisons when it is sensible to do so.
:vartype IONOSPHERE_MINMAX_SCALING_RANGE_TOLERANCE: float
"""

IONOSPHERE_ECHO_ENABLED = True
"""
:var IONOSPHERE_ECHO_ENABLED: This enables Ionosphere to create and test
    features profiles for Mirage metrics but at settings.FULL_DURATION as well.
    Features profiles will be made on the fly for any existing, validated Mirage
    metric features profiles.  Ionosphere's matching performance is increased
    between 30 to 50 percent when Ionosphere echo is run.
:vartype IONOSPHERE_ECHO_ENABLED: boolean
"""

IONOSPHERE_ECHO_MAX_FP_CREATE_TIME = 55
"""
:var IONOSPHERE_ECHO_MAX_FP_CREATE_TIME: The maximum number of seconds an
    Ionosphere echo process should run creating FULL_DURATION features profiles
    for created Mirage features profiles.  This setting is specifically relevant
    for Skyline implematations pre Ionosphere echo (v1.2.12) to prevent timeouts
    if Ionosphere echo needs to make > 30 echo features profiles for Mirage
    metrics with lots of existing features profiles.
:vartype IONOSPHERE_ECHO_MAX_FP_CREATE_TIME: int
"""

IONOSPHERE_ECHO_FEATURES_PERCENT_SIMILAR = 2.5
"""
:var IONOSPHERE_ECHO_FEATURES_PERCENT_SIMILAR: In terms of Ionosphere echo a
    value of 2.0 is the default.  This default is above the normal
    IONOSPHERE_FEATURES_PERCENT_SIMILAR due to that fact that the the resolution
    of Ionosphere echo is at FULL_DURATION.  During testing this value was
    tested at 1.0, 2 and 2.5, with 2.5 resulting in the most desirable results
    in terms of matching time series that are similarly not anomalous.
:vartype IONOSPHERE_ECHO_FEATURES_PERCENT_SIMILAR: float
"""

IONOSPHERE_ECHO_MINMAX_SCALING_FEATURES_PERCENT_SIMILAR = 3.5
"""
:var IONOSPHERE_ECHO_MINMAX_SCALING_FEATURES_PERCENT_SIMILAR: In terms of
    Ionosphere echo Min-Max scaling percentage similar, a value of 3.5 is the
    default.  This default is above the normal IONOSPHERE_FEATURES_PERCENT_SIMILAR
    due to that fact that the the resolution of Ionosphere echo is at FULL_DURATION
    and echo is using the normal IONOSPHERE_MINMAX_SCALING_RANGE_TOLERANCE to
    determine if Min-Max scaling should be run.  During testing this value was
    tested at 1, 2 and 3.5, with 3.5 resulting in the most desirable results in
    terms of matching time series that are similarly not anomalous.
:vartype IONOSPHERE_ECHO_MINMAX_SCALING_FEATURES_PERCENT_SIMILAR: float
"""

IONOSPHERE_LAYERS_USE_APPROXIMATELY_CLOSE = True
"""
:var IONOSPHERE_LAYERS_USE_APPROXIMATELY_CLOSE: The D and E boundary limits will
    be matched if the value is approximately close to the limit.  This is only
    implemented on boundary values that are > 10.  The approximately close value
    is calculated as within 10 percent for limit values between 11 and 29 and
    within 5 percent when the limit value >= 30.  It is only applied to the D
    layer with a '>' or '>=' condition and to the E layer with a '<' or '<='
    condition.
:vartype IONOSPHERE_LAYERS_USE_APPROXIMATELY_CLOSE: boolean
"""

IONOSPHERE_LEARN = True
"""
:var IONOSPHERE_LEARN: Whether Ionosphere is set to learn
:vartype IONOSPHERE_LEARN: boolean

.. note:: The below ``IONOSPHERE_LEARN_DEFAULT_`` variables are all overrideable in
    the IONOSPHERE_LEARN_NAMESPACE_CONFIG tuple per defined metric namespace
    further to this ALL metrics and their settings in terms of the Ionosphere
    learning context can also be modified via the webapp UI Ionosphere section.
    These settings are the defaults that are used in the creation of learnt
    features profiles and new metrics, HOWEVER the database is the preferred
    source of truth and will always be referred to first and the default or
    settings.IONOSPHERE_LEARN_NAMESPACE_CONFIG values shall only be used if
    database values are not determined. These settings are here so that it is
    easy to paint all metrics and others specifically as a whole, once a metric
    is added to Ionosphere via the creation of a features profile, it is painted
    with these defaults or the appropriate namespace settings in
    settings.IONOSPHERE_LEARN_NAMESPACE_CONFIG

.. warning:: Changes made to a metric settings in the database directly via the
    UI or your own SQL will not be overridden ``IONOSPHERE_LEARN_DEFAULT_``
    variables or the IONOSPHERE_LEARN_NAMESPACE_CONFIG tuple per defined metric
    namespace even if the metric matches the namespace, the database is the
    source of truth.

"""

IONOSPHERE_LEARN_DEFAULT_MAX_GENERATIONS = 16
"""
:var IONOSPHERE_LEARN_DEFAULT_MAX_GENERATIONS: The maximum number of generations
    that Ionosphere can automatically learn up to from the original human created
    features profile within the IONOSPHERE_DEFAULT_MAX_PERCENT_DIFF_FROM_ORIGIN
    Overridable per namespace in settings.IONOSPHERE_LEARN_NAMESPACE_CONFIG
    and via webapp UI to update DB
:vartype IONOSPHERE_LEARN_DEFAULT_MAX_GENERATIONS: int
"""

# TODO - testing to 100, tested and set as default, leaving 7 as a very
# conservative option for anyone initial scared of what it may do :)
# IONOSPHERE_LEARN_DEFAULT_MAX_PERCENT_DIFF_FROM_ORIGIN = 7.0
IONOSPHERE_LEARN_DEFAULT_MAX_PERCENT_DIFF_FROM_ORIGIN = 100.0
"""
:var IONOSPHERE_LEARN_DEFAULT_MAX_PERCENT_DIFF_FROM_ORIGIN: The maximum percent
    that an automatically generated features profile can be from the original
    human created features profile, any automatically generated features profile
    with the a greater percent difference above this value when summed common
    features are calculated will be discarded.  Anything below this value will
    be considered a valid learned features profile.
:vartype IONOSPHERE_LEARN_DEFAULT_MAX_PERCENT_DIFF_FROM_ORIGIN: float

.. note:: This percent value will match -/+ e.g. works both ways x percent above
    or below.  In terms of comparisons, a negative percent is simply multiplied
    by -1.0.  The lower the value, the less Ionosphere can learn, to literally
    disable Ionosphere learning set this to 0.  The difference can be much
    greater than 100, but between 7 and 100 is reasonable for learning.  However
    to really disable learning, also set all max_generations settings to 1.

"""

IONOSPHERE_LEARN_DEFAULT_FULL_DURATION_DAYS = 30
"""
:var IONOSPHERE_LEARN_DEFAULT_FULL_DURATION_DAYS: The default full duration in
    in days at which Ionosphere should learn, the default is 30 days.
    Overridable per namespace in settings.IONOSPHERE_LEARN_NAMESPACE_CONFIG
:vartype IONOSPHERE_LEARN_DEFAULT_FULL_DURATION_DAYS: int
"""

IONOSPHERE_LEARN_DEFAULT_VALID_TIMESERIES_OLDER_THAN_SECONDS = 3661
"""
:var IONOSPHERE_LEARN_VALID_TIMESERIES_OLDER_THAN_SECONDS: The number of seconds that
    Ionosphere should wait before surfacing the metric timeseries for to learn
    from.  What Graphite aggregration do you want the retention to run before
    querying it to learn from?
    Overridable per namespace in settings.IONOSPHERE_LEARN_NAMESPACE_CONFIG
:vartype IONOSPHERE_LEARN_DEFAULT_VALID_TIMESERIES_OLDER_THAN_SECONDS: int
"""

IONOSPHERE_LEARN_NAMESPACE_CONFIG = (
    ('skyline_test.alerters.test', 30, 3661, 16, 100.0),
    # Learn all Ionosphere enabled metrics at 30 days, allow 16 generations and
    # a learnt features profile can be 100% different for the origin features
    # profile
    ('.*', 30, 3661, 16, 100.0),
)
"""
:var IONOSPHERE_LEARN_NAMESPACE_CONFIG: Configures specific namespaces at
    specific learning full duration in days.
    Overrides settings.IONOSPHERE_LEARN_DEFAULT_FULL_DURATION_DAYS,
    settings.IONOSPHERE_LEARN_DEFAULT_VALID_TIMESERIES_OLDER_THAN_SECONDS,
    settings.IONOSPHERE_MAX_GENERATIONS and settings.IONOSPHERE_MAX_PERCENT_DIFF_FROM_ORIGIN
    per defined namespace, first matched, used.  Order highest to lowest
    namespace resoultion. Like settings.ALERTS, you know how this works now...
:vartype IONOSPHERE_LEARNING: tuples

This is the config by which each declared namespace can be assigned a learning
full duration in days.  It is here to allow for overrides so that if a metric
does not suit being learned at say 30 days, it could be learned at say 14 days
instead if 14 days was a better suited learning full duration.

To specifically disable learning on a namespace, set LEARN_FULL_DURATION_DAYS
to 0

- **Tuple schema example**::

    IONOSPHERE_LEARN_NAMESPACE_CONFIG = (
        # ('<metric_namespace>', LEARN_FULL_DURATION_DAYS,
        #  LEARN_VALID_TIMESERIES_OLDER_THAN_SECONDS, MAX_GENERATIONS,
        #  MAX_PERCENT_DIFF_FROM_ORIGIN),
        # Wildcard namespaces can be used as well
        ('metric3.thing\\..*', 90, 3661, 16, 100.0),
        ('metric4.thing\\..*.\\.requests', 14, 3661, 16, 100.0),
        # However beware of wildcards as the above wildcard should really be
        ('metric4.thing\\..*.\\.requests', 14, 7261, 3, 7.0),
        # Disable learning on a namespace
        ('metric5.thing\\..*.\\.rpm', 0, 3661, 5, 7.0),
        # Learn all Ionosphere enabled metrics at 30 days
        ('.*', 30, 3661, 16, 100.0),
    )

- Namespace tuple parameters are:

:param metric_namespace: metric_namespace pattern
:param LEARN_FULL_DURATION_DAYS: The number of days that Ionosphere should
    should surface the metric timeseries for
:param LEARN_VALID_TIMESERIES_OLDER_THAN_SECONDS: The number of seconds that
    Ionosphere should wait before surfacing the metric timeseries for to learn
    from.  What Graphite aggregration do you want the retention at before
    querying it to learn from?  REQUIRED, NOT optional, we could use the
    settings.IONOSPHERE_LEARN_DEFAULT_VALID_TIMESERIES_OLDER_THAN_SECONDS but
    that be some more conditionals, that we do not need, be precise, by now if
    you are training Skyline well you will understand, be precise helps :)
:param MAX_GENERATIONS: The maximum number of generations that Ionosphere can
    automatically learn up to from the original human created features profile
    on this metric namespace.
:param MAX_PERCENT_DIFF_FROM_ORIGIN: The maximum percent that an automatically
    generated features profile can be from the original human created features
    profile for a metric in the namespace.
:type metric_namespace: str
:type LEARN_FULL_DURATION_DAYS: int
:type LEARN_VALID_TIMESERIES_OLDER_THAN_SECONDS: int
:type MAX_GENERATIONS: int

"""

IONOSPHERE_AUTOBUILD = True
"""
:var IONOSPHERE_AUTOBUILD: Make best effort attempt to auto provision any
    features_profiles directory and resources that have been deleted or are
    missing.  NOT IMPLEMENTED YET
:vartype IONOSPHERE_AUTOBUILD: boolean

.. note:: This is highlighted as a setting as the number of features_profiles
    dirs that Ionosphere learn could spawn and the amount of data storage that
    would result is unknown at this point. It is possible the operator is going
    to need to prune this data a lot of which will probably never be looked
    at. Or a Skyline node is going to fail, not have the features_profiles dirs
    backed up and all the data is going to be lost or deleted. So it is possible for
    Ionosphere to created all the human interrupted resources for the features
    profile back under a best effort methodology. Although the original Redis graph
    image would not be available, nor the Graphite graphs in the resolution at which
    the features profile was created, however the fp_ts is available so the Redis
    plot could be remade and all the Graphite graphs could be made as best effort
    with whatever resoultion is available for that time period.
    This allows the operator to delete/prune feature profile dirs by possibly least
    matched by age, etc or all and still be able to surface the available features
    profile page data on-demand.  NOT IMPLEMENTED YET

"""

IONOSPHERE_UNTRAINABLES = []
"""
:var IONOSPHERE_UNTRAINABLES: a list of metric names or namespaces that should
    be deemed as untrainable.  For example you do not want to allow
    http.status.500 to be trained that an occassional 1 or 2 errors is normal
    and can be expected.
:vartype IONOSPHERE_UNTRAINABLES: list
"""

IONOSPHERE_PERFORMANCE_DATA_POPULATE_CACHE = False
"""
:var IONOSPHERE_PERFORMANCE_DATA_POPULATE_CACHE: whether metrics_manager should
    populate the performance cache items at the beginning of the data.  Under
    normal circumstances this is not required, it just make the Ionosphere
    performance graphs build quicker.
:vartype IONOSPHERE_PERFORMANCE_DATA_POPULATE_CACHE: boolean
"""

IONOSPHERE_PERFORMANCE_DATA_POPULATE_CACHE_DEPTH = 0
"""
:var IONOSPHERE_PERFORMANCE_DATA_POPULATE_CACHE_DEPTH: namespace element depth
    to cache.  If IONOSPHERE_PERFORMANCE_DATA_POPULATE_CACHE is enabled this is
    the number of namespace elements (0 indexed) to populate the cache to
    (including :mod:`settings.FULL_NAMESPACE` for which we shall use ``metrics.``
    in the example below).  For example take the metric namespaces,
    metrics.stats.* and metrics.telegraf.* if you wanted to cache the
    performance data for the metrics.stats.* namespace as a whole and the
    metrics.telegraf.* namespace as a whole, you would set this to 1.
    0 would cache perfromance data for all metrics
    1 would cache perfromance data for the sum of all metrics.*, and sum of all
    metrics.stats.* and sum of all metrics.telegraf.*. e.g. 0 is implied
:vartype IONOSPHERE_PERFORMANCE_DATA_POPULATE_CACHE_DEPTH: int
"""

IONOSPHERE_INFERENCE_MOTIFS_ENABLED = True
"""
:var IONOSPHERE_INFERENCE_MOTIFS_ENABLED: Whether to have Ionosphere use the
    motif similarily matching method, based on mass-ts.
:vartype IONOSPHERE_INFERENCE_MOTIFS_ENABLED: boolean
"""

IONOSPHERE_INFERENCE_MOTIFS_SETTINGS = {
    'default_inference_batch_sizes': {
        1440: {'top_matches': 50, 'max_distance': 30, 'max_area_percent_diff': 20.0, 'range_padding_percent': 10.0, 'find_exact_matches': False},
        720: {'top_matches': 50, 'max_distance': 25, 'max_area_percent_diff': 20.0},
        360: {'top_matches': 50, 'max_distance': 23, 'max_area_percent_diff': 20.0},
        180: {'top_matches': 50, 'max_distance': 22, 'max_area_percent_diff': 20.0, 'find_exact_matches': False},
    },
}
"""
:var IONOSPHERE_INFERENCE_MOTIFS_SETTINGS: variables required for motif analysis
    can be added per namespace.  Each variable is explained below.  If
    :mod:`settings.IONOSPHERE_INFERENCE_MOTIFS_ENABLED` is enabled a
    default_inference_batch_sizes must exist defining the default variables for
    all namespaces.  Additional namespaces can be added with different variables
    if the defaults are not applicable to the namespace.  It is important to
    note that the minimum batch_size should not be less than 180 data points so
    as to represent a large enough sample of the data to exclude repetitive
    volatility shifts.  Any batch_size setting of less that 180 **can** result
    in false negatives.
:vartype IONOSPHERE_INFERENCE_MOTIFS_SETTINGS: dict

    IONOSPHERE_INFERENCE_MOTIFS_SETTINGS = {
        'default_inference_batch_sizes': {
            1440: {'top_matches': 50, 'max_distance': 30, 'max_area_percent_diff': 20.0, 'find_exact_matches': False},
            720: {'top_matches': 50, 'max_distance': 25, 'max_area_percent_diff': 20.0},
            360: {'top_matches': 50, 'max_distance': 23, 'max_area_percent_diff': 20.0},
            180: {'top_matches': 50, 'max_distance': 22, 'max_area_percent_diff': 20.0, 'find_exact_matches': True},
        },
        'telegraf.server1': {
            360: {'top_matches': 50, 'max_distance': 30, 'max_area_percent_diff': 20.0},
            180: {'top_matches': 10, 'max_distance': 15, 'max_area_percent_diff': 20.0},
        },
    }

"""

IONOSPHERE_INFERENCE_MOTIFS_TOP_MATCHES = 50
"""
:var IONOSPHERE_INFERENCE_MOTIFS_TOP_MATCHES: The total number of similar motifs
    to return. 10 is too little, 100 does not surface more, so 50 it is. Default
    if not defined.
:vartype IONOSPHERE_INFERENCE_MOTIFS_TOP_MATCHES: int
"""

IONOSPHERE_INFERENCE_MASS_TS_MAX_DISTANCE = 20.0
"""
:var IONOSPHERE_INFERENCE_MASS_TS_MAX_DISTANCE: The maximum mass-ts distance
    value to consider a motif as similar.  Any motif with a distance value above
    this is not similar enough to be used.  0.0 being exactly the same. Default
    if not defined.
:vartype IONOSPHERE_INFERENCE_MASS_TS_MAX_DISTANCE: float
"""

IONOSPHERE_INFERENCE_MOTIFS_RANGE_PADDING = 10.0
"""
:var IONOSPHERE_INFERENCE_MOTIFS_RANGE_PADDING: The amount of padding to be
    added to found similar motifs in terms of percentage of the max(y) and
    min(y) in the motif.  If the values in a potentially anomalous motif fall
    within the padded range, the motif is matched as not anomalous.  A lower
    value will result in less matches, a too high values will result in false
    negatives (not desirable).  Default if not defined.
:vartype IONOSPHERE_INFERENCE_MOTIFS_RANGE_PADDING: float
"""

IONOSPHERE_INFERENCE_MOTIFS_SINGLE_MATCH = True
"""
:var IONOSPHERE_INFERENCE_MOTIFS_SINGLE_MATCH: ADVANCED FEATURE.  By default
    Ionosphere returns as not_anomalous on the best matching motif/shapelet
    that is found.  However if this setting is set to False, Ionosphere
    return as not anomalous with ALL the matched shapelets.  This is useful for
    testing and debugging when using
    :mod:`settings.IONOSPHERE_INFERENCE_MOTIFS_TEST_ONLY`
:vartype IONOSPHERE_INFERENCE_MOTIFS_SINGLE_MATCH: boolean
"""

IONOSPHERE_INFERENCE_MOTIFS_TEST_ONLY = False
"""
:var IONOSPHERE_INFERENCE_MOTIFS_TEST: ADVANCED FEATURE.  If this is set to
    True, inference will only record results for testing purposes, Ionosphere
    will not classify any checks as not anomalous even if similar motifs were
    found, inference will simply record the results in the database as normal in
    the motif_matched database table, no updated will be made to the ionosphere
    or the ionosphere_matched.  If you do wish to run inference in test mode you
    can set the above IONOSPHERE_INFERENCE_MOTIFS_SINGLE_MATCH setting to False
    as well to enable inference to check ALL trained data at all durations and
    record all valid similar motifs and not return on the first match on found.
:vartype IONOSPHERE_INFERENCE_MOTIFS_TEST_ONLY: boolean
"""

MEMCACHE_ENABLED = False
"""
:var MEMCACHE_ENABLED: Enables the use of memcache in Ionosphere to optimise
    DB usage [USER_DEFINED]
:vartype MEMCACHE_ENABLED: boolean
"""

MEMCACHED_SERVER_IP = '127.0.0.1'
"""
:var MEMCACHE_SERVER_IP: The IP address of the memcached server
:vartype MEMCACHE_SERVER_IP: str
"""

MEMCACHED_SERVER_PORT = 11211
"""
:var MEMCACHE_SERVER_PORT: The port of the memcached server
:vartype MEMCACHE_SERVER_IP: int
"""

"""
Luminosity settings
"""

LUMINOSITY_PROCESSES = 1
"""
:var LUMINOSITY_PROCESSES: This is the number of Luminosity processes to run.
:vartype LUMINOSITY_PROCESSES: int
"""

ENABLE_LUMINOSITY_DEBUG = False
"""
:var ENABLE_LUMINOSITY_DEBUG: To allow luminosity debug logging.
:vartype ENABLE_LUMINOSITY_DEBUG: boolean
"""

LUMINOSITY_DATA_FOLDER = '/opt/skyline/luminosity'
"""
:var LUMINOSITY_DATA_FOLDER: This is the path for luminosity data where
    classify_metrics plot images, etc are stored - absolute path.
:vartype LUMINOSITY_DATA_FOLDER: str
"""

OTHER_SKYLINE_REDIS_INSTANCES = []
"""
:var OTHER_SKYLINE_REDIS_INSTANCES: This a nested list of any Redis instances
    that Skyline should query for correlation time series ONLY applicable if
    there are multiple Skyline instances each with their own Redis.
:vartype OTHER_SKYLINE_REDIS_INSTANCES: list

THIS IS TO BE DEPRECATED IN v.1.2.5, there is no longer a requirement to access
Redis remotely between Skyline instances, this has been replaced by a api
method which uses the REMOTE_SKYLINE_INSTANCES settings below as of v1.2.4.
For example, the IP or FQDN as a string and the port as an int and the Redis
password as a str OR if there is no password the boolean None:
OTHER_SKYLINE_REDIS_INSTANCES = [['192.168.1.10', 6379, 'this_is_the_redis_password'], ['192.168.1.15', 6379, None]]

.. note:: If you run multiple Skyline instances and are going to run cross
    correlations and query another Redis please ensure that you have Redis
    authentication enabled.  See https://redis.io/topics/security and
    http://antirez.com/news/96 for more info.

"""

ALTERNATIVE_SKYLINE_URLS = []
"""
:var ALTERNATIVE_SKYLINE_URLS: The alternative URLs of any other Skyline
    instances. This ONLY applicable if there are multiple Skyline instances each
    with their own Redis.
:vartype ALTERNATIVE_SKYLINE_URLS: list

For example (note NO trailing slash):
ALTERNATIVE_SKYLINE_URLS = ['http://skyline-na.example.com:8080','http://skyline-eu.example.com']
"""

REMOTE_SKYLINE_INSTANCES = []
"""
:var REMOTE_SKYLINE_INSTANCES: ADVANCED FEATURE - This a nested list of any remote
    instances that Skyline should query for correlation time series this is ONLY
    applicable if there are multiple Skyline instances each with their own
    Redis data.  This is for Skyline Luminosity to query other Skyline instances
    via the luminosity_remote_data API get the relevant time series fragments,
    by default the previous 12 minutes, for all the metrics on the other Skyline
    instance/s (gizpped) in order to run correlations in all metrics in the
    population.  Related settings are :mod:`settings.HORIZON_SHARDS` and
    :mod:`settings.SYNC_CLUSTER_FILES`
:vartype REMOTE_SKYLINE_INSTANCES: list

**For example**, the IP or FQDN, the username, password and hostname as strings str::

    REMOTE_SKYLINE_INSTANCES = [
        ['http://skyline-na.example.com:8080','remote_WEBAPP_AUTH_USER','remote_WEBAPP_AUTH_USER_PASSWORD', 'skyline-1'],
        ['http://skyline-eu.example.com', 'another_remote_WEBAPP_AUTH_USER','another_WEBAPP_AUTH_USER_PASSWORD', 'skyline-2']]


The hostname element must match the hostnames in :mod:`settings.HORIZON_SHARDS`
for the instances to be able to determine which Skyline instance is authorative
for a metric.
"""

CORRELATE_ALERTS_ONLY = True
"""
:var CORRELATE_ALERTS_ONLY: Only cross correlate anomalies the have an alert
    setting (other than syslog).  This reduces the number of correlations that
    are recorded in the database.  Non alerter metrics are still however cross
    correlated against when an anomaly triggers on an alerter metric.
:vartype CORRELATE_ALERTS_ONLY: boolean
"""

LUMINOL_CROSS_CORRELATION_THRESHOLD = 0.9
"""
:var LUMINOL_CROSS_CORRELATION_THRESHOLD: Only record Luminol cross correlated
    metrics where the correlation coefficient is > this float value.  Linkedin's
    Luminol library is hardcoded to 0.8, however with lots of testing 0.8 proved
    to be too low a threshold and resulted in listing many metrics that were
    not related.  You may find 0.9 too low as well, it can also record a lot,
    however in root cause analysis and determining relationships between metrics
    0.9 has proved more useful for in seeing the tress in the forest.
    This can be a value between 0.0 and 1.00000 - 1.0 being STRONGEST cross
    correlation
:vartype LUMINOL_CROSS_CORRELATION_THRESHOLD: float
"""

LUMINOSITY_RELATED_TIME_PERIOD = 240
"""
:var LUMINOSITY_RELATED_TIME_PERIOD: The time period (in seconds) either side of
    the anomaly that should be checked to report possible related anomalies.
:vartype LUMINOSITY_RELATED_TIME_PERIOD: int
"""

LUMINOSITY_CORRELATE_ALL = True
"""
:var LUMINOSITY_CORRELATE_ALL: By default all metrics will be correlated with
    the entire metric population.
:vartype LUMINOSITY_CORRELATE_ALL: boolean
"""

LUMINOSITY_CORRELATE_NAMESPACES_ONLY = []
"""
:var LUMINOSITY_CORRELATE_NAMESPACES_ONLY: A list of namespaces that metrics in
    the same namespace should be correlated with.  The default is an empty list
    which results in all metrics being correlated with all metrics.  If
    namespaces are declared in the list, all metrics will be evaluated as to
    whether they are in the list.  Metrics will be evaluated against namespaces
    in this list using :func:`.matched_or_regexed_in_list` which determines if a
    pattern is in a list as a: 1) absolute match 2) match been dotted elements
    3) matched by a regex. Metrics in the list will only be correlated with
    metrics in the same namespace and excluded from correlations within ANY
    other namespace, unless defined in the below
    mod:`settings.LUMINOSITY_CORRELATION_MAPS` method.
:vartype LUMINOSITY_CORRELATE_NAMESPACES_ONLY: list

- **List example**::

    LUMINOSITY_CORRELATE_NAMESPACES_ONLY = [
        'aws.euw1',
        'aws.use1',
        'gcp.us-east4',
    }

In the above example, metrics with the ``aws.euw1`` namespace would only be
correlated against other ``aws.euw1`` metrics, likewise for ``aws.use1`` and
``gcp.us-east4``.  This also means that if there were other metric namespaces
like ``gcp.southamerica-east1`` and ``gcp.asia-east1`` they would not be
correlated with any of the above example namespaces.
"""

LUMINOSITY_CORRELATION_MAPS = {}
"""
:var LUMINOSITY_CORRELATION_MAPS: A dictionary of lists of metrics
    which should be correlated.  These lists hold absolute metric names, to
    correlate using namespaces use the above
    mod:`settings.LUMINOSITY_CORRELATE_NAMESPACES_ONLY` method.  Although both
    methods can be run simultaneosly, this method allows for only correlating
    specific groups of metrics.  It be used to on its own only correlate certain
    metrics if nothing is defined in
    mod:`settings.LUMINOSITY_CORRELATE_NAMESPACES_ONLY`.  Or it can also be used
    in conjunction with mod:`settings.LUMINOSITY_CORRELATE_NAMESPACES_ONLY` to
    also correlate metrics in different namespaces.  For example say you
    defined `aws.euw1` and `aws.use1` as seperate namespaces in
    mod:`settings.LUMINOSITY_CORRELATE_NAMESPACES_ONLY` but you also want to
    correlate some specific group/s of metrics that occur in both `aws.euw1` and
    `aws.use1`, you could define those here.
:vartype LUMINOSITY_CORRELATION_MAPS: dictionary

- **Dictionary example**::

    LUMINOSITY_CORRELATION_MAPS = {
        'aws.webservers.nginx': [
            'aws.euw1.webserver-1.nginx.apm.mainsite.avg_request_timing',
            'aws.euw1.webserver-2.nginx.apm.mainsite.avg_request_timing',
            'aws.use1.webserver-6.nginx.apm.mainsite.avg_request_timing',
            'aws.use1.webserver-8.nginx.apm.mainsite.avg_request_timing',
        ]
    }

Note that if both methods are enabled, correlations will be done on a common
result of both methods, meaning that a metric will be evaluated against both
methods and the resulting list of metrics that it should be correlated against
will be used.

"""

LUMINOSITY_CLASSIFY_METRICS_LEVEL_SHIFT = False
"""
:var LUMINOSITY_CLASSIFY_METRICS_LEVEL_SHIFT: ADVANCED FEATURE. Enable
    luminosity/classify_metrics to identify metrics that experience significant
    level shifts.
:vartype LUMINOSITY_CLASSIFY_METRICS_LEVEL_SHIFT: boolean
"""

LUMINOSITY_LEVEL_SHIFT_SKIP_NAMESPACES = []
"""
:var LUMINOSITY_LEVEL_SHIFT_SKIP_NAMESPACES: namespace to skip level shift
    classification.
:vartype LUMINOSITY_LEVEL_SHIFT_SKIP_NAMESPACES: list

These are metrics that you want luminosity to not attempt to classify as level
shift metrics, generally sparsely populated namespaces. Works in the same way
that SKIP_LIST works, it matches in the string or dotted namespace elements.
"""

LUMINOSITY_CLASSIFY_ANOMALIES = False
"""
:var LUMINOSITY_CLASSIFY_ANOMALIES: Whether to classify anomaly types.
:vartype LUMINOSITY_CLASSIFY_ANOMALIES: boolean
"""

LUMINOSITY_CLASSIFY_ANOMALY_ALGORITHMS = [
    'adtk_level_shift', 'adtk_volatility_shift', 'adtk_persist',
    'adtk_seasonal',
]
"""
:var LUMINOSITY_CLASSIFY_ANOMALY_ALGORITHMS: ADVANCED FEATURE. List of custom
    algorithms to be used for classifying anomalies.
:vartype LUMINOSITY_CLASSIFY_ANOMALIES: list
"""

LUMINOSITY_CLASSIFY_ANOMALIES_SAVE_PLOTS = False
"""
:var LUMINOSITY_CLASSIFY_ANOMALIES_SAVE_PLOTS: ADVANCED FEATURE. Whether to save
    anomaly classification plots in the training data.
:vartype LUMINOSITY_CLASSIFY_ANOMALIES_SAVE_PLOTS: boolean
"""

LUMINOSITY_CLOUDBURST_ENABLED = True
"""
:var LUMINOSITY_CLOUDBURST_ENABLED: Whether to enable Luminosity cloudburst to
    run and identify significant changepoints
:vartype LUMINOSITY_CLOUDBURST_ENABLED: boolean
"""

LUMINOSITY_CLOUDBURST_PROCESSES = 1
"""
:var LUMINOSITY_CLOUDBURST_PROCESSES: The number of processes that
    luminosity/cloudbursts should divide all the metrics up between to identify
    significant changepoints in the metrics.  In a very large metric population
    you may need to set this to more than 1.  As a guide a single process can
    generally analyse and identify potentially significant changepoints in about
    1500 metrics in 60 seconds.
:vartype LUMINOSITY_CLOUDBURST_PROCESSES: int
"""

LUMINOSITY_CLOUDBURST_RUN_EVERY = 900
"""
:var LUMINOSITY_CLOUDBURST_RUN_EVERY: This is how often to run
    luminosity/cloudbursts to identify significant changepoints in metrics.  To
    disable luminosity/cloudbursts set this to 0.
:vartype LUMINOSITY_CLOUDBURST_RUN_EVERY: int
"""

LUMINOSITY_RELATED_METRICS = False
"""
:var LUMINOSITY_RELATED_METRICS: Whether to enable Luminosity related_metrics to
    run and learn related metrics.
:vartype LUMINOSITY_RELATED_METRICS: boolean
"""

LUMINOSITY_RELATED_METRICS_MAX_5MIN_LOADAVG = 2.0
"""
:var LUMINOSITY_RELATED_METRICS_MAX_5MIN_LOADAVG: The Luminosity related_metrics
    will ONLY run When the 5min loadavg on the machine running the process is
    BELOW this loadavg.  Because the process is an offline process it can run as
    and when appropriate.
:vartype LUMINOSITY_RELATED_METRICS_MAX_5MIN_LOADAVG: float
"""

LUMINOSITY_RELATED_METRICS_MIN_CORRELATION_COUNT_PERCENTILE = 95.0
"""
:var LUMINOSITY_RELATED_METRICS_MIN_CORRELATION_COUNT_PERCENTILE: The percentile
    of cross correlation counts to be included in the related metrics
    evaluation.  This value should be in the range of >= 95.0 to ensure that
    metrics are only related when there is a high degree of confidence.
    This results in only assessing the metrics which have the highest number of
    cross correlations being considered to be related.  Its purpose is to
    discard the outlier, numeric significance only recorded correlations.
:vartype LUMINOSITY_RELATED_METRICS_MIN_CORRELATION_COUNT_PERCENTILE: float
"""

LUMINOSITY_RELATED_METRICS_MINIMUM_CORRELATIONS_COUNT = 3
"""
:var LUMINOSITY_RELATED_METRICS_MINIMUM_CORRELATIONS_COUNT: The minimum number
    of cross correlations recorded to include the metric for evaluation in
    get_cross_correlation_relationships.  This number should not be less than 3
    to ensure that metrics are only related when there is a high degree of
    confidence.
:vartype LUMINOSITY_RELATED_METRICS_MINIMUM_CORRELATIONS_COUNT: int
"""

"""
Docker settings
"""

DOCKER = False
"""
:var DOCKER: Whether Skyline is running on Docker or not
:vartype DOCKER: boolean
"""

DOCKER_DISPLAY_REDIS_PASSWORD_IN_REBROW = False
"""
:var DOCKER_DISPLAY_REDIS_PASSWORD_IN_REBROW: Whether to show the Redis password
    in the webapp Rebrow login page
:vartype DOCKER_DISPLAY_REDIS_PASSWORD_IN_REBROW: boolean
"""

DOCKER_FAKE_EMAIL_ALERTS = False
"""
:var DOCKER_FAKE_EMAIL_ALERTS: Whether to make docker fake email alerts.  At the
    moment docker has no support to send email alerts, however an number of
    Ionosphere resources are created when a email alert is sent.  Therefore in
    the docker context email alerts are processed only the SMTP action is not
    run.  If Skyline is running on docker, this must be set to True.
:vartype DOCKER_FAKE_EMAIL_ALERTS: boolean
"""

"""
Flux settings
"""

FLUX_IP = '127.0.0.1'
"""
:var FLUX_IP: The IP to bind the gunicorn flux server on.
:vartype FLUX_IP: str
"""

FLUX_PORT = 8000
"""
:var FLUX_PORT: The port for the gunicorn flux server to listen on.
:vartype FLUX_PORT: int
"""

FLUX_WORKERS = 1
"""
:var FLUX_WORKERS: The number of gunicorn flux workers
:vartype FLUX_WORKERS: int
"""

FLUX_VERBOSE_LOGGING = True
"""
:var FLUX_VERBOSE_LOGGING: If set to True flux will log the data recieved in
    requests and the data sent to Graphite.  It is sent to True by default as it
    was the default before this option was added.  If flux is going to ingest
    1000s of metrics consider setting this to False.
:vartype FLUX_VERBOSE_LOGGING: boolean
"""

FLUX_SELF_API_KEY = 'YOURown32charSkylineAPIkeySecret'
"""
:var FLUX_SELF_API_KEY: this is a 32 alphanumeric string that is used to
    validate direct requests to Flux. Vista uses it and connects directly to
    Flux and bypass the reverse proxy and authenticates itself.  It can only be
    digits and letters e.g. [0-9][a-Z] [USER_DEFINED]
:vartype FLUX_SELF_API_KEY: str
"""

FLUX_API_KEYS = {}
"""
:var FLUX_API_KEYS: The flux /flux/metric_data and /flux/metric_data_post
    endpoints are controllered via API keys.  Each API key can additionally
    specific a metric namespace prefix to prefix all metric names submitted with
    the key with the defined namespace prefix.
    Each API key must be a 32 character alphanumeric string [a-Z][0-9]
    The trailing dot of the namespace prefix must not be specified it will be
    automatically added as the separator between the namespace prefix and the
    metric name. For more see
    https://earthgecko-skyline.readthedocs.io/en/latest/upload-data-to-flux.html
:vartype FLUX_API_KEYS: dict

- **Example**::

    FLUX_API_KEYS = {
        'ZlJXpBL6QVuZg5KL4Vwrccvl8Bl3bBjC': 'warehouse-1',
        'KYRsv508FJpVg7pr11vnZTbeu11UvUqR': 'warehouse-1'  # allow multiple keys for a namesapce to allow for key rotation
        'ntG9Tlk74FeV7Muy65EdHbZ07Mpvj7Gg': 'warehouse-2.floor.1'
    }

"""

FLUX_BACKLOG = 254
"""
:var FLUX_BACKLOG: The maximum number of pending connections.  This refers to
    the number of clients that can be waiting to be served. Exceeding this number
    results in the client getting an error when attempting to connect. It should
    only affect servers under significant load.  As per
    http://docs.gunicorn.org/en/stable/settings.html#backlog
:vartype FLUX_WORKERS: int
"""

FLUX_MAX_AGE = 3600
"""
:var FLUX_MAX_AGE: The maximum age of a timestamp that flux will accept as valid.
    This can vary depending on retentions in question and this method may be
    altered in a future release.
:vartype FLUX_MAX_AGE: int
"""

FLUX_PERSIST_QUEUE = False
"""
:var FLUX_PERSIST_QUEUE: By default flux does not persist the incoming queue on
    a flux restart any metrics in the queue will be lost.  If flux is only
    accepting a small amount of data points, flux will probably get through the
    queue in seconds.  However if 1000s of metrics are being sent to flux per
    minute and you do not want to lose any metrics and data points when flux is
    restarted you can set flux to persist the queue, which is done via the
    flux.queue Redis set.  Persisting the queue has a computational cost and
    if the queue is large then when flux restarts it may lag and not get through
    all the queue for some time unless you adjust the amount of FLUX_WORKERS.
:vartype FLUX_PERSIST_QUEUE: boolean
"""

FLUX_CHECK_LAST_TIMESTAMP = True
"""
:var FLUX_CHECK_LAST_TIMESTAMP: By default flux deduplicates data and only
    allows for one data point to be submitted per timestamp, however this has a
    cost in terms of requests and number of keys in Redis.  If you have lots of
    metrics coming into flux consider setting this to ``False`` and ensure that
    the application/s submitting data to flux do not submit data unordered.
:vartype FLUX_CHECK_LAST_TIMESTAMP: boolean
"""

FLUX_SEND_TO_CARBON = False
"""
:var FLUX_SEND_TO_CARBON: Whether to send metrics recieved by flux to
    Graphite.
:vartype FLUX_SEND_TO_CARBON: boolean
"""

FLUX_CARBON_HOST = CARBON_HOST
"""
:var FLUX_CARBON_HOST: The carbon host that flux should send metrics to if
    FLUX_SEND_TO_CARBON is enabled.
:vartype FLUX_CARBON_HOST: str
"""

FLUX_CARBON_PORT = CARBON_PORT
"""
:var FLUX_CARBON_PORT: The carbon host port that flux should send metrics via
    FLUX_SEND_TO_CARBON is enabled.
:vartype FLUX_CARBON_PORT: int
"""

FLUX_CARBON_PICKLE_PORT = 2004
"""
:var FLUX_CARBON_PICKLE_PORT: The port for the Carbon PICKLE_RECEIVER_PORT on
    Graphite as per defined in Graphite's carbon.conf
:vartype FLUX_CARBON_PICKLE_PORT: int
"""

FLUX_GRAPHITE_WHISPER_PATH = '/opt/graphite/storage/whisper'
"""
:var FLUX_GRAPHITE_WHISPER_PATH: This is the absolute path on your GRAPHITE server, it is
    required by flux to determine that a metric name and the path does not
    exceed the maximum maximum path of length of 4096 characters.
:vartype FLUX_GRAPHITE_WHISPER_PATH: str
"""

FLUX_PROCESS_UPLOADS = False
"""
:var FLUX_PROCESS_UPLOADS: Whether flux is enabled to process uploaded data
    files in :mod:`settings.DATA_UPLOADS_PATH`.  This is related to the
    :mod:`settings.WEBAPP_ACCEPT_DATA_UPLOADS` setting and files in
    :mod:`settings.DATA_UPLOADS_PATH` are processed and the data is sent to
    Graphite.
:vartype FLUX_PROCESS_UPLOADS: boolean
"""

FLUX_SAVE_UPLOADS = False
"""
:var FLUX_SAVE_UPLOADS: Whether flux should save processed upload data in
    :mod:`settings.FLUX_SAVE_UPLOADS_PATH`.
:vartype FLUX_SAVE_UPLOADS: boolean
"""

FLUX_SAVE_UPLOADS_PATH = '/opt/skyline/flux/processed_uploads'
"""
:var FLUX_SAVE_UPLOADS_PATH: The path flux saves processed data to if
    :mod:`settings.FLUX_SAVE_UPLOADS` is True.  Note that this directory must
    exist and be writable to the user that Skyline processes are running
    as. Or the parent directory must exist and be owned by the user that Skyline
    processes are running as.
:vartype FLUX_SAVE_UPLOADS_PATH: str
"""

FLUX_UPLOADS_KEYS = {}
"""
:var FLUX_UPLOADS_KEYS: For each parent_metric_namespace a key must be assigned
    to the namespace as the upload_data endpoint is not authenticated.  For
    uploads via the webapp Flux page these are handled using the
    :mod:`settings.FLUX_SELF_API_KEY` key.
:vartype FLUX_UPLOADS_KEYS: dict

- **Example**::

    FLUX_UPLOADS_KEYS = {
        'remote_sites.warehouse.1': 'c65909df-9e06-41b7-a455-4f10b99aa741',
        'remote_sites.warehouse.2': '1e8b1c63-10d3-4a24-bb27-d2513861dbf6'
    }

"""

FLUX_ZERO_FILL_NAMESPACES = []
"""
:var FLUX_ZERO_FILL_NAMESPACES: For each namespace or namespace elements
    declared in this list, flux will send 0 if no data is recieved for a
    metric in the namespace in the last 60 seconds.  This enables Skyline to
    fill sparsely populated metrics with 0 where appropriate, continous data
    time series are better for analysing, especially in terms of features
    profiles.  The namespaces declared here can be absolute metric names,
    elements or a regex of the namespace.
:vartype FLUX_ZERO_FILL_NAMESPACES: list

- **Example**::

    FLUX_ZERO_FILL_NAMESPACES = [
        'nginx.errors',
        'external_sites.www_example_com.avg_pageload',
        'sites.avg_pageload',
    ]

"""

FLUX_LAST_KNOWN_VALUE_NAMESPACES = []
"""
:var FLUX_LAST_KNOWN_VALUE_NAMESPACES: For each namespace or namespace elements
    declared in this list, flux will send the last value if no data is recieved
    for a metric in the namespace in the last 60 seconds. The namespaces
    declared here can be absolute metric names, elements or a regex of the
    namespace.
:vartype FLUX_LAST_KNOWN_VALUE_NAMESPACES: list

- **Example**::

    FLUX_LAST_KNOWN_VALUE_NAMESPACES = [
        'external_sites.www_example_com.users',
        'external_sites.shop_example_com.products',
    ]

"""

FLUX_AGGREGATE_NAMESPACES = {}
"""
:var FLUX_AGGREGATE_NAMESPACES: For each namespace or namespace elements
    declared in this dict, flux/listen will send data points received to
    flux/aggregator which will then submit metrics to flux/worker at every
    interval, aggregating by the defined method/s. If multiple methods are used
    flux will submit a metric per method defined.
:vartype FLUX_AGGREGATE_NAMESPACES: dict

- **Example**::

    FLUX_AGGREGATE_NAMESPACES = {
        'mysite.events.loadtime': {
            'method': ['avg'],
            'interval': 60,
            'zero_fill': True,
            'last_known_value': False,
            'method_suffix': False},
        'mysite.events.pageloads': {
            'method': ['avg', 'sum', 'high'],
            'interval': 60,
            'zero_fill': False,
            'last_known_value': False,
            'method_suffix': True},
        'warehouse1.kwh.meter.reading': {
            'method': ['avg'],
            'interval': 60,
            'zero_fill': False,
            'last_known_value': False,
            'method_suffix': False},
    }
"""

FLUX_EXTERNAL_AGGREGATE_NAMESPACES = False
"""
:var FLUX_EXTERNAL_AGGREGATE_NAMESPACES: If there are aggrgegate metrics
    defined in any external settings and there are none defined in the above
    FLUX_AGGREGATE_NAMESPACES the setting can force flux to check for aggregate
    metrics from the metrics_manager.
:vartype FLUX_EXTERNAL_AGGREGATE_NAMESPACES: boolean
"""


FLUX_SEND_TO_STATSD = False
"""
:var FLUX_SEND_TO_STATSD: Whether to send metrics recieved by flux to statsd.
:vartype FLUX_SEND_TO_STATSD: boolean
"""

FLUX_STATSD_HOST = None
"""
:var FLUX_STATSD_HOST: The statsd host that flux should send metrics to if
    FLUX_SEND_TO_STATSD is enabled.
:vartype FLUX_STATSD_HOST: str
"""

FLUX_STATSD_PORT = 8125
"""
:var FLUX_STATSD_PORT: The statsd host port that flux should send metrics via
    FLUX_SEND_TO_STATSD is enabled.
:vartype FLUX_STATSD_PORT: int
"""

"""
Vista settings
"""

VISTA_ENABLED = False
"""
:var VISTA_ENABLED: Enables Skyline vista
:vartype VISTA_ENABLED: boolean
"""

VISTA_VERBOSE_LOGGING = False
"""
:var VISTA_VERBOSE_LOGGING: As of Skyline 3.0, apps log notice and errors
    only.  To have addtional info logged set this to True.  Useful for debugging
    but less verbose than LOCAL_DEBUG.
:vartype VISTA_VERBOSE_LOGGING: boolean
"""

VISTA_FETCHER_PROCESSES = 1
"""
:var VISTA_FETCHER_PROCESSES: the number of Vista fetcher processes to run.
    In all circumstances 1 process should be sufficient as the process runs
    asynchronous requests.
:vartype VISTA_FETCHER_PROCESSES: int
"""

VISTA_FETCHER_PROCESS_MAX_RUNTIME = 50
"""
:var VISTA_FETCHER_PROCESS_MAX_RUNTIME: the maximum number of seconds Vista
    fetcher process/es should run before being terminated.
:vartype VISTA_FETCHER_PROCESS_MAX_RUNTIME: int
"""

VISTA_WORKER_PROCESSES = 1
"""
:var VISTA_WORKER_PROCESSES: the number of Vista worker processes to run to
    validate and submit the metrics to Flux and Graphite.
:vartype VISTA_WORKER_PROCESSES: int
"""

VISTA_DO_NOT_SUBMIT_CURRENT_MINUTE = True
"""
:var VISTA_DO_NOT_SUBMIT_CURRENT_MINUTE: Do not resample or send data that falls
    into the current minute bin to Graphite.  This means that Skyline will only
    analyse data 60 seconds behind.  In terms of fetching high frequency data
    this should always be the default, so that Skyline is analysing the last
    complete data point for a minute and is not analysing a partially populated
    data point which will result in false positives.
:vartype VISTA_DO_NOT_SUBMIT_CURRENT_MINUTE: boolean
"""

VISTA_FETCH_METRICS = ()
"""
:var VISTA_FETCH_METRICS: Enables Skyline vista
:vartype VISTA_FETCH_METRICS: tuple

This is the config where metrics that need to be fetched are defined.

- **Tuple schema example**::

    VISTA_FETCH_METRICS = (
        # (remote_host, remote_host_type, frequency, remote_target, graphite_target, uri, namespace_prefix, api_key, token, user, password, (populate_at_resolution_1, populate_at_resolution_2, ...)),
        # Example with no authentication
        ('https://graphite.example.org', 'graphite', 60, 'stats.web01.cpu.user', 'stats.web01.cpu.user', '/render/?from=-10minutes&format=json&target=', 'vista.graphite_example_org', None, None, None, None, ('90days', '7days', '24hours', '6hours')),
        ('https://graphite.example.org', 'graphite', 60, 'sumSeries(stats.*.cpu.user)', 'stats.cumulative.cpu.user', '/render/?from=-10minutes&format=json&target=', 'vista.graphite_example_org', None, None, None, None, ('90days', '7days', '24hours', '6hours')),
        ('https://graphite.example.org', 'graphite', 3600, 'swell.tar.hm0', 'swell.tar.hm0', '/render/?from=-120minutes&format=json&target=', 'graphite_example_org', None, None, None, None, ('90days', '7days', '24hours', '6hours')),
        ('http://prometheus.example.org:9090', 'prometheus', 60, 'node_load1', 'node_load1', 'default', 'vista.prometheus_example_org', None, None, None, None, ('15d',))
        ('http://prometheus.example.org:9090', 'prometheus', 60, 'node_network_transmit_bytes_total{device="eth0"}', 'node_network_transmit_bytes_total.eth0', '/api/v1/query?query=node_network_transmit_bytes_total%7Bdevice%3D%22eth0%22%7D%5B5m%5D', 'vista.prometheus_example_org', None, None, None, None, None)
    )

- All the fetch tuple parameters are required to be present in each fetch tuple.

:param remote_host: the remote metric host base URL, including the protocol and
    port.
:param remote_host_type: the type of remote host, valid options are graphite and
    prometheus.
:param frequency: the frequency with which to fetch data in seconds.
:param remote_target: the remote target to fetch, this can be a single metric,
    a single metric with function/s applied or a series of metrics with
    function/s applied which result is a single derived time series.
:param graphite_target: the absolute metric name to be used to store in Graphite
    this excludes the namespace_prefix set by the namespace_prefix param below.
:param uri: the metric host endpoint URI used to retrieve the metric.
    FOR GRAPHITE: valid Graphite URIs are only in the form of
    `from=-<period><minutes|hours|days>``, only minutes, hours and days can be
    passed otherwise the regexs for back filling any missing data will not work.
    FOR PROMETHEUS: the uri can be passed a 'default', this will dynamically
    generate a URI in terms of the URLENCODED_TARGET, start=
    and end= parameters that are passed to query_range, based on the current
    time.  A Prometheus API URI can be passed, but the query should be URL
    encoded and cannot have any dynamic date based parameters.
:param namespace_prefix: the Graphite namespace prefix to use for submitting
    metrics to, can be passed as `''` if you do not want to prefix the metric
    names.  The namespace_prefix must NOT have a trailing dot.
:param api_key: an API key if one is required, otherwise pass the boolean None
:param token: a token if one is required, otherwise pass the boolean None
:param user: a username if one is required, otherwise pass the boolean None
:param password: a password if one is required, otherwise pass the boolean None
:param populate_at_resolutions: if you want Vista to populate the metric with
    historic data this tuple allows you to declare at what resolutions to
    populate the data with.  If you do not want to pre-populated then do not
    declare a tuple and simply pass an empty tuple (). NOTE - you cannot declare
    a resolution from Prometheus metrics which use a custom uri, historic
    metrics cannot currently be pulled with a custom uri.  For a detailed
    description of this functionality please see the Vista documentation page
    at:
    https://earthgecko-skyline.readthedocs.io/en/latest/vista.html#pre-populating-metrics-with-historic-data
    https://earthgecko-skyline.readthedocs.io/en/latest/vista.html#populate_at_resolutions
:type remote_host: str
:type remote_host_type: str
:type frequency: int
:type remote_target: str
:type graphite_target: str
:type uri: str
:type namespace_prefix: str
:type api_key:
:type token: str
:type user: str
:type password: str
:type populate_with_resolutions: tuple (with trailing comma if a single
    resolution is declared)
"""

VISTA_GRAPHITE_BATCH_SIZE = 20
"""
:var VISTA_GRAPHITE_BATCH_SIZE: The number of metrics that Vista should retrieve
    from a Graphite host in a single request, if the metrics being requested are
    being requested with the same from parameter (timestamp).
:vartype VISTA_GRAPHITE_BATCH_SIZE: int
"""

"""
SNAB settings
"""

SNAB_ENABLED = False
"""
:var SNAB_ENABLED: Whether SNAB is enabled or nor,
:vartype SNAB_ENABLED: str
"""

SNAB_DATA_DIR = '/opt/skyline/SNAB'
"""
:var SNAB_DATA_DIR: The directory where SNAB writes data files.
:vartype SNAB_DATA_DIR: str
"""

SNAB_anomalyScore = {}
"""
:var SNAB_anomalyScore: Each analysis app or all apps can record an anomalyScore
    for each analysis.  This is an advanced feature for testing and development
    purposes. NOTE that the above :mod:`settings.SNAB_ENABLED` does not have to
    be set to True for the SNAB_anomalyScore function to work.
:vartype SNAB_anomalyScore: dict

- **Examples**::

    SNAB_anomalyScore = {}

    SNAB_anomalyScore = {
        'all': ['telegraf.test-server1'],
        'analyzer': ['telegraf.test-server1'],
        'analyzer_batch': ['telegraf.test-server1', 'test_batch_metrics.'],
        'mirage': ['telegraf.test-server1', 'test_batch_metrics.'],
        'SNAB': ['\\.'],
    }

    SNAB_anomalyScore = {
        'all': [],
        'analyzer': ['telegraf.test-server1'],
        'analyzer_batch': ['telegraf.test-server1', 'test_batch_metrics.'],
        'mirage': ['telegraf.test-server1', 'test_batch_metrics.'],
    }

"""

SNAB_CHECKS = {}
"""
:var SNAB_CHECKS: ADVANCED FEATURE.  A dictionary that defines the
    any SNAB checks for apps (mirage only) in terms of what namespaces should
    be submitted to snab to be checked by which algortihm/s.
    EXPERIMENTAL.
:vartype SNAB_CHECKS: dict

- **Example**::

    SNAB_CHECKS = {
        'mirage': {
            'testing': {
                'skyline_matrixprofile': {
                    'namespaces': ['telegraf'],
                    'algorithm_source': '/opt/skyline/github/skyline/skyline/custom_algorithms/skyline_matrixprofile.py',
                    'algorithm_parameters': {'windows': 5, 'k_discords': 20},
                    'max_execution_time': 10.0,
                    'debug_logging': True,
                    'alert_slack_channel': '#skyline'
                }
            }
        },
    }

"""

SNAB_LOAD_TEST_ANALYZER = 0
"""
:var SNAB_LOAD_TEST_ANALYZER: ADVANCED and EXPERIMENTAL FEATURE.  Declare the
    number of metrics you want to load test via Analyzer.
:vartype SNAB_LOAD_TEST_ANALYZER: int
"""

SNAB_FLUX_LOAD_TEST_ENABLED = False
"""
:var SNAB_FLUX_LOAD_TEST_ENABLED: ADVANCED FEATURE.  Run flux load testing with
    snab.
:vartype SNAB_FLUX_LOAD_TEST_ENABLED: boolean
"""

SNAB_FLUX_LOAD_TEST_METRICS = 0
"""
:var SNAB_FLUX_LOAD_TEST_METRICS: ADVANCED FEATURE.  Declare the
    number of metrics you want to load test via flux.  0 disables.
:vartype SNAB_FLUX_LOAD_TEST_METRICS: int
"""

SNAB_FLUX_LOAD_TEST_METRICS_PER_POST = 480
"""
:var SNAB_FLUX_LOAD_TEST_METRICS_PER_POST: ADVANCED FEATURE.  Declare the
    number of metrics per POST to flux.  This should ideally be less than the
    Graphite (not Skyline) setting of MAX_DATAPOINTS_PER_MESSAGE which is by
    default in Graphite 500
:vartype SNAB_FLUX_LOAD_TEST_METRICS_PER_POST: int
"""

SNAB_FLUX_LOAD_TEST_NAMESPACE_PREFIX = 'test.snab.flux_load_test'
"""
:var SNAB_FLUX_LOAD_TEST_NAMESPACE_PREFIX: ADVANCED FEATURE.  Declare the
    namespace for the test metrics. This is the namespace that will appear in
    Graphite with randomly generated metric names. NO trailing dot is required.
:vartype SNAB_FLUX_LOAD_TEST_NAMESPACE_PREFIX: str
"""

"""
External settings
"""
EXTERNAL_SETTINGS = {
    'mock_api_external_settings': {
        'endpoint': 'http://127.0.0.1:1500/mock_api?test_external_settings',
        'post_data': {'token': None},
        # Or a dict of the data to POST
        # 'post_data': {'data': {'token': '123456abcdef'}},
        # 'post_data': {'auth': {'id': 286, 'key': '123456abcdef'}},
        'enabled': False
    }
}
"""
:var EXTERNAL_SETTINGS: ADVANCED FEATURE.  Skyline can fetch settings for
    namespaces from an external source. For full details of this functionality
    please see the External Settings documentation page at:
    https://earthgecko-skyline.readthedocs.io/en/latest/external_settings.html
:vartype EXTERNAL_SETTINGS: dict
"""

"""
Prometheus settings
"""
PROMETHEUS_SETTINGS = {}
"""
:var PROMETHEUS_SETTINGS: UNDER DEVELOPMENT Prometheus can be enabled to push
    metrics to Skyline which Skyline will add to Redis and analyse.  In the
    Mirage context Skyline will fetch data from Prometheus.  For full details of
    this functionality please see the Prometheus integration documentation page at:
    https://earthgecko-skyline.readthedocs.io/en/latest/prometheus.html
:vartype PROMETHEUS_SETTINGS: dict

- **Example**::

    PROMETHEUS_SETTINGS = {
        'prometheus.example.org': {
            'scheme': 'https',
            'port': '443',
            # Usernames and hashed passwords that have full access to the web
            # server via basic authentication. If empty, no basic authentication is
            # required. Passwords are hashed with bcrypt.
            'basic_auth_users': {
                'prometheus': '<bcrypt_password_str>',
            },
            'endpoint': 'api/v1',
            'format': '<influxdb|graphite>',  # default influxdb
            'learn_key_metrics': True,
            'analyse_mode': '<key_metrics_and_group|key_metrics|all>',
            'key_metrics_config': '<path_to_key_metrics_file>',
            'longterm_storage': '<promscale|thanos|cortex>',
            'longterm_storage_endpoint': '<promscale|thanos|cortex>',
        }
    }

"""
