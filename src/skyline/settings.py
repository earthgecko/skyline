"""
Shared settings
"""

# The path for the Redis unix socket
REDIS_SOCKET_PATH = '/tmp/redis.sock'

# The Skyline logs directory. Do not include a trailing slash.
LOG_PATH = '/var/log/skyline'

# The Skyline pids directory. Do not include a trailing slash.
PID_PATH = '/var/run/skyline'

# Metrics will be prefixed with this value in Redis.
FULL_NAMESPACE = 'metrics.'

# The Horizon agent will make T'd writes to both the full namespace and the
# mini namespace. Oculus gets its data from everything in the mini namespace.
MINI_NAMESPACE = 'mini.'

# This is the rolling duration that will be stored in Redis. Be sure to pick a
# value that suits your memory capacity, your CPU capacity, and your overall
# metrics count. Longer durations take a longer to analyze, but they can
# help the algorithms reduce the noise and provide more accurate anomaly
# detection.
FULL_DURATION = 86400

# This is the duration of the 'mini' namespace, if you are also using the
# Oculus service. It is also the duration of data that is displayed in the
# web app 'mini' view.
MINI_DURATION = 3600

# If you have a Graphite host set up, set this metric to get graphs on
# Skyline and Horizon. Don't include http:// since this is used for carbon host as well.
GRAPHITE_HOST = 'your_graphite_host.com'

# The Graph url used to link to Graphite (Or another graphite dashboard)
# %s will be replaced by the metric name
GRAPH_URL = 'http://' + GRAPHITE_HOST + '/render/?width=1400&from=-1hour&target=%s'

# If you have a Graphite host set up, set its Carbon port.
CARBON_PORT = 2003

# If you have Oculus set up, set this metric to set the clickthrough on the
# webapp. Include http://. If you don't want to use Oculus, set this to an
# empty string. If you comment this out, Skyline won't work! Speed improvements
# will occur when Oculus support is disabled.
OCULUS_HOST = 'http://your_oculus_host.com'

"""
Analyzer settings
"""

# This is the location the Skyline agent will write the anomalies file to disk.
# It needs to be in a location accessible to the webapp.
ANOMALY_DUMP = 'webapp/static/dump/anomalies.json'

# This is the number of processes that the Skyline analyzer will spawn.
# Analysis is a very CPU-intensive procedure. You will see optimal results
# if you set ANALYZER_PROCESSES to several less than the total number of
# CPUs on your box. Be sure to leave some CPU room for the Horizon workers,
# and for Redis.
ANALYZER_PROCESSES = 5

# This is the duration, in seconds, for a metric to become 'stale' and for
# the analyzer to ignore it until new datapoints are added. 'Staleness' means
# that a datapoint has not been added for STALE_PERIOD seconds.
STALE_PERIOD = 500

# This is the minimum length of a timeseries, in datapoints, for the analyzer
# to recognize it as a complete series.
MIN_TOLERABLE_LENGTH = 1

# Sometimes a metric will continually transmit the same number. There's no need
# to analyze metrics that remain boring like this, so this setting determines
# the amount of boring datapoints that will be allowed to accumulate before the
# analyzer skips over the metric. If the metric becomes noisy again, the
# analyzer will stop ignoring it.
MAX_TOLERABLE_BOREDOM = 100

# By default, the analyzer skips a metric if it it has transmitted a single
# number MAX_TOLERABLE_BOREDOM times. Change this setting if you wish the size
# of the ignored set to be higher (ie, ignore the metric if there have only
# been two different values for the past MAX_TOLERABLE_BOREDOM datapoints).
# This is useful for timeseries that often oscillate between two values.
BOREDOM_SET_SIZE = 1

# The canary metric should be a metric with a very high, reliable resolution
# that you can use to gauge the status of the system as a whole.
CANARY_METRIC = 'statsd.numStats'

# These are the algorithms that the Analyzer will run. To add a new algorithm,
# you must both define the algorithm in algorithms.py and add its name here.
ALGORITHMS = [
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

# This is the number of algorithms that must return True before a metric is
# classified as anomalous.
CONSENSUS = 6

# This is to enable second order anomalies. This is an experimental feature, so
# it's turned off by default.
ENABLE_SECOND_ORDER = False

# This enables alerting.
ENABLE_ALERTS = True

# This is the config for which metrics to alert on and which strategy to use for each.
# Alerts will not fire twice within EXPIRATION_TIME, even if they trigger again.
# Schema: (
#          ("metric1", "smtp", EXPIRATION_TIME),
#          ("metric2", "pagerduty", EXPIRATION_TIME),
#          ("metric3", "hipchat", EXPIRATION_TIME),
#         )
ALERTS = (
    ("skyline", "smtp", 1800),
)

# Each alert module requires additional information.
SMTP_OPTS = {
    # This specifies the sender of email alerts.
    "sender": "skyline-alerts@etsy.com",
    # recipients is a dictionary mapping metric names
    # (exactly matching those listed in ALERTS) to an array of e-mail addresses
    "recipients": {
        "skyline": ["abe@etsy.com", "you@yourcompany.com"],
    },
}

# HipChat alerts require python-simple-hipchat
HIPCHAT_OPTS = {
    "auth_token": "pagerduty_auth_token",
    # list of hipchat room_ids to notify about each anomaly
    # (similar to SMTP_OPTS['recipients'])
    "rooms": {
        "skyline": (12345,),
    },
    # Background color of hipchat messages
    # (One of "yellow", "red", "green", "purple", "gray", or "random".)
    "color": "purple",
}

# PagerDuty alerts require pygerduty
PAGERDUTY_OPTS = {
    # Your pagerduty subdomain and auth token
    "subdomain": "example",
    "auth_token": "your_pagerduty_auth_token",
    # Service API key (shown on the detail page of a "Generic API" service)
    "key": "your_pagerduty_service_api_key",
}


"""
Horizon settings
"""
# This is the number of worker processes that will consume from the Horizon
# queue.
WORKER_PROCESSES = 2

# The IP address for Horizon to listen on.  Defaults to gethostname()
# HORIZON_IP = '0.0.0.0'

# This is the port that listens for Graphite pickles over TCP, sent by Graphite's
# carbon-relay agent.
PICKLE_PORT = 2024

# This is the port that listens for Messagepack-encoded UDP packets.
UDP_PORT = 2025

# This is how big a 'chunk' of metrics will be before they are added onto
# the shared queue for processing into Redis. If you are noticing that Horizon
# is having trouble consuming metrics, try setting this value a higher.
CHUNK_SIZE = 10

# This is the maximum allowable length of the processing queue before new
# chunks are prevented from being added. If you consistently fill up the
# processing queue, a higher MAX_QUEUE_SIZE will not save you. It most likely
# means that the workers do not have enough CPU alotted in order to process the
# queue on time. Try increasing CHUNK_SIZE, decreasing ANALYZER_PROCESSES, or
# decreasing ROOMBA_PROCESSES.
MAX_QUEUE_SIZE = 500

# This is the number of Roomba processes that will be spawned to trim
# timeseries in order to keep them at FULL_DURATION. Keep this number small,
# as it is not important that metrics be exactly FULL_DURATION *all* the time.
ROOMBA_PROCESSES = 1

# Normally Roomba will clean up everything that is older than FULL_DURATION
# if you have metrics that are not coming in every second, it can happen
# that you'll end up with INCOMPLETE metrics.
# With this setting Roomba will clean up evertyhing that is older than
# FULL_DURATION + ROOMBA_GRACE_TIME
ROOMBA_GRACE_TIME = 600

# The Horizon agent will ignore incoming datapoints if their timestamp
# is older than MAX_RESOLUTION seconds ago.
MAX_RESOLUTION = 1000

# These are metrics that, for whatever reason, you do not want to store
# in Skyline. The Listener will check to see if each incoming metrics
# contains anything in the skip list. It is generally wise to skip entire
# namespaces by adding a '.' at the end of the skipped item - otherwise
# you might skip things you don't intend to.
SKIP_LIST = [
    'example.statsd.metric',
    'another.example.metric',
    # if you use statsd, these can result in many near-equal series
    #'_90',
    #'.lower',
    #'.upper',
    #'.median',
    #'.count_ps',
    #'.sum',
]


"""
Webapp settings
"""

# The IP address for the webapp
WEBAPP_IP = '127.0.0.1'

# The port for the webapp
WEBAPP_PORT = 1500
