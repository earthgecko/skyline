## Skyline

![skyline web app](screenshot.png)

Skyline is a real-time* anomaly detection* system*, built to enable passive
monitoring of hundreds of thousands of metrics, without the need to configure a
model/thresholds for each one, as you might do with Nagios. It is designed to be
used wherever there are a large quantity of high-resolution timeseries which
need constant monitoring. Once a metrics stream is set up (from StatsD or
Graphite or other source), additional metrics are automatically added to Skyline
for analysis. Skyline's easily extendible algorithms automatically detect what
it means for each metric to be anomalous. After Skyline detects an anomalous
metric, it surfaces the entire timeseries to the webapp, where the anomaly can be
viewed and acted upon.

Read the details in the [wiki](https://github.com/etsy/skyline/wiki).

## Install

1. `sudo pip install -r requirements.txt` for the easy bits

2. Install numpy, scipy, pandas, patsy, statsmodels, msgpack_python in that
order.

2. You may have trouble with SciPy. If you're on a Mac, try:

* `sudo port install gcc48`
* `sudo ln -s /opt/local/bin/gfortran-mp-4.8 /opt/local/bin/gfortran`
* `sudo pip install scipy`

On Debian, apt-get works well for Numpy and SciPy. On Centos, yum should do the
trick. If not, hit the Googles, yo.

3. `cp src/settings.py.example src/settings.py`

4. Add directories:

```
sudo mkdir /var/log/skyline
sudo mkdir /var/run/skyline
sudo mkdir /var/log/redis
sudo mkdir /var/dump/
```

5. Download and install the latest Redis release

6. Start 'er up

* `cd skyline/bin`
* `sudo redis-server redis.conf`
* `sudo ./horizon.d start`
* `sudo ./analyzer.d start`
* `sudo ./webapp.d start`

By default, the webapp is served on port 1500.

7. Check the log files to ensure things are running.

[Debian + Vagrant specific, if you prefer](https://github.com/etsy/skyline/wiki/Debian-and-Vagrant-Installation-Tips)

### Gotchas

* If you already have a Redis instance running, it's recommended to kill it and
restart using the configuration settings provided in bin/redis.conf

* Be sure to create the log directories.

### Hey! Nothing's happening!
Of course not. You've got no data! For a quick and easy test of what you've
got, run this:
```
cd utils
python seed_data.py
```
This will ensure that the Horizon
service is properly set up and can receive data. For real data, you have some
options - see [wiki](https://github.com/etsy/skyline/wiki/Getting-Data-Into-Skyline)

Once you get real data flowing through your system, the Analyzer will be able
start analyzing for anomalies!

### Alerts
Skyline can alert you! In your settings.py, add any alerts you want to the ALERTS
list, according to the schema:

* `(metric keyword, strategy, expiration seconds, <SECOND_ORDER_RESOLUTION_HOURS>)`

Where `strategy` is one of `smtp`, `hipchat`, `pagerduty` or `syslog`.  Wildcards
can be used in the `metric keyword` as well. You can also add your own alerting
strategies. For every anomalous metric, Skyline will search for the given
keyword and trigger the corresponding alert(s). To prevent alert fatigue, Skyline
will only alert once every <expiration seconds> for any given metric/strategy
combination. To enable Hipchat integration, uncomment the python-simple-hipchat
line in the requirements.txt file.  If using syslog then the `EXPIRATION_TIME`
should be set to 1 for this to be effective in catching every anomaly, e.g.

* `("stats", "syslog", 1)`

The optional `SECOND_ORDER_RESOLUTION_HOURS` setting is only required should
you want to send any metrics to mirage at a different time resolution period.

### How do you actually detect anomalies?
An ensemble of algorithms vote. Majority rules. Batteries __kind of__ included.
See [wiki](https://github.com/etsy/skyline/wiki/Analyzer)

## mirage
mirage is an extension of skyline that enables second order resolution
analysis of metrics that have a `SECOND_ORDER_RESOLUTION_HOURS` defined in the
alert tuple.
Skyline's `FULL_DURATION` somewhat limits Skyline's usefulness for metrics
that have a seasonality / periodicity that is greater than `FULL_DURATION`.
Increasing skyline's `FULL_DURATION` to anything above 24 hours (86400) is not
necessarily realistic or useful, because the greater the `FULL_DURATION`, the
greater redis memory and the longer `skyline.analyzer.run_time` and if you do not
analyze all your metrics within as close to a 60 second period as possible, lag
begins to inhibits efficiency.
mirage uses the user-defined seasonality for a metric (`SECOND_ORDER_RESOLUTION_HOURS`)
and if analyzer finds a metric to be anomalous at `FULL_DURATION` and the
metric alert tuple has `SECOND_ORDER_RESOLUTION_HOURS` and `ENABLE_MIRAGE`
is 'True', analyzer will push the metric variables to the mirage check dir for
mirage to surface the metric timeseries at its proper seasonality, in realtime
from graphite in json format and then analyze the timeseries to determine if the
datapoint that triggered analyzer, is anomalous at the metric's true seasonality.

By default mirage is disabled, various mirage options can be configured in the
settings.py file and analyzer and mirage can be configured as approriate for your
environment.

mirage requires some directories as per settings.py defines (these require absolute path):

```
sudo mkdir -p $MIRAGE_CHECK_PATH
sudo mkdir -p $MIRAGE_DATA_FOLDER
```

Configure settings.py with some alert tuples that have the ```SECOND_ORDER_RESOLUTION_HOURS```
defined, e.g.:

```
ALERTS = (
           ("skyline", "smtp", 1800),
           ("stats_counts.http.rpm.publishers.*", "smtp", 300, 168),
)
```

And ensure that settings.py has mirage options enabled:

```
ENABLE_MIRAGE = True

MIRAGE_ENABLE_ALERTS = True
```

Start mirage:

* `cd skyline/bin`
* `sudo ./mirage.d start`

mirage allows for testing of realtime data and algorithms in parallel to analyzer
allowing for comparisons of different timeseries and/or algorithms.  mirage was
inspired by crucible and the desire to extend the temporal data pools available
to analyzer in an attempt to handle seasonality better, reduce noise and increase
signal, specfically on seasonal metrics.

mirage is rate limited to analyse 30 metrics per minute, this is by design and
desired. Surfacing data from graphite and analysing ~1000 datapoints in a timeseries
takes less than 1 second and is much less CPU intensive than analyzer, but it is
probably sufficient to have 30 calls to graphite per minute and if a large number
of metrics went anomalous, even with mirage discarding `MIRAGE_STALE_SECONDS`
checks due to processing limit, signals would still be sent.

## boundary
boundary is an extension of skyline that enables very specific analysis of
specified metrics with specified algorithms, with specified alerts.

boundary was added to allow for threshold-like monitoring to the skyline model,
it was specifically added to enable the detect_drop_off_cliff algorithm which
could not be bolted nicely into analyzer (although it was attempted, it was ugly).
While analyzer allows for the passive analysis of 1000s of metrics, its algorithms
are not perfect.  boundary allows for the use of the skyline data and model as a
scapel, not just a sword.  Just like analyzer, boundary has its own algorithms
and importantly, boundary is *not* `CONSENSUS` based.  This means that you can
match key metrics on "thresholds/limits" and somewhat dynamically too.

The boundary concept is quite like skyline backwards, enilyks.  This is because
where analyzer is almost all to one configuration, boundary is more one
configuration to one or many.  Where analyzer is all metrics through all algorithms,
boundary is each metric through one algorithm.  analyzer uses a large range of
the timeseries data, boundary uses the most recent (the now) portion of the
timeseries data.

boundary currently has 3 defined algorithms:
* detect_drop_off_cliff
* less_than
* greater_than

boundary is run as a separate process just like analyzer, horizon and mirage.  It
was not envisaged to analyse all your metrics, but rather your key metrics in an
additional dimension/s.   If it was run across all of your metrics it would probably
be a) VERY noisy, b) VERY CPU intensive; if deployed only key metrics it has a
very low footprint (9 seconds on 150 metrics with 2 processes assigned) and a
high return.  If deployed as intended it should be able to easily coexist with
an existing skyline analyzer/mirage setup, with adding minimal load.  This also
allows one to implement boundary independently without changing, modifying or
impacting on a running analyzer.

boundary alerting is similar to analyzer alerting, but a bit more featureful and
introduces the ability to rate limit alerts per alerter channel, as it is not
beyond the realms of possibility that at some point all your key metrics may drop
off a cliff, but maybe 15 pagerduty alerts every 30 minutes is sufficient, so
alert rates are configurable.

### Configuration and running boundary
settings.py has an independent setting blocks and the `settings.py.example` has
the detailed information on each setting, the main difference from analyzer being
in terms of number of variables that have to be declared in the alert tuples,
e.g:

```
BOUNDARY_METRICS = (
    # ("metric", "algorithm", EXPIRATION_TIME, MIN_AVERAGE, MIN_AVERAGE_SECONDS, TRIGGER_VALUE, ALERT_THRESHOLD, "ALERT_VIAS"),
    ("nometrics", "detect_drop_off_cliff", 1800, 500, 3600, 0, 2, "smtp"),
    ("nometrics.either", "less_than", 3600, 0, 0, 15, 2, "smtp"),
    ("nometrics.other", "greater_than", 3600, 0, 0, 100000, 1, "smtp|hipchat|pagerduty"),
)
```

Once settings.py has all the boundary configuration done, start boundary:

* `cd skyline/bin`
* `sudo ./boundary.d start`

### detect_drop_off_cliff algorithm - EXPERIMENTAL
The detect_drop_off_cliff algorithm provides a method for analysing a timeseries
to determine is the timeseries "dropped off a cliff".  The standard skyline
analyzer algorithms do not detect the drop off cliff pattern very well at all,
testing with crucible has proven.  Further to this, the `CONSENSUS` methodology
used to determine whether a timeseries deemed anomalous or not, means that even
if one or two algorithms did detect a drop off cliff type event in a timeseries,
it would not be flagged as anomalous if the `CONSENSUS` threshold was not breached.

The detect_drop_off_cliff algorithm - does just what it says on the tin. Although
this may seem like setting and matching a threshold, it is more effective than a
threshold as it is dynamically set depending on the data range.

Some things to note about analysing a timeseries with the algorithm are:
* This algorithm is most suited (accurate) with timeseries where there is a
large range in the timeseries most datapoints are > 100 (e.g high rate). Arbitrary
`trigger` values in the algorithm do filter peaky low rate timeseries, but they
can become more noisy with lower value datapoints, as significant cliff drops
are from a lower height, however it still generally matches drops off cliffs on
low range metrics.
* The trigger tuning based on the timeseries sample range is fairly arbitrary,
but has been tested and does filter peaky noise in low range timeseries, which
filters most/lots of noise.
* The alogrithm is more suited to datasets which come from multiple sources,
e.g. an aggregation of a count from all servers, rather than from individual
sources, e.g. a single server's metric.  The many are less likely to experience
false positive cliff drops, whereas the individual is more likely to experience
true cliff drops.
* __ONLY TESTED WITH__:
** positive, whole number timeseries data
** Does __not__ currently work with negative integers in the timeseries values (although it will not break, will just skip if a negative integer is encountered)
** For more info see [detect_drop_off_cliff](https://github.com/earthgecko/crucible/tree/master/examples/detect_drop_off_cliff)

### Architecture
See the rest of the
[wiki](https://github.com/etsy/skyline/wiki)

### Contributions
1. Fork earthgeko/skyline
2. Hack away
3. If you are adding new functionality, document it in the README or examples
4. If necessary, rebase your commits into logical chunks, without errors
5. Verfiy your code by running the test suite and pep8, adding additional tests if able.
6. Push the branch up to GitHub
7. Send a pull request to the earthgeko/skyline project.

We actively welcome contributions. If you don't know where to start, try
checking out the [issue list](https://github.com/earthgecko/skyline/issues) and
fixing up the place. Or, you can add an algorithm - a goal of this project
is to have a very robust set of algorithms to choose from.

Also, feel free to join the
[skyline-dev](https://groups.google.com/forum/#!forum/skyline-dev) mailing list
for support and discussions of new features.

(*depending on your data throughput, *you might need to write your own
algorithms to handle your exact data, *it runs on one box)
