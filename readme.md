## Skyline

[![Build Status](https://travis-ci.org/etsy/skyline.svg)](https://travis-ci.org/etsy/skyline)

![x](https://raw.github.com/etsy/skyline/master/screenshot.png)

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

### mirage
mirage is an extension of skyline that enables second order resolution 
analysis of metrics that have a `SECOND_ORDER_RESOLUTION_HOURS` defined in the 
alert tuple.
Skyline's `FULL_DURATION` somewhat limits Skyline's usefulness for metrics 
that have a seasonality / periodicity > `FULL_DURATION`. mirage uses the 
user defined seasonality for a metric (`SECOND_ORDER_RESOLUTION_HOURS`) 
and if analyzer finds a metric to be anomalous at `FULL_DURATION` and the 
metric alert tuple has `SECOND_ORDER_RESOLUTION_HOURS` and `ENABLE_MIRAGE` 
is 'True', analyzer will push the metric variables to the mirage check dir for 
mirage to surface the metric timeseries at its proper seasonality, in realtime 
from graphite in json format and then analyze the timeseries to determine if the 
datapoint that triggered analyzer, is anomalous at the metric's true seasonality.

By default mirage is disabled, various mirage options can be configured in the 
settings.py file and analyzer and mirage can be configured as approriate for your 
environment.

mirage requires some directories: 

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

### Architecture
See the rest of the
[wiki](https://github.com/etsy/skyline/wiki)

### Contributions
1. Clone your fork
2. Hack away
3. If you are adding new functionality, document it in the README or wiki
4. If necessary, rebase your commits into logical chunks, without errors
5. Verfiy your code by running the test suite and pep8, adding additional tests if able.
6. Push the branch up to GitHub
7. Send a pull request to the etsy/skyline project.

We actively welcome contributions. If you don't know where to start, try
checking out the [issue list](https://github.com/etsy/skyline/issues) and
fixing up the place. Or, you can add an algorithm - a goal of this project
is to have a very robust set of algorithms to choose from.

Also, feel free to join the 
[skyline-dev](https://groups.google.com/forum/#!forum/skyline-dev) mailing list
for support and discussions of new features.

(*depending on your data throughput, *you might need to write your own
algorithms to handle your exact data, *it runs on one box)
