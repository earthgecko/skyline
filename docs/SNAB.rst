SNAB
====

*EXPERIMENTAL*

SNAB is for algorithm testing and benchmarking with real time data and
historic data.  Allowing for custom_algorithms to be run against certain metrics
in the real time workflow pipeline.

SNAB was inspired by the The Numenta Anomaly Benchmark project (https://github.com/numenta/NAB)
and attempts to provide a framework within Skyline for testing and benchmark
algorithms on real time data.

SNAB is *not a replacement for NAB*, it is simply the beginning of a NAB-ish
like implementation for testing that suits Skyline.

SNAB components
---------------

SNAB uses the :mod:`settings.SNAB_DATA_DIR` directory (for time series data),
the snab database table for results and reports to log and a slack channel (if
configured too) and it needs to be able to access the algorithm/s code for the
algorithm/s that are configured to be used.

settings.py
-----------

Currently the snab module has two relevant settings :mod:`settings.SNAB_ENABLED`
and :mod:`settings.SNAB_CHECKS`.  For Analyzer and Mirage to send anomalies to
SNAB to check against other algorithms, :mod:`settings.SNAB_ENABLED` must be
set to ``True``.

Running Modes
-------------

SNAB has 2 modes it can run in, ``testing`` and ``realtime`` (not tested).
SNAB has only be tested running with Mirage.

SNAB is only implemented as on v2.1.0 with the ``testing`` mode with Mirage with
a single algorithm.

The snab process
----------------

The snab Skyline app is started like the other apps, using bin/snab.d in the
repo and your choice of process manager, e.g. systemctl, etc.

In terms of running, when a Skyline app is defined in the :mod:`settings.SNAB_CHECKS`,
when the app triggers an anomaly on a metric, it will save the time series data
to the :mod:`settings.SNAB_DATA_DIR` directory (if the time series has not been
saved for Ionosphere training data) and instruct Panorama to create a snab DB entry for
the originating algorithm_group and the anomaly_id via the ``panorama.snab``
Redis set.  The app will then send the metric and details through to snab to
check against the defined algorithm/s.

SNAB will then load the saved time series from the file specified in the check
details and run the defined algorithm against it, determine the result, instruct
Panorama to also create a snab DB entry for the algorithm, anomaly_id and
algorithm_run_time and snab will post the result to slack for evaluation.

The operator can then evaluate the results for the originating anomaly and
algorithm_group and the SNAB algorithm/s in slack and assign a
tP (true positive), fP (false positive), tN (true negative), fN (false negative)
or unsure value to the result of each.  This provides data in the snab database
table with which to evaluate the performance of different algorithms.

The use of slack is required for evaluation, evaluation from the snab.log and
manual creation of the webapp API snab endpoint URLs is not really feasible.
The slack alerts provide all the graphs and data necessary for a proper
evaluation to be made.

SNAB_CHECKS
-----------

To send any metrics to SNAB for them to be checked they must be defined in an
item in the :mod:`settings.SNAB_CHECKS` dictionary and :mod:`settings.SNAB_ENABLED`
must be set to ``True``.  The :mod:`settings.SNAB_CHECKS` dictionary has the
following structure:

.. code-block:: python

    SNAB_CHECKS = {
        '<skyline_app>': {
            '<mode>': {
                '<algorithm>': {
                    'namespaces': [''<metric_namespace_1>', '<metric_namespace_2>'],
                    'algorithm_source': '<absolute_path and filename>',
                    'algorithm_parameters': {'<algorithm_parameter_1>': <value>, '<algorithm_parameter_2>': <value>},
                    'max_execution_time': <seconds|float>,
                    'debug_logging': <boolean>,
                    'alert_slack_channel': '<slack_channel>'
                }
            }
        },
    }

An example of this would be:

.. code-block:: python

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

SNAB_LOAD_TEST_ANALYZER
-----------------------

SNAB can be used to load test Analyzer by defining the number of metrics you
want to load test with is :mod:`settings.SNAB_LOAD_TEST_ANALYZER`.  The load
testing is not an exact reflection of the capability of Analyzer but rather an
indication of the possible capability of Analyzer.  This is becasue of how the
load test is run.  The load testing is only run after normal analysis and the
metrics run through the load test are not submitted to any of the normal
classification that metrics run through the normal process are subjected to.
Metrics run through the normal analysis process are subjected to any
classification checks that may be done on metrics, e.g.

- Is this a mirage metric?
- Is this a strictly increasing monotonic metric? Should the derivative be used?
- Should airgaps be identified in this metric?
- Is this a flux filled metric?
- etc

These classifications and transformations may be done on some metrics during the
normal analysis process and each takes some time.  However these classifications
and transformations are not desired in the load testing because during load
testing, the metric data being used to load test should not impact or modify any
of the normal analysis process resources in terms of Redis sets and keys, etc.

The load testing uses the same time series data of the metrics that run through
the normal analysis process by iterating this data through the three-sigma
algorithms after normal analysis until:

- The load test has completed the analysis of :mod:`settings.SNAB_LOAD_TEST_ANALYZER`
  metrics.
- Or :mod:`settings.MAX_ANALYZER_PROCESS_RUNTIME` - 5 is reached and the load
  test exits.

Load test results are reported in the analyzer.log

Running a load testing is as easy as defining a number in
:mod:`settings.SNAB_LOAD_TEST_ANALYZER`, restarting analyzer and checking the
log.  Remember to set :mod:`settings.SNAB_LOAD_TEST_ANALYZER` to 0 after your
load test and restart analyzer again.

Example of log output:

.. code-block::

    2020-10-07 16:52:04 :: 3580119 :: SNAB_LOAD_TEST_ANALYZER set to test 27000 metrics
    2020-10-07 16:52:04 :: 3580119 :: SNAB_LOAD_TEST_ANALYZER - there are currently 1392 unique metrics, 25608 snab.analyzer_load_test metrics will be added
    2020-10-07 16:52:04 :: 3580119 :: starting 1 of 1 spin_process/es
    2020-10-07 16:52:04 :: 3581073 :: spin_process started
    2020-10-07 16:52:04 :: 3581073 :: SNAB_LOAD_TEST_ANALYZER will run after normal analysis
    ...
    ...
    2020-10-07 16:52:17 :: 3581073 :: SNAB_LOAD_TEST_ANALYZER set to test 27000 metrics
    2020-10-07 16:52:17 :: 3581073 :: SNAB_LOAD_TEST_ANALYZER - 1392 unique metrics were analyzed, 25608 snab.analyzer_load_test metrics to be done on this process
    2020-10-07 16:52:17 :: 3581073 :: SNAB_LOAD_TEST_ANALYZER - load testing completed on 1392 load test metrics
    2020-10-07 16:52:18 :: 3581073 :: SNAB_LOAD_TEST_ANALYZER - load testing completed on 2784 load test metrics
    2020-10-07 16:52:18 :: 3581073 :: SNAB_LOAD_TEST_ANALYZER - load testing completed on 4176 load test metrics
    2020-10-07 16:52:18 :: 3581073 :: SNAB_LOAD_TEST_ANALYZER - load testing completed on 5568 load test metrics
    2020-10-07 16:52:19 :: 3581073 :: SNAB_LOAD_TEST_ANALYZER - load testing completed on 6960 load test metrics
    2020-10-07 16:52:19 :: 3581073 :: SNAB_LOAD_TEST_ANALYZER - load testing completed on 8352 load test metrics
    2020-10-07 16:52:20 :: 3581073 :: SNAB_LOAD_TEST_ANALYZER - load testing completed on 9744 load test metrics
    2020-10-07 16:52:20 :: 3581073 :: SNAB_LOAD_TEST_ANALYZER - load testing completed on 11136 load test metrics
    2020-10-07 16:52:21 :: 3581073 :: SNAB_LOAD_TEST_ANALYZER - load testing completed on 12528 load test metrics
    2020-10-07 16:52:21 :: 3581073 :: SNAB_LOAD_TEST_ANALYZER - load testing completed on 13920 load test metrics
    2020-10-07 16:52:21 :: 3581073 :: SNAB_LOAD_TEST_ANALYZER - load testing completed on 15312 load test metrics
    2020-10-07 16:52:22 :: 3581073 :: SNAB_LOAD_TEST_ANALYZER - load testing completed on 16704 load test metrics
    2020-10-07 16:52:22 :: 3581073 :: SNAB_LOAD_TEST_ANALYZER - load testing completed on 18096 load test metrics
    2020-10-07 16:52:23 :: 3581073 :: SNAB_LOAD_TEST_ANALYZER - load testing completed on 19488 load test metrics
    2020-10-07 16:52:23 :: 3581073 :: SNAB_LOAD_TEST_ANALYZER - load testing completed on 20880 load test metrics
    2020-10-07 16:52:24 :: 3581073 :: SNAB_LOAD_TEST_ANALYZER - load testing completed on 22272 load test metrics
    2020-10-07 16:52:24 :: 3581073 :: SNAB_LOAD_TEST_ANALYZER - load testing completed on 23664 load test metrics
    2020-10-07 16:52:25 :: 3581073 :: SNAB_LOAD_TEST_ANALYZER - load testing completed on 25056 load test metrics
    2020-10-07 16:52:25 :: 3581073 :: SNAB_LOAD_TEST_ANALYZER - load testing completed on 25608 load test metrics
    2020-10-07 16:52:25 :: 3581073 :: SNAB_LOAD_TEST_ANALYZER - results load test metrics - anomalous: 0, not_anomalous: 25608
    2020-10-07 16:52:25 :: 3581073 :: SNAB_LOAD_TEST_ANALYZER - load testing completed on 25608 load test metrics in 7.71 seconds
    2020-10-07 16:52:25 :: 3581073 :: spin_process took 20.39 seconds
    2020-10-07 16:52:25 :: 3580119 :: analyzer :: 1 spin_process/es completed in 20.50 seconds
    2020-10-07 16:52:25 :: 3580119 :: spin_process with pid 3581073 completed
    ...
    ...
    2020-10-07 16:52:26 :: 3580119 :: seconds to run     :: 26.42
    2020-10-07 16:52:26 :: 3580119 :: total metrics      :: 1392
    2020-10-07 16:52:26 :: 3580119 :: total analyzed     :: 531
    2020-10-07 16:52:26 :: 3580119 :: total anomalies    :: 1
    2020-10-07 16:52:26 :: 3580119 :: exception stats    :: {'Boring': 688, 'TooShort': 165, 'Stale': 8, 'Other': 0}
    2020-10-07 16:52:26 :: 3580119 :: anomaly breakdown  :: {'histogram_bins': 0, 'first_hour_average': 0, 'stddev_from_average': 0, 'grubbs': 0, 'ks_test': 0, 'mean_subtraction_cumulation': 0, 'median_absolute_deviation': 0, 'stddev_from_moving_average': 0, 'least_squares': 0, 'abs_stddev_from_median': 0, 'last_same_hours': 0}
    2020-10-07 16:52:26 :: 3580119 :: mirage_periodic_checks  :: 0
    2020-10-07 16:52:26 :: 3580119 :: sent_to_mirage     :: 2
    2020-10-07 16:52:26 :: 3580119 :: Mirage metrics     :: 1274
    2020-10-07 16:52:26 :: 3580119 :: mirage_periodic_checks  :: 0
    2020-10-07 16:52:26 :: 3580119 :: sent_to_panorama   :: 0
    2020-10-07 16:52:26 :: 3580119 :: sent_to_ionosphere :: 0
    2020-10-07 16:52:26 :: 3580119 :: Ionosphere metrics :: 763
    2020-10-07 16:52:26 :: 3580119 :: canary duration    :: 24.17
    2020-10-07 16:52:26 :: 3580119 :: sleeping for 33.01 seconds due to low run time...

SNAB flux load tester
---------------------

So how many metrics can Skyline handle running on that digitalocean droplet?

It is difficult to tell how many metrics a single Skyline server can handle
given the myriad combinations of configurations and hardware resources it may be
run on.

This is where SNAB flux load tester comes into play.  It allows you to deploy
Skyline and the snab app can be configured to send as many metrics as you want
to Graphite and Skyline.

.. warning:: DO NOT run this on an existing Skyline that is running with real
  data, it is meant to be run on a new, disposal Skyline build.  This because it
  populates Graphite, Redis, the database tables, etc with real data of test
  metrics.  Unless you want to test and remove all the test metrics from Redis,
  Graphite and MariaDB manually, which would be possible, but not advisable.

To enable a snab_flux_load_test set the following in settings.py:

.. code-block:: python

    SNAB_FLUX_LOAD_TEST_ENABLED = True
    # the number of metrics you want to load test with, use the number appropriate
    # for you
    SNAB_FLUX_LOAD_TEST_METRICS = 2000

Ensure that horizon, analyzer, flux and Graphite are running and start
snab_flux_load_test.d as appropriate

.. code-block:: bash

    sudo -u skyline /opt/skyline/github/skyline/bin/snab_flux_load_test.d start
    tail -n 80 /var/log/skyline/snab_flux_load_test.log

    # Stop the load test
    /opt/skyline/github/skyline/bin/snab_flux_load_test.d stop

You will want to let the load test run for a while and you may want to adjust
the :mod:`settings.SNAB_FLUX_LOAD_TEST_METRICS` value and restart the test a few
times.
