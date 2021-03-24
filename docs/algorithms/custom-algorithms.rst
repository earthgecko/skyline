=================
Custom algorithms
=================

**EXPERIMENTAL - py3 only**

This section describes the process, steps and resources required to run custom
algorithms in Skyline. Adding a custom algorithm or algorithms is easier and
better than modifying the core Skyline algorithm files yourself.

If you are wanting to implement a custom algorithm, ensure you read this
documentation **thoroughly** and **understand the layout** in the example
algorithms.  This will save you time if you do it properly from the beginning.

Custom algorithms are currently implemented in analyzer, analyzer_batch and
mirage (at some point they may be added to crucible), they are not yet consumed
in boundary and probably will not unless there is a good use case for them to be
added.

To implement a custom algorithm, you need to define it in
:mod:`settings.CUSTOM_ALGORITHMS` and add the Python source custom algorithm
file.

.. warning:: **A note on speed**, bear in mind that any custom algorithms added
  have to run **FAST**, otherwise analysis stops being real time and the
  Skyline apps will terminate their own spawned processes if they run too long.
  Consider that Skyline's three-sigma triggered algorithms take on average
  0.0023 seconds to run and all 9 are run on a metric in about 0.0207 seconds.
  Adding any algorithms that run substantially slower is **not** recommended,
  even if it is on a small set of metrics.  Any algorithms added to should be as
  computationally efficient as possible and suitable for processing real time
  streaming data, e.g. O(n).  This is especially true for any custom algorithms
  added to Analyzer.  This is not a hard requirement, just a recommendation.
  It is possible to add non O(n) algorithms to Mirage, but they should be
  designed and implemented to be as computationally efficient as possible.
  If you break Skyline with your own custom algorithms, be that on your head.

Custom algorithms can be run **before** or **after** the core three-sigma
algorithms. The custom algorithm can be configured to run before the three-sigma
algorithms which allows the user to disable the running of three-sigma
algorithms on namespaces if they so desire, more on this below.  By default
custom algorithms are run **before** three-sigma algorithms.
Running the custom algorithm **after** three-sigma allows the user to only run
a custom algorithm if three-sigma achieves :mod:`settings.CONSENSUS` or run the
custom algorithm regardless of the CONSENSUS that was achieved.

The custom algorithms settings are therefore highly configurable, specifically
take note of the consensus, run_3sigma_algorithms, run_before_3sigma and
run_only_if_consensus parameters, especially if you are attempting to run
multiple custom algorithms at different stages, before or after three-sigma and
what consensus should be applied.  It is probably possible to configure poorly
given the methods and modes that can be configured.

:mod:`settings.CUSTOM_ALGORITHMS`
---------------------------------

Custom algorithms are defined in the :mod:`settings.CUSTOM_ALGORITHMS`
dictionary.  The format and key values of the dictionary are shown in the
following **example**:

.. code-block:: python

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
            'use_with': ['analyzer', 'crucible'],
            'debug_logging': True,
        },
        'matrixprofile': {
            'namespaces': ['*'],
            'algorithm_source': '/opt/skyline/github/skyline/skyline/custom_algorithms/skyline_matrixprofile.py',
            'algorithm_parameters': {},
            'max_execution_time': 5.0,
            'consensus': 1,
            'algorithms_allowed_in_consensus': ['matrixprofile'],
            'run_3sigma_algorithms': False,
            'run_before_3sigma': True,
            'run_only_if_consensus': False,
            'use_with': ['snab'],
            'debug_logging': False,
        },
    }

Within the dictionary each custom algorithm is declared and its variables are
defined.  Each custom algorithm defined is required to adhere to the following
requirements.

- **algorithm_name**: firstly and importantly, name of algorithm must be simple,
  unbroken, alphanumeric string.  It **must** also be the name of the main
  algorithm function, this is because it is loaded by ``importlib`` and the
  name in the dictionary is used to load the custom algorithm at runtime.
- ``namespaces``: this is a list of the namespaces you want to run the custom
  algorithm against.  These can be absolute metric names, substrings or dotted
  elements of a namespace or a regex of a namespace.
- ``algorithm_source``: the full path to the custom algorithm Python file, the
  file can be deployed to any directory it does not need to be in the same path
  as the Skyline code, just ensure the user running the Skyline process has read
  permissions on the path and file itself.
- ``algorithm_parameters`` - this is a dictionary of any parameters/arguments
  that you want to pass to your algorithm.  Your custom algorithm will need to
  interpolate your parameters/arguments (key/value) from this dictionary. If
  none are required simply use an empty dict `{}`.
- ``max_execution_time`` - a float (and read the warning about speed above).
- ``consensus`` - this allows you to add your algorithm to the ``CONSENSUS`` or
  override ``CONSENSUS`` by setting this to 1.  If you are running
  ``CONSENSUS = 6`` and wanted to just add your custom algorithm as an addition
  to the normal three-sigma algorithms, you would just pass ``'consensus': 6`` or
  ``'consensus': 7`` depending on what you want.  The only other option currently
  is to **override** the ``CONSENSUS``, if you want an anomaly triggered every
  time your custom algorithm triggers, regardless of three-sigma ``CONSENSUS`` then
  set ``'consensus': 1``
- ``algorithms_allowed_in_consensus``: must be passed but is **not implemented yet**
  but this is a list of algorithms that must have triggered for consensus to be
  achieved. If an empty list is passed `[]` this will be ignored and normal
  ``CONSENSUS`` will be used.
- ``run_3sigma_algorithms``: a boolean stating whether to run the normal three-sigma
  algorithms, this is optional and defaults to ``True`` if it is not passed
  in the dictionary.  **NOTE** - If any custom algorithm is run that has this
  set to ``False`` no three-sigma algorithms will be run regardless of what any
  other custom algorithms are set to.  If multiple custom algorithms are being
  run and only 1 has this set to ``False`` it will be applied to all.
- ``run_before_3sigma``: a boolean stating whether to run the custom algorithm
  before the normal three-sigma algorithms, this defaults to ``True``.  If you
  want your custom algorithm to run after the three-sigma algorithms set this to
  ``False``.
- ``run_only_if_consensus``: a boolean stating whether to run the custom
  algorithm only if CONSENSUS or MIRAGE_CONSENSUS is achieved, it defaults to
  ``False``.  This only applies to custom algorithms that are run after
  three-sigma algorithms, e.g. with the parameter ``run_before_3sigma: False``
  Currently this parameter only uses the CONSENSUS or MIRAGE_CONSENSUS setting
  and does not apply the consensus parameter above.
- ``use_with`` - a list of the Skyline apps that should apply the custom
  algorithm.  All the apps can be declared but they will only apply the custom
  algorithm **if** they actually handle the metric.  Simply declaring them in
  the list does not mean that the app will just automatically run them all the
  time.  If the app does not handle the metric, it being declared makes no
  difference, therefore if you are unsure, it is safe to list them all.
  Although do **note** that if your custom algorithm needs more data than
  :mod:`settings.FULL_DURATION` then do not specify ``'analyzer', 'analyzer_batch'``
  as apps to run the custom algorithm with.
- ``debug_logging``: a boolean to enable debug_logging, which wraps the custom
  algorithm run in a bit of additional logging, regarding timings, etc this is
  useful for development and testing.  In general use and production this should
  always be set to ``False``.

It is also possible to set :mod:`settings.DEBUG_CUSTOM_ALGORITHMS` to ``True``
and this enables debug logging on all custom algorithms, regardless of what
their ``debug_logging`` is set to.  However if this is set to ``False`` debug
logging can still be implemented on each custom_algorithm individually using
``'debug_logging': True,`` in the algorithm item in
:mod:`settings.CUSTOM_ALGORITHMS`.

The custom algorithm file
-------------------------

Although any Python code can be added to a custom algorithm file, the algorithm
file must meet some basic requirements that are required to properly integrate
and be run by Skyline.

Below the requirements are outlined, please read them and you can refer to a
couple of example custom algorithm files in the skyline/custom_algorithms
directory of the repo.  https://github.com/earthgecko/skyline/tree/master/skyline/custom_algorithms

.. warning:: Do remember if the algorithm has requirements that are not declared
  in Skyline's requirements.txt file, ensure that you install the algorithm's
  requirements in the Skyline virtualenv.

``anomalyScore``
~~~~~~~~~~~~~~~~

Unlike the core Skyline algorithms, custom algorithms introduces the requirement
for the algorithm to also return a ``anomalyScore``.  The concept of the
``anomalyScore`` is used in many anomaly detection algorithms and methods and it
is useful in many cases for algorithm testing.

Custom algorithm requirements
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Must be written in Python
- Must import all modules and classes it requires.
- The algorithm must have the following four parameters, e.g.

.. code-block:: python

    def last_same_hours_weekly(current_skyline_app, parent_pid, timeseries, algorithm_parameters):

- The four parameters are:

  - ``current_skyline_app`` - this will be passed to the custom algorithm by
    Skyline to identify which Skyline app is executing the algorithm, this is
    **required** for error handling and logging.  You do not have to worry about
    handling the ``current_skyline_app`` argument in your algorithm, your
    algorithm must just accept it as the first argument.
  - ``parent_pid`` - this will be passed to the custom algorithm by
    Skyline to identify which pid has executed the algorithm, this is
    **required** for error handling and logging.  You do not have to worry about
    handling the ``parent_pid`` argument in your algorithm, your algorithm must
    just accept it as the second argument.
  - ``timeseries`` - the algorithm must accept a time series as a list e.g.
    ``[[1578916800.0, 29.0], [1578920400.0, 55.0], ... [1580353200.0, 55.0]]``
  - ``algorithm_parameters`` - this is a dictionary of any of parameters that
    the algorithm requires.

- Your algorithm should be a simple single function, see the example algorithms
  for guidance.  It is possible that a multi classed algorithm could work, but
  your mileage may vary.  This method is only tested with the algorithm being a
  simple function.
- The custom algorithm must return a boolean to state whether the data point is
  anomalous **and** a ``anomalyScore``, e.g.

.. code-block:: python

    # return (anomalous, anomalyScore)
        return (True, 1.0)
    return (False, 0.2)

- The returned boolean must be one of the following three choices:

  - ``True`` - the data point **is** anomalous
  - ``False`` - the data point **is not** anomalous
  - ``None`` - returned when the algorithm could not determine ``True`` or
    ``False``, an error occurred or there was no data, etc.

- The returned ``anomalyScore`` must be a **float** between 0.0 and 1.0, 0.0
  being not anomalous and 1.0 being a certain anomaly.  You can pass
  `(False, 0.7)`,  you just have to normalise your ``anomalyScore`` between 0.0
  and 1.0.  The ``anomalyScore`` is currently only for testing it is not used in
  any way but it **must** be returned.  The anomalous classification is
  currently **only** determined from the boolean and the ``anomalyScore`` is
  currently not used in any way other than for testing.  If your algorithm does
  not calculate an anomaly score, when your algorithm returns ``False`` just
  return it with a 0.0 and when your algorithm returns ``True`` just return it
  with 1.0

Error handling
~~~~~~~~~~~~~~

In the example algorithms there are examples of how to wrap your algorithm in
normal Skyline algorithm exception handling method.  Although you can implement
your own logging in a custom algorithm, before you do, consider using the method
described in the example algorithms, because the algorithms iterate over 1000s
of time series every minute, logging all errors that are encountered in the
developing or running of an algorithm is not practical (due to simply I/O) or
desired.  To accommodate error logging from algorithms, Skyline's error handling
method writes out any errors to a single file per algorithm during the analysis
phase, overwriting the file with each error.  The errors files are handled in
the /tmp directory which is normal memory based tmpfs resources so no disk I/O
is encountered.  Once analysis is complete, the parent process checks for any
algorithm error files and logs any errors found to the main application log
once.  As shown in the example below.

.. code-block:: none

    2020-06-07 05:47:40 :: 12856 :: error :: spin_process with pid 12870 has reported an error with the abs_stddev_from_median algorithm
    2020-06-07 05:47:40 :: 12856 :: Traceback (most recent call last):
      File "/opt/skyline/github/skyline/skyline/custom_algorithms/abs_stddev_from_median.py", line 46, in abs_stddev_from_median
        make_an_error = median * UNDEFINED_VARIABLE
    NameError: name 'UNDEFINED_VARIABLE' is not defined

This allows for errors to be encountered while not spewing 1000s and 1000s of
lines of errors to disk based the application logs and incurring masses of I/O.

Example custom algorithms
~~~~~~~~~~~~~~~~~~~~~~~~~

There are two example custom algorithms in the repo for you to model the
structure of your custom algorithm on.

abs_stddev_from_median
^^^^^^^^^^^^^^^^^^^^^^

This is the simplest custom algorithm structure, it does not have any
``algorithm_parameters`` and has no debug logging.

https://github.com/earthgecko/skyline/tree/master/skyline/custom_algorithms/abs_stddev_from_median.py

last_same_hours
^^^^^^^^^^^^^^^

This is an example of a more complex custom algorithm structure, that uses
``algorithm_parameters`` and can even debug log to the Skyline app log if
``debug_logging`` is passed and enabled via the ``algorithm_parameters``.

https://github.com/earthgecko/skyline/tree/master/skyline/custom_algorithms/last_same_hours.py

Running a Mirage only custom algorithm on a metric all the time
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Normally for Analyzer to push a metric to Mirage, Analyzer would have to trigger
on it as anomalous.  However if you wish to run a custom algorithm on a metric
that requires ``SECOND_ORDER_RESOLUTION_HOURS`` of data to run against as the
:mod:`settings.FULL_DURATION` data is not sufficient for the custom algorithm,
perhaps due to seasonality, then you need to declare the metric in
:mod:`settings.MIRAGE_ALWAYS_METRICS`.  This will cause Analyzer to add the
metric to Mirage on every run.  Note that the metric needs to be defined as a
mirage enabled metric in the normal way, ensuring it matches a smtp alert
defined in :mod:`settings.ALERTS` with a ``SECOND_ORDER_RESOLUTION_HOURS``
declared.

Some things to consider
~~~~~~~~~~~~~~~~~~~~~~~

- Think about what Skyline apps you want your algorithm to run in.  If you are
  wanting to use data > :mod:`settings.FULL_DURATION` then ensure you only
  specify ``'use_with': ['mirage', 'crucible'],``.
- Thoroughly test your algorithm with ``debug_logging``
- Purposefully break your algorithm during testing to test and see how the error
  handling is working.
- Any custom algorithms applied to analyzer must be **FAST**.  Custom algorithms
  that are only applied to mirage and analyzer_batch can take a bit longer to
  run, but they will delay analysis the longer their execution time.
