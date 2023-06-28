######
Vortex
######

Skyline Vortex is a service responsible for adhoc analysis of timeseries data
submitted via HTTP POSTs.  This allows for adhoc analysis of any timeseries
data, these metrics can be stored anywhere, Skyline does not require access to
them, but metrics already in Skyline can be run through Vortex as well.  The
timeseries data is posted to Flux with json data that provides details about the
metric, the timeseries data itself and what algorithms to analyse the data with.

Although Vortex is accessible via API, there is a webapp UI page to submit local
metric timeseries and csv data as well.

Vortex is handled by two services, Flux and Mirage.  Requests are submitted to
Flux, which are added to a queue for mirage_vortex to process.  mirage_vortex
processes the items in the queue and adds the results to a queue for Flux to
return the results in the request response.

A few things to be aware of are:

- Timestamps must be unix timestamps and will be coerced into ints.
- Values must be ints, floats or nans and will be coerced into floats.
- Timestamps and values that cannot be coerced will be removed.
- The requesting client should use a timeout of 60 seconds.
- The requesting client must follow HTTP status 303 and 302 redirects.
- Only one timeseries can be submitted per request.  This is for the sake of
  simplicity, if multiple metrics/timeseries were accepted the client would
  possibly have to parse and process HTTP 207 responses **and** follow any 303
  or 302 items returned in the 207 response, which could hold a number of 200,
  303 and 302 items.  This would make it impossible to use simple HTTP methods
  such as cURL or python requests, without having to wrap their usage in
  complicated conditions. Therefore to keep it simple - one timeseries per
  request.

Preprocessing
=============

All data submitted to Vortex is preprocessed.  Preprocessing is required to
ensure that the data is suitable for algorithmic analysis, clean and consistent.
Certain algorithms require that there are no gaps or nans in the data to
generate reliable results, others are more tolerant.  Best efforts are made to
assess the data appropriately per algorithm.  Preprocessing also ensures that
the shape of the data conforms to additional analysis in the pipeline such as
training and matching features profiles, etc.

The each preprocessing step that is applied to every data set is described
below.  There may be additional preprocessing required specific to each
algorithm and those preprocessing steps are described in the documentation
specific to the algorithm and are not covered here.

Coercing
--------

Timestamps and values will be coerced into ints and floats respectively.  Any
data that cannot be coerced into the said types will be discarded from the
dataset.

Analysis periods
----------------

To allow Vortex analysis and results to be trained on, by default Vortex
can handle two periods, 86400 seconds (24 hours) and 604800 seconds (7 days).
Data submitted must be either 24 hours worth of data or 7 days worth of data,
if you wish to be able to train on it.

Any data that is submitted that is greater than 7 days in length will be
shortened to start at ``last_timestamp - 604800``. 

Any data that is submitted that is greater in length than 1 day but less than 7
will **only have the last 24 hour period analysed** and the preceding data will
be discarded.

Unless, you pass the ``override_7_day_limit`` parameter, then the data can be
any length, but it will not be trainable.

Downsampling
------------

During preprocessing Vortex will downsample 7 day data to a resolution of 600
seconds and 24 hour data to 60 seconds (if the resolution is < 60).

Data is downsampled on the mean and aligned to the end of the dataset, rather
than the start of the dataset, which results in the final period have a full
sample rather than a partial sample.

If your data is high resolution, e.g. a scrape_interval of 10 seconds, try and
ensure that you use an appropriate rate/step values when surfacing the data to
send. This will result in less bandwidth and time to process than sending your
raw high resolution data.  Although you can send high resolution data, the
downsampling will result in different **exact** values to what your original
data may return when a rate/step is applied, which can lead to small
inconsistencies/discrepancies which may raise needless questions at some point.

**Always try to send 7 days worth of data at a resolution of 600 seconds or 24
hours of data at a resolution of 60 seconds.**

Unless you pass the ``no_downsample`` parameter and in this case the data will
not be trainable.

Strictly increasing monotonic data transformation
-------------------------------------------------

Any data submitted that is strictly increasing monotonic data (counters, counts)
will be transformed to a non-negative derivative timeseries (resets discarded).

Similar to downsampling, **always try to apply an appropriate rate** to any
count based metrics before sending the data rather than sending the raw
monotonic data.


HTTP 303 and 302s responses
===========================

Due to the fact that 100s or 1000s of requests could be made to Vortex at the
same time, the expected response times can vary.  Under general load (and
depending on the algortihms requests) Vortex should return responses within a
few seconds, however if the workload is high that time can be much longer.
This is why the client needs to use a 60 second timeout on requests.  Further
to that if Vortex has not processed the request in the defined timeout period,
it will issue a HTTP 303 (See other) response with a GET URL location for the
client to request to get the result from, the original timeout is automatically
added as a URL parameter.  The client should follow the 303 and will once again
wait for the timeout period for the results response.  If the results are not
ready after this timeout period, then a HTTP 302 response is issues with the
same URL and the retry parameter incurred by 1.  This process is repeated 3
times after which if no results are available, a HTTP 200 response will be
issued with json response of:

.. code-block:: json

    {
      "request_id": "<REQUEST_ID>",
      "results": null,
      "reason": "No results were returned . The request can be retried later, results will be available for 1 hour.",
      "retry_url": "<SCHEME>://<HOST>/flux/vortex_results?request_id=<REQUEST_ID>&timeout=<TIMEOUT>"
    }

The ``retry_url`` will only be present on the final request.


Vortex POST data
================

Due to the number of options available the POST data object can be quite complex.
Each algorithm has it's own parameters, which if not passed will be set to a
sensible default.  Sometimes these sensible defaults are calculated from the
timeseries data itself.

The basic json POST data has the following structure, the keys in the below
example are required.

.. code-block:: json

  {
      "key": "apikey|str|required",
      "metric": "metric|str|required",
      "timeout": seconds|int|required,
      "timeseries": timeseries|[list,dict]|required,
      "algorithms": {
          "sigma": {"consensus": 6},
          "spectral_residual": {},
      },
      "consensus": [["sigma", "spectral_residual"]],
      "reference": "a reference id for you|str|optional"
  }

Required key value pairs that must be sent in the POST json data are:

- ``key``: The flux API token for the namespace.
- ``metric``: The metric name.
- ``timeout``: The timeout to use in seconds.
- ``timeseries``: A list or dict (k/v pairs) of unix timestamps and values.

Optional key value pairs are:

- ``algorithms``: A dict of the algorithms to run, their parameters and a
  consensus patterns.  ``algorithms`` is covered in detail below.  There is a
  maximum of 3 algorithms that can be passed (unless the
  mod:`settings.FLUX_SELF_API_KEY` key is used, see below).
- ``reference``: This can be a string of a reference you wish to assign to the
  analysis task.  This could be a trace id, a metric name or a timeseries, it
  can be anything, but it **must** be cast as a string.  Therefore if it is an
  int or float it must be passed as ``"1987"`` or ``"1669399450.337939"``.
- ``no_downsample``: Allows analysis to be run without downsampling the data,
  **be advised no training_data is saved for these requests**, only results are
  returned.
- ``override_7_day_limit``:  Allows to override the requirement for 24h or 7d
  data, **be advised training_data is not suitable for training with on these
  requests**

Using the mod:`settings.FLUX_SELF_API_KEY` key allows for additional parameters
to be passed and can be used to remove the maximum algorithm limit of 3.
Additional keys that can be passed with this key are:

- ``save_training_data_on_false``: A boolean that enables the saving of
  training data even if the instance is not found to be anomalous.
- ``metric_namespace_prefix``: A string of the namespace prefix to use or a
  ``None`` boolean value.
- ``shard_test``: A boolean that allows for run a shard test while Skyline is
  running in a clustered mode.  No analysis is done, the requests is just
  distributed to correct cluster node which returns a response indicating that
  it received and would process the request.

``algorithms``
--------------

DOCUMENTATION STILL UNDER DEVELOPMENT

The ``algorithms`` object is the most complicated so it is covered in detail
here.  It consists keys for each algorithm to be run with any algorithm
parameters defined for each algorithm.  If no algorithm parameters are passed
e.g. ``{}`` then the default parameters for the algorithm will be used.

Along with these algorithm definitions there is a ``consensus`` key with can
hold a list of lists to define what combinations of triggered algorithms will be
classed as an anomaly.  For example below we define that both the ``sigma`` and
``spectral_residual`` algorithms must be triggered to be classed as an anomaly.

.. code-block:: json-object

  {
      "key": "0123456789abcdefghijlmnopqrstu",
      "metric": "prometheus_http_requests_total{alias=\\"Prometheus\\", code=\\"200\\", handler=\\"/api/v1/query_range\\", instance=\\"localhost:9090\\", job=\\"prometheus\\"}",
      "timeout": 60,
      "timeseries": [[1668689060, 23.3], ..., [1668689120, 162.9]],
      "algorithms": {
          "sigma": {"sigma_value": 3, "consensus": 6},
          "spectral_residual": {},
      },
      "consensus": [["sigma", "spectral_residual"]],
      "reference": "128186f730da2d9e"
  }

Above we define that the ``sigma`` algorithms must achieve a consensus of 6
e.g. 6 of the 9 sigma algorithms must trigger for ``sigma`` to be classed as
anomalous.  We also define that ``spectral_residual`` should be run with it's
default settings ``{}``.  In the overall ``consensus`` key we define that
**both** ``sigma`` and ``spectral_residual`` **must** trigger to class the
analysis as anomalous.

Should you wish to have an anomalous classification based on any of the
algorithms triggering, you can specify the overall ``consensus`` as:

.. code-block:: json-object

      "algorithms": {
          "sigma": {"sigma_value": 3, "consensus": 6},
          "spectral_residual": {},
      },
      "consensus": [["sigma"], ["spectral_residual"]],

This would mean that if either one of the algorithms triggered, the instance
would be classed as anomalous.

It is possible to not specify the ``algorithms`` key and defaults will be used.
Currently the defaults are ``sigma`` and ``spectral_residual``, however if a
better combination of algorithms is discovered this can change in future
versions.

``anomaly_window``
^^^^^^^^^^^^^^^^^^

A number of the algorithms have a special algorithm parameter of
``anomaly_window``.  This parameter allows one to specify a window size in which
to classify an instance as anomalous.

Basically this allows you to configure the algorithm to check if any value in
the last x values are anomalous, rather than just checking the last value.

If present in an algorithm, by default the ``anomaly_window`` is 1.  Skyline
generally only determines if the final value in a timeseries is anomalous
related to the rest of the timeseries, however, due to the nature of Vortex, it
may help the set the ``anomaly_window`` to a number of data points.

Because Vortex is adhoc analysis, the methods you use to decide whether to
analyse a timeseries may be lagged, meaning by the time your analysis/alerter
has decided something should be further assessed and surfaces the data, the
timeseries may already have changed.  Perhaps you see a value of 800 and your
normal values are between 10 and 30, by the time you surface the data to send
to Vortex the last value may have updated to say 22, using the default
``anomaly_window`` the timeseries may be classified as not anomalous, because
the value of 22 is being used as the decider.  Having an ``anomaly_window`` of
say 5 and say the following values would be evaluated ``17, 14, 24, 800, 22``
and the timeseries would be classified as anomalous.

**It is important to consider this window in the context of any downsampling
that may be applied to the data.**

For example if you are sending 7 days of data at a resolution of 5 seconds
(because you have decided to not following the advice on downsampling above) and
you wanted to check for anomalies in the last 10 minutes, the ``anomaly_window``
will be 1 not 40 when downsampling is applied.

Available algorithms
^^^^^^^^^^^^^^^^^^^^

The currently the following algorithms are available to use with Vortex, each
listed here will be handled in detail regarding their individual
``algorithm_parameters`` which are available.  These may be subject to or
removal at some point in the future:

- ``default``
- ``sigma`` - Collection of the original Skyline 3sigma algorithms
- ``dbscan`` - Density-based spatial clustering of applications with noise
- ``lof`` - Local Outlier Factor
- ``one_class_svm`` - One Class SVM
- ``pca`` - Principal Component Analysis
- ``prophet`` - the fbprophet algorithm (long running and not suited to realtime 
    analysis)
- ``spectral_residual`` - Spectral Residual
- ``isolation_forest`` - Isolation Forest
- ``m66`` - A skyline changepoint detection algorithm, similar to
    PELT, ruptures and Bayesian Online Changepoint Detection, however it is
    more robust to instaneous outliers and more conditionally selective of
    changepoints.
- ``adtk_level_shift`` - ADTK LevelShiftAD algorithm
- ``adtk_persist`` - ADTK PersistAD algorithm
- ``adtk_seasonal`` - ADTK SeasonalAD algorithm
- ``adtk_volatility_shift`` - ADTK VolatilityShiftAD algorithm
- ``macd`` - Moving Average Convergence/Divergence
- ``spectral_entropy`` - Spectral Entropy
- ``mstl`` - statsforecast MSTL algorithm (the mstl algorithm is very long running
    and not suited for realtime analysis)

The algorithms are run in the order in which they are declared and the analysis
will stop before running all algorithms, if a consensus is reached before all
the algorithms are run or if consensus cannot be reached.

To determine the various parameters that can be passed for each algorithm and
understand what those parameters do, please refer to the algorithm source code
in skyline/skyline/custom_algorithms/ algorithm py file.

DOCUMENTATION STILL UNDER DEVELOPMENT