######
Mirage
######

The Mirage service is responsible for analyzing selected timeseries at custom
time ranges when a timeseries seasonality does not fit within
:mod:`settings.FULL_DURATION`.  Mirage allows for testing of real time data
and algorithms in parallel to Analyzer.  Mirage was inspired by Abe Stanway's
Crucible and the desire to extend the temporal data pools available to Skyline
in an attempt to handle seasonality better, reduce noise and increase signal,
specifically on seasonal metrics.

An overview of Mirage
=====================

- Mirage is fed specific user defined metrics by Analyzer.
- Mirage gets timeseries data for metrics from Graphite.
- Mirage does not have its own ``ALERTS`` settings it uses :mod:`settings.ALERTS`
  just like Analyzer does.
- Mirage also sends anomaly details to Panorama, like Analyzer does.

.. figure:: images/crucible/mirage/skyline.mirage.overview.png
   :alt: An overview of Mirage

`Fullsize overview image <_images/skyline.mirage.overview.png>`_ for a clearer picture.

Why Mirage?
-----------

Analyzer's :mod:`settings.FULL_DURATION` somewhat limits Analyzer's usefulness
for metrics that have a seasonality / periodicity that is greater than
:mod:`settings.FULL_DURATION`.  This means Analyzer is not great in terms of
"seeing the bigger picture" when it comes to metrics that have a weekly pattern
as well as a daily patterns for example.

Increasing :mod:`settings.FULL_DURATION` to anything above 24 hours (86400) is
not necessarily realistic or useful, because the greater the
:mod:`settings.FULL_DURATION`, the greater memory required for Redis and the
longer Analyzer will take to run.

What Mirage can and cannot do
=============================

It is important to know that Mirage is not necessarily suited to making highly
variable metrics less noisy e.g. spikey metrics.

Mirage is more useful on fairly constant rate metrics which contain known
or expected seasonalities.  For example take a metric such as
office.energy.consumation.per.hour,  this type of metric would most likely have
2 common seasonalities.

As an example we can use the `Department of Education
<https://www.gov.uk/government/publications/greening-government-and-transparency-commitments-real-time-energy-data>`_
Sanctuary Buildings energy consumption public `data set
<https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/476574/Real_time_energy_data_October.csv>`_
to demonstrate how Mirage and Analyzer views are different.  The energy
consumption in an office building is a good example of a multi-seasonal data set.

* Office hour peaks
* Out of hour troughs
* Weekend troughs
* Holidays
* There could be summer and winter seasonality too

For now let us just consider the daily and weekly seasonality.

The difference between the Analyzer and Mirage views of a timeseries
--------------------------------------------------------------------

.. plot::

    # A bit of a contrived example...
    import numpy as np

    # @added 20161119 - Task #1758: Update deps in Ionosphere
    # The update to Sphinx or matplotlib is causing a user error warning
    # This call to matplotlib.use() has no effect
    # because the backend has already been chosen;
    # matplotlib.use() must be called *before* pylab, matplotlib.pyplot,
    # or matplotlib.backends is imported for the first time.
    # Seen in adding Redis data plot to the alerters, fixes like this
    import matplotlib
    matplotlib.use('Agg')

    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import matplotlib.patches as mpatches
    import csv
    import string
    import os
    import datetime as dt
    import urllib2

    # Department for Education real-time energy data: October 2015
    # https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/476574/Real_time_energy_data_October.csv
    datafile = '../examples/data/Real_time_energy_data_October.csv'
    if not os.path.exists(datafile):
        datafile = '/tmp/Real_time_energy_data_October.csv'
        url = 'https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/476574/Real_time_energy_data_October.csv'
        response = urllib2.urlopen(url)
        with open(datafile, 'w') as fw:
            fw.write(response.read())

    values = []
    with open(datafile, 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for row in reader:
            values_row = ', '.join(row)
            values_only_string = string.replace(values_row, ' ', '')
            values_list = values_only_string.split(',')
            values.append(values_list)

    hours = []
    current_index = 2
    for index, value in enumerate(values):
        if value[1] == 'Day/Time':
            while current_index < 50:
                hours.append(value[current_index])
                current_index += 1
    two_weeks = '01/10/2015 02/10/2015 03/10/2015 04/10/2015 05/10/2015 06/10/2015 07/10/2015 08/10/2015 09/10/2015 10/10/2015 11/10/2015 12/10/2015 13/10/2015 14/10/2015'
    data = []
    for index, value in enumerate(values):
        # if value[0] != 'Site' and value[1] != '31/10/2015':
        if value[1] in two_weeks:
            current_index = 2
            current_hour = 0
            while current_index < 50:
                date = '%s %s' % (value[1], hours[current_hour])
                line_data = [date, value[current_index]]
                data.append(line_data)
                current_index += 1
                current_hour += 1

    tmp_datafile = '/tmp/skyline.docs.mirage.energy_data.csv'
    if os.path.exists(tmp_datafile):
        os.remove(tmp_datafile)

    for element in data:
        ts_line = '%s, %.2f\n' % (element[0], float(element[1]))
        with open(tmp_datafile, 'a') as fw:
            fw.write(ts_line)

    hours, consumption = np.loadtxt(
        tmp_datafile, unpack=True,
        delimiter=',',
        converters={0: mdates.strpdate2num('%d/%m/%Y %H:%M')})

    if os.path.exists(tmp_datafile):
        os.remove(tmp_datafile)

    fig = plt.figure(figsize=(14, 5))

    x_anno1 = dt.datetime.strptime('02/10/2015 06:00', '%d/%m/%Y %H:%M')
    x_anno2 = dt.datetime.strptime('03/10/2015 06:00', '%d/%m/%Y %H:%M')
    plt.annotate(
        'Analyzer at 86400\nFULL_DURATION\nwould probably fire\naround here',
        xy=(x_anno2, 130), xycoords='data',
        xytext=(0.2, 0.5), textcoords='axes fraction',
        arrowprops=dict(facecolor='red', shrink=0.01),
        horizontalalignment='right', verticalalignment='top')

    plt.axvspan(x_anno1, x_anno2, alpha=0.4, color='pink')
    analyzer_full_duration = mpatches.Patch(color='pink', label='Analyzer FULL_DURATION')
    plt.legend(handles=[analyzer_full_duration])

    x_anno3 = dt.datetime.strptime('09/10/2015 06:00', '%d/%m/%Y %H:%M')
    x_anno4 = dt.datetime.strptime('10/10/2015 06:00', '%d/%m/%Y %H:%M')
    plt.annotate(
        'Analyzer at 86400\nFULL_DURATION\nwould probably fire\naround here',
        xy=(x_anno4, 130), xycoords='data',
        xytext=(0.7, 0.5), textcoords='axes fraction',
        arrowprops=dict(facecolor='red', shrink=0.01),
        horizontalalignment='right', verticalalignment='top')

    plt.axvspan(x_anno3, x_anno4, alpha=0.4, color='pink')

    x_anno5 = dt.datetime.strptime('03/10/2015 06:00', '%d/%m/%Y %H:%M')
    x_anno6 = dt.datetime.strptime('09/10/2015 06:00', '%d/%m/%Y %H:%M')
    plt.axvspan(x_anno5, x_anno6, alpha=0.4, color='blue')
    x_anno7 = dt.datetime.strptime('02/10/2015 04:00', '%d/%m/%Y %H:%M')
    x_anno8 = dt.datetime.strptime('02/10/2015 06:00', '%d/%m/%Y %H:%M')
    plt.axvspan(x_anno7, x_anno8, alpha=0.4, color='blue')
    x_anno9 = dt.datetime.strptime('10/10/2015 06:00', '%d/%m/%Y %H:%M')
    x_anno10 = dt.datetime.strptime('10/10/2015 08:00', '%d/%m/%Y %H:%M')
    plt.axvspan(x_anno9, x_anno10, alpha=0.4, color='blue')

    mirage_full_duration = mpatches.Patch(color='blue', label='Mirage FULL_DURATION')

    plt.annotate(
        '', xy=(x_anno7, 370), xycoords='data',
        xytext=(x_anno10, 370), textcoords='data',
        arrowprops={'arrowstyle': '<->'})
    plt.text(x_anno1, 375, 'Mirage FULL_DURATION period')

    plt.annotate(
        '', xy=(x_anno2, 310), xycoords='data',
        xytext=(x_anno1, 310), textcoords='data',
        arrowprops={'arrowstyle': '<->'})
    plt.text(x_anno1, 310, 'Analyzer FULL_DURATION period')

    plt.annotate(
        '', xy=(x_anno3, 310), xycoords='data',
        xytext=(x_anno4, 310), textcoords='data',
        arrowprops={'arrowstyle': '<->'})
    plt.text(x_anno3, 310, 'Analyzer FULL_DURATION period')

    plt.legend(handles=[analyzer_full_duration, mirage_full_duration])

    plt.title('Department of Education Sanctuary Buildings - energy consumption\nAn example of Skyline Analyzer and Mirage data views')
    plt.figtext(0.99, 0.01, 'Sample data from https://www.gov.uk/government/uploads/system/uploads/attachment_data/file/476574/Real_time_energy_data_October.csv', horizontalalignment='right')
    plt.plot_date(x=hours, y=consumption, markersize=1.3)
    plt.gcf().autofmt_xdate()

    plt.show()

`Fullsize image <_images/mirage-1.png>`_ for a clearer picture.

As we can see above, on a Saturday morning the energy consumption does not
increase as it normally does during the week days. Analyzer would probably find
the metric to be anomalous if :mod:`settings.FULL_DURATION` was set to 86400 (24
hours), Saturday morning would seem anomalous.

However, if the metric's alert tuple was set up with a
``SECOND_ORDER_RESOLUTION_HOURS`` of 168, Mirage would analyze the data point
against a week's worth of data points and the Saturday and Sunday daytime data
points would have less probability of triggering as anomalous.  *The above
image is plotted as if the Mirage* ``SECOND_ORDER_RESOLUTION_HOURS`` *was set to
172 hours just so that the trailing edges can be seen.*

A real world example with tenfold.com
-------------------------------------

:blak3r2: Our app logs phone calls for businesses and I want to be able to
  detect when VIP phone systems go down or act funny and begin flooding us with
  events.  Our work load is very noisy from 9-5pm... where 9-5 is different for
  each customer depending on their workload so thresholding and modeling isn't
  good.

:earthgecko:  Yes, Mirage is great at user defined seasonality, in your case
  weekday 9-5 peaks, evening drop offs, early morning and weekend lows - multi
  seasonal, Mirage is the ticket.
  Your best bet would be to try 7days (168) as your SECOND_ORDER_RESOLUTION_HOURS
  value for those app log metrics, however, you may get away with a 3 day
  window, it depends on the metrics really, but it may not be noisy at 3 days
  resolution, even at the weekends.

Mirage "normalizes"
-------------------

Mirage is a "tuning" tool for seasonal metrics and it is important to understand
that Mirage is probably using aggregated data (unless your Graphite is not using
retentions and aggregating) and due to this Mirage will lose some resolution
resulting in it being less sensitive to anomalies than Analyzer is.

So Mirage does some "normalizing" if your have aggregations in Graphite (e.g
retentions), however it is analyzing the timeseries at the aggregated resolution
so it is "normalised" as the data point that Analyzer triggered on is ALSO
aggregated in the timeseries resolution that Mirage is analyzing.
Intuitively one may think it may miss it in the aggregation then.  This is true
to an extent, but Analyzer will likely trigger multiple times if the metric
**IS** anomalous, so when Analyzer pushes to Mirage again, each aggregation is
more likely to trigger as anomalous, **IF** the metric anomalous at the user
defined full duration.  A little flattened maybe, a little lag maybe, but less
noise, more signal.

Setting up and enabling Mirage
==============================

By default Mirage is disabled, various Mirage options can be configured in the
``settings.py`` file and Analyzer and Mirage can be configured as appropriate
for your environment.

For all the specific alert configurations see the `Alerts <alerts.html>`__ page.

Mirage requires some directories as per ``settings.py`` defines (these require
absolute path):

.. code-block:: bash

  mkdir -p $MIRAGE_CHECK_PATH
  mkdir -p $MIRAGE_DATA_FOLDER


Configure ``settings.py`` with some :mod:`settings.ALERTS` alert tuples that
have the ``SECOND_ORDER_RESOLUTION_HOURS`` defined. For example below is an
Analyzer only :mod:`settings.ALERTS` tuple that does not have Mirage enabled as
it has no ``SECOND_ORDER_RESOLUTION_HOURS`` defined:

.. code-block:: python

  ALERTS = (
             ('stats_counts.http.rpm.publishers.*', 'smtp', 300),  # --> Analyzer sends to alerter
  )

To enable Analyzer to send the metric to Mirage we append the metric alert tuple
in :mod:`settings.ALERTS` with the ``SECOND_ORDER_RESOLUTION_HOURS`` value.
Below we have used 168 hours to get Mirage to analyze **any** anomalous metric
in the 'stats_counts.http.rpm.publishers.*' namespace using using 7 days worth
of timeseries data from Graphite:

.. code-block:: python

  ALERTS = (
  #          ('stats_counts.http.rpm.publishers.*', 'smtp', 300),  # --> Analyzer sends to alerter
             ('stats_counts.http.rpm.publishers.*', 'smtp', 300, 168),  # --> Analyzer sends to Mirage
  )

Order Matters
-------------

.. warning:: It is important to note that Mirage enabled metric namespaces must
  be defined before non Mirage enabled metric namespace tuples as Analyzer uses
  the first alert tuple that matches.

So for example, with some annotation

.. code-block:: python

  ALERTS = (
             ('skyline', 'smtp', 1800),
             ('stats_counts.http.rpm.publishers.seasonal_pub1', 'smtp', 300, 168),    # --> To Mirage
             ('stats_counts.http.rpm.publishers.seasonal_pub_freddy', 'smtp', 300, 168),    # --> To Mirage
             ('stats_counts.http.rpm.publishers.*', 'smtp', 300),    # --> To alerter
  )

The above would ensure if Analyzer found seasonal_pub1 or seasonal_pub_freddy
anomalous, instead of firing an alert as it does for all other
``stats_counts.http.rpm.publishers.*``, because they have 168 defined, Analyzer
sends the metric to Mirage.

The below would NOT have the desired effect of analysing the metrics
seasonal_pub1 and seasonal_pub_freddy with Mirage

.. code-block:: python

  ALERTS = (
             ('skyline', 'smtp', 1800),
             ('stats_counts.http.rpm.publishers.*', 'smtp', 300),    # --> To alerter
             ('stats_counts.http.rpm.publishers.seasonal_pub1', 'smtp', 300, 168),    # --> NEVER gets reached
             ('stats_counts.http.rpm.publishers.seasonal_pub_freddy', 'smtp', 300, 168),    # --> NEVER gets reached
  )

Hopefully it is clear that the first ``stats_counts.http.rpm.publishers.*``
alert tuple would route ALL to alerter and seasonal_pub1 and seasonal_pub_freddy
would never get sent to Mirage to be analyzed.

Enabling
--------

And ensure that ``settings.py`` has Mirage options enabled, specifically the
basic ones:

.. code-block:: python

  ENABLE_MIRAGE = True
  ENABLE_FULL_DURATION_ALERTS = False
  MIRAGE_ENABLE_ALERTS = True

Start Mirage and restart Analyzer:

.. code-block:: bash

  cd skyline/bin
  ./mirage.d start
  ./analyzer.d restart

Rate limited
------------

Mirage is rate limited to analyze 30 metrics per minute, this is by design and
desired. Surfacing data from Graphite and analyzing ~1000 data points in a
timeseries takes less than 1 second and is much less CPU intensive than
Analyzer in general, but it is probably sufficient to have 30 calls to Graphite
per minute.  If a large number of metrics went anomalous, even with Mirage
discarding :mod:`settings.MIRAGE_STALE_SECONDS` checks due to processing limit,
signals would still be sent.

Periodic Checks
===============

Due to the fact that Analyzer feeds Mirage metrics to check when Analyzer finds
a Mirage metric anomalous, there are situations where a metric may change fairly
substantially over a period greater than :mod:`settings.FULL_DURATION`, for
example over 36 hours.  In these cases Analyser will not detect these changes as
anomalous and therefore Mirage will not have a chance to check them.

Periodic checks can be enabled on Mirage metric namespaces that are declared in
:mod:`settings.MIRAGE_PERIODIC_CHECK_NAMESPACES`.  It is not advisable to
analyse all Mirage metrics periodically as this would probably have a
significant impact on both Skyline and Graphite.

Periodic checks should only be run on Mirage key metrics.  Periodic checks suit
KPI metrics that are have fairly constant rate.  Periodic checks should not be
implemented on general server and app metrics.

It is important to note that the :mod:`settings.MIRAGE_PERIODIC_CHECK_NAMESPACES`
will match the exact string and dotted namespace elements as per
documented in :mod:`settings.SKIP_LIST`.

The types of metrics that suit periodic checks are total/global metrics,
summed metrics rather than individual metrics.  The type of individual metrics
that suit Mirage periodic checks are metrics like % disk space used on fairly
consistent servers that are not expected to fluctuate too drastically over a
period of days, like a DB server volume.

You definitely do not what to run all your server or app metrics through Mirage
periodic checks.

What Mirage does
================

- If Analyzer finds a metric to be anomalous at :mod:`settings.FULL_DURATION`
  and the metric alert tuple has ``SECOND_ORDER_RESOLUTION_HOURS`` and
  :mod:`settings.ENABLE_MIRAGE` is ``True``, Analyzer will push the metric
  variables to the Mirage check file.
- Mirage watches for added check files.
- When a check is found, Mirage determines what the configured
  ``SECOND_ORDER_RESOLUTION_HOURS`` is for the metric from the tuple in
  :mod:`settings.ALERTS`
- Mirage queries graphite to surface the json data for the metric timeseries at
  ``SECOND_ORDER_RESOLUTION_HOURS``.
- Mirage then analyses the retrieved metric timeseries against the configured
  :mod:`settings.MIRAGE_ALGORITHMS`.
- If a metric is an Ionosphere enabled metric, then Mirage does not alert,
  but hands the metric off to Ionosphere by adding an Ionosphere check
  file.
- If the metric is anomalous over ``SECOND_ORDER_RESOLUTION_HOURS`` then alerts
  via the configured alerters for the matching metric :mod:`settings.ALERT`
  tuple and sets the metric alert key for ``EXPIRATION_TIME`` seconds.
- Mirage will alert for a Mirage metric that has been returned from Ionosphere
  as anomalous having not matched any known features profile or layers.
