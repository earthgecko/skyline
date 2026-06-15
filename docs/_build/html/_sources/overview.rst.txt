.. role:: skyblue
.. role:: red
.. role:: brow

Overview
========

Welcome to Skyline.

What is Skyline?
----------------

Skyline is a Python based anomaly detection and not anomalous detection stack
that analyses, anomaly detects, deflects, fingerprints, compares and learns vast
amounts of streamed time series data.  Skyline consists of a number of apps that
each do different things in the analysis pipeline.

**The anomaly detection problem has been solved.**

We have a plethora of algorithms and algorithmic methods for detecting anomalies.
Detecting anomalies is **EASY**!  Not detecting anomalies is **HARD**.

As Rajiv Shah (@rajistics) from Snowflake recently said:

  "After simple baselines, Anomaly detection is hard."

The irony is that in advanced 3rd, 4th and 5th generation anomaly detection, it
is not about detecting anomalies (the easy part).  Once you move from 1st and
2nd generation anomaly detection methods, anomaly detection is about **not**
detecting anomalies.  Luckily metrics are generally co-operative in that sense.

Seeing as we desire our metrics to be not anomalous most of the time and we want
to know when they ARE anomalous and given the fact that we try and build systems
that try to behave within not anomalous bounds so they perform well, due to
these facts we have:

- A lot of metric time series data that are not anomalous most of the time.
- A lot of data to train with on what is **NOT anomalous** given a time series
  data set, rather than simply focusing on what is anomalous, also focusing on
  what is not anomalous.

Skyline focuses on anomaly detection but it's primary focus has become on
**not anomalous** detection.  This is acheived with using multiple time series
similiarity search methods, which are highly accurate.

- SITTSSCA - The Skyline-Ionosphere-Tsfresh Time Series Similarities Comparison
  Algorithm which compares the "fingerprints" of the two time series generated
  from features extracted from the time series.  These can determine if they
  closely resemble each other in terms of the "power/energy", range and
  "movement".
- Making extensive use of motifs with Matrix Profile and MASS (Mueen's Algorithm
  for Similarity Search) (via stumpy) for searching time series sub-sequences
  (motifs) under z-normalized Euclidean distance for similarity.  These with a
  few additional simple validation algorithms such as area of the curve and
  max and min range comparison to ensure that matches are accurate.

The use of these methods enables Skyline to be trained and autonomously learn
what is normal without any training (unsupervised).  This autonomous learning is
highly accurate and reliable.  This is due to the fact it is based on
well-established mathematical and geometric principles.  Compared to neural
networks and black-box and probabilistic methods, which can be harder
to understand and interpret, Matrix Profile and MASS are straightforward
distance calculations which are relatively simple to understand in terms of
explainability and they are reliable in terms of their results can be trusted
because the results are based purely on maths and geometry.  It is a no
parameters, zero knowledge, no tuning, FAST method that is accurate.

It sounds perfect and it pretty much is.

This all results in having a 6th generation anomaly detection and not anomalous
detection system, that is years ahead of anything else in this space.

However it takes a little effort on your part to set up and configure Skyline,
and then there is the time it takes you to do training, but Skyline is now very
good at learning on its own, so you no longer need to do much training.

What you need
-------------

Curiosity and a reasonable Linux VM running CentOS Stream 9 and quite a bit of
time :)

Too much effort?
----------------

`anomify`_ offer a managed version of Skyline for people that do not have a vast
amount of time to spare.  You'll get access to unreleased features and support
from developers that have honed numerous Skyline integrations to alert on
important metrics.

Anomify are looking for further test partners with various types of data and
data sources, if you think you may have interesting metrics send an email to
hello@anomify.ai and get in touch.

.. _anomify: https://anomify.ai
