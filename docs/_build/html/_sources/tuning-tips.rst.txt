===========
Tuning tips
===========

Okay, so you've got everything all set up, data is flowing through, and...what?
You can't consume everything on time? Allow me to help:

1. Try increasing :mod:`settings.CHUNK_SIZE` - this increases the size of a chunk of
metrics that gets added onto the queue. Bigger chunks == smaller network traffic.

2. Try increasing :mod:`settings.WORKER_PROCESSES` - this will add more workers to
consume metrics off the queue and insert them into Redis.

3. Try decreasing :mod:`settings.ANALYZER_PROCESSES` - this all runs on one box (for
now), so share the resources!

4. Still can't fix the performance? Try reducing your `settings.FULL_DURATION`.
If this is set to be too long, Redis will buckle under the pressure.

5. Is your analyzer taking too long? Maybe you need to make your algorithms
faster, or use fewer algorithms in your ensemble.

6. Reduce your metrics! If you're using StatsD, it will spit out lots of
variations for each metric (sum, median, lower, upper, etc). These are largely
identical, so it might be worth it to put them in `settings.SKIP_LIST`.

7. Disable Oculus - if you set :mod:`settings.OCULUS_HOST` to '', Skyline will not
write metrics into the `mini.` namespace - this should result in dramatic speed
improvements.  At Etsy, they had a flow of about 5k metrics coming in every
second on average (with 250k distinct metrics). They used a 32 core Sandy Bridge
box, with 64 gb of memory. We experience bursts of up to 70k TPS on Redis. Here
are their relevant settings:

```
CHUNK_SIZE: 7000
WORKER_PROCESSES: 2
ANALYZER_PROCESSES: 25
FULL_DURATION: 86400
```

Smaller deployments
-------------------

Skyline runs OK on much less.  It can handle ~45000 metrics per minute on a 4
vCore, 4GB RAM cloud SSD server where the metric resolution is 1 datapoint per
60 seconds, it will run loaded, but OK.

Do take note of the notes in settings.py related to the :mod:`settings.ANALYZER_PROCESSES`
and :mod:`settings.ANALYZER_OPTIMUM_RUN_DURATION` if you are only processing a few
1000 metrics with a datapoint every minute then the optimum settings will most
likely be something similar to:

```
ANALYZER_PROCESSES = 1
ANALYZER_OPTIMUM_RUN_DURATION = 60
```

Reliability
-----------

Skyline has been verified running in production against 6.5 million request per
minute peak internet advertising infrastructure, since 20131016.

However it should be noted that something should monitor the Skyline processes.
Over a long enough timeline Python or Redis or some I/O issue is going to lock
up and the Python process is just going to hang.  This means that it is not
really sufficient to just monitor the process with something like monit.

Skyline has made some progress in monitoring its own process threads and keeping
itself running sanely, however not every possible issue can be mitigated against,
therefore some another external to Python monitoring can help.

See `Monitoring Skyline <monitoring-skyline.html>`__
