# Logging

A few considerations to note about logging.

1. Logging Python multiprocessing threads is not easy.
2. Rotating Python TimedRotatingFileHandler logs in not easy.
3. logrotate on Python multiprocessing threads is not easy.
4. Logging something that eats through as much data and does as many things as
   Skyline does is not easy.

Skyline logging can be viewed as having 2 contexts:

1. Logging the Skyline application operations - Skyline app log
2. Logging the details of anomalous time series - syslog

Any long time users of Skyline will have undoubtedly run into a logging pain
with Skyline at some point.  Whether that be in terms of logs being overwritten
or no errors being logged but a Skyline module broken or hung.
Although logging is very mature, especially in an ecosystem as mature as Python
and one may be led to believe it should be easy, however in reality it is
non-trivial.

## A modified logging methodology

The logging implementation in Skyline is quite complicated due to the
abovementioned reasons.  It is important to note that some optimisations have
had to be added to the logging process which could be described as unintuitive AND
simply as un-log like.

Therefore it is important to explain why it is necessary to use a modified
logging methodology and so that the user understands conceptually what is being
logged and what is not being logged.

In terms of Analyzer if errors are encountered in any algorithms, we sample the
errors.  This achieves a balance between reporting errors useful and not using
lots of I/O and disk space if something goes wrong.  Skyline has the potential
to do a runaway log, but not reporting errors is not useful when you are trying
to pinpoint what is wrong.

However, it is right that algorithms should just True or False and not impact on
the performance or operation of Skyline in anyway.

This achieves a balance.

## Skyline app logs

Each Skyline app has its own log.  These logs are important from a Skyline
perspective in terms of monitoring the Skyline processes.
See [Monitoring Skyline]((monitoring-skyline.html))

It is recommended to NOT stream the Skyline app logs through any logging
pipeline e.g. rsyslog, logstash, elasticsearch, etc.  The syslog alert trigger
was added for this purpose.  If you wish to parse and rate anomalies do so via
syslog (see below).

Log rotation is handled by the Python TimedRotatingFileHandler and by default
keeps the 5 last logs:

```
        handler = logging.handlers.TimedRotatingFileHandler(
            settings.LOG_PATH + '/' + skyline_app + '.log',
            when="midnight",
            interval=1,
            backupCount=5)
```

The Skyline app logs and the rotation is relatively cheap on disk space even on
production machines handling 10s of 1000s of metrics each.

```
===============================================================
HOST:skyline-prod-4-96g-luk1 EXITCODE:0 STDOUT:
===============================================================
38M     /var/log/skyline
===============================================================
===============================================================
HOST:skyline-prod-3-40g-ruk2 EXITCODE:0 STDOUT:
===============================================================
22M     /var/log/skyline
===============================================================
===============================================================
HOST:skyline-prod-2-96g-luk1 EXITCODE:0 STDOUT:
===============================================================
52M     /var/log/skyline
===============================================================
```

## Skyline app log preservation

It should be noted that the bin/ bash scripts are used to ensure logs are
preserved and not overwritten by any Python multiprocessing processes or the
lack of `mode='a'` in the Python TimedRotatingFileHandler.  It is for this
reason that the Skyline app logs should not be streamed through a logging
pipeline as logstash, et al as this in a logging pipeline with say rsyslog can
result in the log being pushed multiple times due to the following scenario:

- Skyline app bin is called to start
- Skyline app bin makes a last log from the Skyline app log
- Skyline Python app is started, creates log with `mode=w`
- Skyline Python app pauses and the app bin script concatenates the last log and
  new log file
- Skyline bin script exits and Skyline Python app continues writing to the log

In terms of rsyslog pipelining this would result in the log being fully
submitted again on every restart.

## syslog

With this in mind a syslog alert trigger was added to Skyline apps to handle the
logging the details of anomalous time series to syslog,  groking the syslog for
Skyline logs and rates etc is the way to go.
