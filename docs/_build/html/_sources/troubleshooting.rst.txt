.. role:: skyblue
.. role:: red

Trouble shooting
================

:skyblue:`Sky`:red:`line` does a lot of work and because of this you can
experience issues.

Generally the key indicators with :skyblue:`Sky`:red:`line` are CPU usage,
CPU iowait, CPU steal, disk I/O (specifically ``write_time``),
``skyline.<host>.analyzer.runtime`` and some key Redis metrics.

Redis performance is one of the best indicators as it is the cornerstone of
speed in the :skyblue:`Sky`:red:`line` pipeline.
Redis **cannot** be slow.  If Redis is running slow there is a problem.

If you are running a moderately loaded :skyblue:`Sky`:red:`line` instance/s in a
cloud environment over time you **probably will** experience some performance
degradation.  These incidents are generally (and in almost all cases related) to:

- A noisy neighbour on the host machine on which the :skyblue:`Sky`:red:`line`
  VM instances runs, which introduces iowait and resource limiting on
  host/resources which is divided and distributed between all the users of those
  resources.
- An actual issue on the host, disk I/O, memory allocation failure, swap, etc, etc
- Or you are just trying to do too much on the host on which you are running
  :skyblue:`Sky`:red:`line` on.

Hopefully you are monitoring the machine/VM on which you run
:skyblue:`Sky`:red:`line` with :skyblue:`Sky`:red:`line`, if not install
telegraf and start monitoring it!

Key :skyblue:`Sky`:red:`line` performance metrics and things to check if you are
experiencing issues are:

- ``skyline.<host>.analyzer.runtime``
- CPU user, CPU iowait, CPU steal
- disk io ``write_time``
- Using swap (although you swap should always be disabled)
- The roomba runtime in ``/var/log/skyline/horizon.log``
- Check that Transparent huge pages is disabled and has not been accidentally
  re-enabled on you kernel.  Check with
  ``cat /sys/kernel/mm/transparent_hugepage/enabled`` and it should be set to
  ``always madvise [never]``.  If it is not, use
  ``echo never > /sys/kernel/mm/transparent_hugepage/enabled`` to disable and
  restart Redis.
- redis ``INFO`` specifically ``rdb_last_bgsave_time_sec``
- redis ``INFO commandstats`` specifically ``usec_per_call`` stats in the case
  of :skyblue:`Sky`:red:`line` the following samples are normal times for an
  instances handling between 1500 to 3000 metrics (with everything running on
  the :skyblue:`Sky`:red:`line` VM, Redis, MariaDB, Graphite, etc)

::

    # Commandstats
    cmdstat_mget:calls=3879,usec=66793268,usec_per_call=17219.20
    cmdstat_smembers:calls=747026,usec=175273317,usec_per_call=234.63
    cmdstat_sunionstore:calls=687,usec=855735,usec_per_call=1245.61
    cmdstat_hgetall:calls=9518,usec=5108903,usec_per_call=536.76

::

    # Commandstats
    cmdstat_mget:calls=238831,usec=16884528653,usec_per_call=70696.55
    cmdstat_smembers:calls=75653989,usec=24095897878,usec_per_call=318.50
    cmdstat_sunionstore:calls=118861,usec=242901100,usec_per_call=2043.57
    cmdstat_hgetall:calls=1324879,usec=2020767640,usec_per_call=1525.25

If you are experiencing very high ``usec_per_call`` times on the above metrics
(much higher than the examples above) you probably have an IO problem

The good news is that :skyblue:`Sky`:red:`line` is **very robust** and runs in a
degraded state OK, analyzer may take 3 minutes to get through a single analysis
run, but it **still works fine**, it may failover into waterfall alerting and
albeit it will probably be much more chatty about its own metrics, but it will
carry on running.  Don't panic and just methodically try and determine where and
what the bad actor is.

Often just opening an issue with your service provider will get your issue
resolved, unless your are just doing too much on the machine in which case
just scale :skyblue:`Sky`:red:`line`.
