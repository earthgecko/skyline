==================
Monitoring Skyline
==================

It should be noted that something should monitor the Skyline processes.
Over a long enough timeline Python or Redis or some I/O issue is going to lock
up and the Python process is just going to hang.  This means that it is not
really sufficient to just monitor the process with something like monit.

KISS
====

Skyline logs
------------

Each Skyline app is expected to log at a certain intervals, by design for this
very purpose.  So if a Skyline app has not written to its log in 120 seconds, it
has hung.  So some simple bash scripts can be crond to restart the application
if the log has not been written to in 120 seconds.

This very simple mechanism solves a very difficult problem to solve within
Python itself.  It works.

The Graphite pickle
-------------------

The same can be said for the pickle from Graphite.  The same simple methodology
can be employed on Graphite too, a simple script to determine the number of
carbon fullQueueDrops to the destinations metric for your skyline node/s e.g.
`carbon.relays.skyline-host-a.destinations.123_234_213_123:2024:None.fullQueueDrops`
If the number of drops is greater than x, restart carbon-cache or just the
carbon-relay if you have rcarbon-relay enabled, which you should have :)

A further advantage having carbon-relay enabled independently of carbon-cache
is that implementing a monitor script on your `carbon.relays.*.fullQueueDrops`
metrics means that if carbon-cache itself has a pickle issue, it should be
resolved by a carbon-relay restart, just like Skyline Horizon is fixed by its
monitor.
