***********************
Development - Debugging
***********************

A large number of tools and techniques have been used to get to grips
with the Skyline code base, it's debugging, profiling, performance tuning, memory
leaking and so forth.  These tools and techniques give "some" insight
into the number of objects, calls, reference counts, etc, etc.  A myriad maze of
Python's intestinal details, where it is possible to get lost and tangled up
fairly quickly.  Python being such a vast and mature ecosystem, the choices are
vast.  The following is for future reference on what works, where and how and
how well.  Due to the number of objects and calls that Skyline makes in a run
some of these tools perform better than others, however it must be noted that
there may be some tools that are very good, but do not necessarily give output
that is useful in terms of making sense of some of it or knowing where to find
the needle in the haystack, that is if you know it is a needle you are looking
for and not a nail.

Code profiling
==============

The following is a list and notes on some of the tools used, with examples if
useful for future reference.

TODO - doc using vizsnake, cProfile, et al

Memory debugging
================

As per http://www.bdnyc.org/2013/03/memory-leaks-in-python/ and
http://www.lshift.net/blog/2008/11/14/tracing-python-memory-leaks

There are only a few reasons for memory leaks in Python.

- There's some object you are opening (repeatedly) but not closing - ``plt.savefig``
- You are repeatedly adding data to a list/dict and not removing the old data.
  (for instance, if you're appending data to an array instead of replacing the
  array) - ``self.mirage_metrics.append(metric)``
- There's a memory leak in one of the libraries you're calling. (unlikely)

This is ``True``, the hard part is finding 1 and 2 if you do not know what you
are looking for or how to find it.

analyzer_debug
--------------

See examples of the implementation of the below in analyzer_debug/analyzer.py
at commit https://github.com/earthgecko/skyline/commit/c637ae1bf43126459fe82e21c280d457cafb88aa

https://github.com/earthgecko/skyline/tree/c637ae1bf43126459fe82e21c280d457cafb88aa/skyline/analyzer_debug

multiprocessing
---------------

A note on multiprocessing.  multiprocessing has the advantage of a parent
process not inheriting any memory leaks from a spawned process. When the process
ends or is terminated by the parent, all memory is returned and there is no
opportunity for the parent to inherit any objects from the spawned process other
than what is returned by the process itself.  This should probably be caveated
with there may be a possibility that traceback objects may leak under certain
circumstances.

The overall cost in terms of the memory consumed for the spawned process may be
fairly high, currently in Analyzer around 80 MB, but it is known, whereas memory
leaks are very undesirable.

A move has been made always use multiprocessing spawned processes to deal with
any function or operation that involves surfacing and analyzing or processing
any timeseries data, so that when done, there are no possibilities of incurring
any memory leaks in the parent, e.g. triggering an alert_smtp is now a
multiprocessing process, yes 80 MB of memory to send an email :)

resource
--------

To be honest in terms of identifying at which point memory leaks were occurring
after trying all of the below listed tools, the one which end up pin pointing
them was literally stepping through the code operation by operation and wrapping
each operation in analyzer.py and alerters.py in:

.. code-block:: python

  import resource
  logger.info('debug :: Memory usage before blah: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
  # do blah
  logger.info('debug :: Memory usage after blah: %s (kb)' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

This was done iteratively at selected points where memory leaking was suspected
to be occurring.  This enabled identifying exactly what the before and after
memory usage where in terms of when functions where executed and after the
process the memory usage incremented up and the increment remained.

Days were spent trying to glean this exact information with all the below
mentioned tools and techniques, but in terms of debugging and understanding
Analyzer's memory usage and allocation none provided exact enough actionable
information, in all likelihood this was probably a result of a PEBKAB type of
problem and possibly not wielding the tools to their full potential.  That said
they definitely do provide some insight in other ways.

pmap
----

Some simple use of pmap can highlight a memory leak if there are a large number
of anon allocations and also gives a good overview of what the process is using
in terms of memory and by what.

From the command line query the app parent pid to determine all the allocations
using more than 1000 kb of memory e.g.

.. code-block:: bash

  pmap 4306 | sed 's/\]//' | awk '{print $2 " " $NF}' | sed 's/K / /' | sort -n | awk '$1 > 1000'

The output is similar to

.. code-block:: bash

  1012 /opt/python_virtualenv/projects/skyline2711/lib/python2.7/site-packages/pandas/lib.so
  1392 /opt/python_virtualenv/projects/skyline2711/lib/python2.7/site-packages/scipy/special/_ufuncs.so
  1504 /opt/python_virtualenv/projects/skyline2711/lib/python2.7/site-packages/pandas/tslib.so
  1516 /opt/python_virtualenv/projects/skyline2711/bin/python2.7
  ...
  ...
  35920 /opt/python_virtualenv/projects/skyline2711/lib/python2.7/site-packages/numpy/.libs/libopenblasp-r0-39a31c03.2.18.so
  63524 anon
  96844 /usr/lib/locale/locale-archive
  798720 798720K

If you pmap a spawned process rather than the parent process fairly variable
results are expected.

In terms of catching python memory leaks, you can grep out the anon allocations
at intervals between app runs

.. code-block:: bash

  pmap 4306 | sed 's/\]//' | awk '{print $2 " " $NF}' | sed 's/K / /' | sort -n | awk '$1 > 1000' | grep anon | tee /tmp/pmap.4306.anon.1
  # Some minutes later
  pmap 4306 | sed 's/\]//' | awk '{print $2 " " $NF}' | sed 's/K / /' | sort -n | awk '$1 > 1000' | grep anon | tee /tmp/pmap.4306.anon.2
  diff /tmp/pmap.4306.anon.1 /tmp/pmap.4306.anon.2

An anon process with incrementing memory usage is a likely list candidate.
More and more added anon objects is likely something/s with a reference cycle or
some object not closed, e.g. a matplotlib savefig which does not have
``fig.clf()`` and ``plt.close()`` on it (although did not help).

gc
--

Forcing Python garbage collection can be useful in terms of determining objects,
type and reference counts

.. code-block:: python

  from gc import get_objects
  # Debug with garbage collection - http://code.activestate.com/recipes/65333/
  import gc

  #
  # Before something
  before = defaultdict(int)
  after = defaultdict(int)
  for i in get_objects():
      before[type(i)] += 1

  # Something or lots of things

  for i in get_objects():
      after[type(i)] += 1
  gc_result = [(k, after[k] - before[k]) for k in after if after[k] - before[k]]
  for i in gc_result:
      logger.info('debug :: %s' % str(i))

In the relevant log there will be a ton of output similar to this

.. code-block:: bash

  tail -n 1000 /var/log/skyline/analyzer.log | grep " (<" | grep "14:21:4"
  2016-08-06 14:21:44 :: 1349 :: (<class '_ast.Eq'>, 36)
  2016-08-06 14:21:44 :: 1349 :: (<class '_ast.AugLoad'>, 36)
  2016-08-06 14:21:44 :: 1349 :: (<class 'six.Module_six_moves_urllib'>, 36)
  2016-08-06 14:21:44 :: 1349 :: (<class 'scipy.stats._continuous_distns.lognorm_gen'>, 36)
  2016-08-06 14:21:44 :: 1349 :: (<class '_weakrefset.WeakSet'>, 6480)
  2016-08-06 14:21:44 :: 1349 :: (<class 'scipy.stats._continuous_distns.halfnorm_gen'>, 36)
  2016-08-06 14:21:44 :: 1349 :: (<class 'matplotlib.markers.MarkerStyle'>, 4318)
  ...
  ...

Forcing ``gc.collect()`` every run did seem ease a memory leak initially, but it
did not solve it and ``gc.collect()`` over the period of 12 hours took
increasing long to run.  Preferably gc should only ever be used in Skyline for
debugging and development.

pyflakes
--------

pyflakes is useful for finding imported and defined things that are not used and
do not need to be imported, every little helps.

.. code-block:: bash

  (skyline2711) earthgecko@localhost:/opt/python_virtualenv/projects/skyline2711$ bin/python2.7 -m flake8 --ignore=E501 /home/earthgecko/github/skyline/develop/skyline/skyline/analyzer/analyzer.py
  /home/earthgecko/github/skyline/develop/skyline/skyline/analyzer/analyzer.py:11:1: F401 'msgpack.unpackb' imported but unused
  /home/earthgecko/github/skyline/develop/skyline/skyline/analyzer/analyzer.py:13:1: F401 'os.system' imported but unused
  /home/earthgecko/github/skyline/develop/skyline/skyline/analyzer/analyzer.py:17:1: F401 'socket' imported but unused
  /home/earthgecko/github/skyline/develop/skyline/skyline/analyzer/analyzer.py:22:1: F401 'sys' imported but unused
  /home/earthgecko/github/skyline/develop/skyline/skyline/analyzer/analyzer.py:28:1: F403 'from algorithm_exceptions import *' used; unable to detect undefined names
  /home/earthgecko/github/skyline/develop/skyline/skyline/analyzer/analyzer.py:54:13: F401 'mem_top.mem_top' imported but unused

pympler
-------

Does not work with Pandas and 2.7

mem_top
-------

Some info but moved on to other tools - see analyzer_debug app

pytracker
---------

pytracker, although did not get to far with pytracker, could only get it running
in agent.py could not get any Trackable outputs in analyzer.py all None?

See analyzer_debug app.

objgraph
--------

Some useful info, see analyzer_debug app.

guppy and heapy
---------------

heapy or hpy can given some useful insight similar to resource.
