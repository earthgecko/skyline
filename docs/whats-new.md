# What's new

## Skyline v1.0.3-beta - the crucible branch

Some documentation updates and setup.py things

## Skyline v1.0.2-beta - the crucible branch

Custom time zone settings for the rendering of Webapp Panorama dygraph graphs
see [Webapp - Time zones](webapp.html#time-zones)

## Skyline v1.0.1-beta - the crucible branch

Analyzer alerts with a graph plotted from Redis data, not just the Graphite
graph see [Analyzer SMTP alert graphs](analyzer.html#analyzer-smtp-alert-graphs)

## Skyline v1.0.0-beta - the crucible branch

The crucible branch had an issue open called `Bug #982: Too much in crucible branch`

> Too much in crucible branch
>
> I have added some pep20, sphinx docs and python package restructuring from @languitar etsy/skyline #93 - https://github.com/languitar/skyline/tree/setuptools - which turns skyline into a python package

The reality was that it was too difficult to reverse engineer all the changes
into separate branches, so it continued unabated...

This version of Skyline sees enough changes to worthy of a major version change.
That said the changes are/should be backwards compatible with older versions,
mostly (best efforts applied) with a caveat on the skyline graphite metrics
namespace and running Skyline under python virtualenv.

### Conceptual changes

The `FULL_DURATION` concept is a variable and it is a variable in more ways than
the settings.py context.  Conceptually now `FULL_DURATION`, `full_duration`,
`full_duration_seconds` and `full_duration_in_hours` are variables in different
contexts.  The `FULL_DURATION` concept was important in the Analyzer context,
but the concept of the full duration of a timeseries has become somewhat more
variably and is different within different scopes or apps within Skyline.  It is
no longer really a single static variable, it is handled quite dynamically in a
number of contexts now.

### Etsy to `time()`

This whats new will cover all the new things that the crucible branch
introduces since the last Etsy commit on master of
[etsy/skyline](https://github.com/etsy/skyline), although not strictly accrurate, for the
purposes of generality it shall be assumed that no one is running the new Skyline
features that have not been merged to the Etsy Skyline.

### anomaly_breakdown metrics

### mirage

See [Mirage](mirage.html)

### boundary

See [Boundary](boundary.html)

### crucible

See [Crucible](crucible.html)

### sphinx documentation

See [Building documentation](building-documentation.html)

### Performance tuning

See [Analyzer Optimisations](analyzer-optimisation.html)

### Process management

A number of the apps have had better process management handling added and the
parent process now spawns processes and terminates then if they have not
completed in `MAX_ANALYZER_PROCESS_RUNTIME`, `ROOMBA_TIMEOUT` and other apps
have this specified too, either using the `MAX_ANALYZER_PROCESS_RUNTIME` as a
hardcoded one where appropriate.  This handles a very limited number of edge
cases where something that is host machine related causes the Python process to
hang.

### Analyzer optimisations

See [Analyzer Optimisations](analyzer-optimisation.html)

### algorithm_breakdown metrics

See [Analyzer Optimisations](analyzer-optimisation.html)

### Improved log handling

- Prevent log overwrites

See [Logging](logging.html)

### Webapp

Some simple and basic security was added to the Webapp now it can be enabled
to access a MySQL database in the Panorama context.

- Only allow IP addresses in `WEBAPP_ALLOWED_IPS`
- There is now a single HTTP auth user `WEBAPP_AUTH_USER` and
  `WEBAPP_AUTH_USER_PASSWORD`
- The Webapp can now be served via gunicorn and Apache (or any other HTTP
  reverse proxy).

See [Webapp](webapp.html)

### Panorama

See [Panorama](panorama.html)
