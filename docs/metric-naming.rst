.. role:: skyblue
.. role:: red

Metric names
============

Follows the Graphite standards see https://graphite.readthedocs.io/en/latest/tags.html#carbon

- No non-ascii names strings containing utf8 chars as they do not always work.

To enable Skyline to analyse data in different ways in the future it is
important to consider how your metric names are structured.

There are numerous best practices concerning metric naming in the monitor space
for different applications.

https://docs.datadoghq.com/developers/guide/what-best-practices-are-recommended-for-naming-metrics-and-tags/
https://prometheus.io/docs/practices/naming/
https://www.robustperception.io/on-the-naming-of-things

However this is a brief example of how to create metric names that are useful in
automatic classification tasks.

Let us say we are collecting oceanographic data we could name so metrics

sea-surface-temperature
salinity
sea-surface-windspeed
sea-surface-winddir

Think of metric name in terms of what is being represented and what features
each can have.

sea.water.temperature.surface
sea.water.salinity
sea.wind.speed.surface
sea.wind.direction.surface

Using

environment.air-quality
environment.air-temp
or
environment.air.quality
environment.air.temp
environment.air.humidity
environment.water.quality
environment.water.temp
environment.water.salinity

air is the "device" in this case and it has features such as quality, temp, etc.
environment has features such as air, water, light, etc
Just as web1 has features such as cpu, memory, etc

In order to allow for automated algorithmic or ML based classification,
clusterings, correlations or learning being able to cluster and slice on the
dimensions of the metric namespaces is important, otherwise mappings would need
to be made for each dimension.

For example, the above type of naming allows for automatically analysis and
correlations to be done on the dimensions of environment.air whereas using
environment.air-quality and environment.air-temp are things unto themselves.
