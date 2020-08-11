SNAB
====

SNAB is for algorithm testing and benchmarking for real time data and historical
data.

SNAB was inspired by the The Numenta Anomaly Benchmark project (https://github.com/numenta/NAB)
and attempts to provide a framework within Skyline to test and benchmark on
both real time and historical static data with any algorithms.

SNAB is not a replacement for NAB, it is simply a NAB like implementation that
suits Skyline.  SNAB is looser in its requirements for algorithms and removes
the O(N) time complexity requirement that NAB has and can also use algorithms
that batch process.
