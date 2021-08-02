from __future__ import division
import redis
import msgpack
import sys
import time
from os.path import dirname, abspath

# add the shared settings file to namespace
sys.path.insert(0, ''.join((dirname(dirname(abspath(__file__))), "")))
if True:
    from skyline import settings
    # @added 20200507 - Feature #3532: Sort all time series
    from skyline_functions import sort_timeseries

metric = 'horizon.test.udp'


def check_continuity(metric, mini=False):
    r = redis.StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
    if mini:
        raw_series = r.get(settings.MINI_NAMESPACE + metric)
    else:
        raw_series = r.get(settings.FULL_NAMESPACE + metric)

    if raw_series is None:
        print('key not found at %s ' % metric)
        return 0, 0, 0, 0, 0

    unpacker = msgpack.Unpacker()
    unpacker.feed(raw_series)
    timeseries = list(unpacker)

    # @added 20200507 - Feature #3532: Sort all time series
    # To ensure that there are no unordered timestamps in the time
    # series which are artefacts of the collector or carbon-relay, sort
    # all time series by timestamp before analysis.
    original_timeseries = timeseries
    if original_timeseries:
        timeseries = sort_timeseries(original_timeseries)
        del original_timeseries

    length = len(timeseries)

    start = time.ctime(int(timeseries[0][0]))
    end = time.ctime(int(timeseries[-1][0]))
    duration = (float(timeseries[-1][0]) - float(timeseries[0][0])) / 3600

    last = int(timeseries[0][0]) - 10
    total = 0
    bad = 0
    missing = 0
    for item in timeseries:
        total += 1
        if int(item[0]) - last != 10:
            bad += 1
            missing += int(item[0]) - last
        last = item[0]

    total_sum = sum(item[1] for item in timeseries[-50:])

    return length, total_sum, start, end, duration, bad, missing


if __name__ == "__main__":
    length, total_sum, start, end, duration, bad, missing = check_continuity(metric)
    print("")
    print("Stats for full %s:" % metric)
    print("Length of %s" % length)
    print("Total sum of last 50 datapoints: %s" % total_sum)
    print("Start time: %s" % start)
    print("End time: %s" % end)
    print("Duration: %.2f hours" % duration)
    print("Number of missing data periods: %s" % bad)
    print("Total duration of missing data in seconds: %s" % missing)

    length, total_sum, start, end, duration, bad, missing = check_continuity(metric, True)
    print("")
    print("Stats for mini %s:" % metric)
    print("Length: %s" % length)
    print("Total sum of last 50 datapoints: %s" % total_sum)
    print("Start time: %s" % start)
    print("End time: %s" % end)
    print("Duration: %.2f hours" % duration)
    print("Number of missing data periods: %s" % bad)
    print("Total duration of missing data in seconds: %s" % missing)
