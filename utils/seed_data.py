#!/usr/bin/env python

import json
import os
import socket
import sys
import time
from os.path import dirname, join, realpath
import traceback

import redis
import msgpack

# @added 20180823 - Bug #2552: seed_data.py testing with UDP does not work
import random

# Get the current working directory of this file.
# http://stackoverflow.com/a/4060259/120999
__location__ = realpath(join(os.getcwd(), dirname(__file__)))

# Add the shared settings file to namespace.
sys.path.insert(0, join(__location__, '..', 'skyline'))
# ignoreErrorCodes E402
import settings


class NoDataException(Exception):
    pass


def seed():

    print('notice :: testing the Horizon parameters')

    if not settings.UDP_PORT:
        print('error  :: could not determine the settings.UDP_PORT, please check you settings.py')
    else:
        print('info   :: settings.UDP_PORT :: %s' % str(settings.UDP_PORT))

    # @modified 20180823 - Bug #2552: seed_data.py testing with UDP does not work
    #                      seed_data.py testing with UDP does not work GH77
    # The use of a UDP socket test is flawed as it will always pass and never
    # except unless Horizon is bound to all, so use settings.HORIZON_IP
    if not settings.HORIZON_IP:
        print('error  :: could not determine the settings.HORIZON_IP, please check you settings.py')
    else:
        HORIZON_IP = settings.HORIZON_IP
        print('info   :: settings.HORIZON_IP :: %s' % str(HORIZON_IP))

    horizon_params_ok = False
    horizon_use_ip = False
    connect_test_metric = 'horizon.test.params'
    # @added 20190130 - Bug #3266: py3 Redis binary objects not strings
    #                   Branch #3262: py3
    print('info   :: connect_test_metric :: %s' % str(connect_test_metric))

    connect_test_datapoint = 1
    packet = msgpack.packb((connect_test_metric, connect_test_datapoint))

    # @modified 20180823 - Bug #2552: seed_data.py testing with UDP does not work
    #                      seed_data.py testing with UDP does not work GH77
    # The use of a UDP socket test is flawed as it will always pass and never
    # except unless Horizon is bound to all.
    if settings.HORIZON_IP == '0.0.0.0':
        test_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            test_sock.sendto(packet, (socket.gethostname(), settings.UDP_PORT))
            horizon_params_ok = True
            print('notice :: Horizon parameters OK')
        except Exception as e:
            print('warning :: there is an issue with the Horizon parameters')
            traceback.print_exc()
            print('info   :: this is possibly a hostname related issue')
            print('notice :: trying on 127.0.0.1')

    if not horizon_params_ok:
        # @modified 20180823 - Bug #2552: seed_data.py testing with UDP does not work
        #                      seed_data.py testing with UDP does not work GH77
        # There is no certainty that an anomaly can be triggered here as UDP is
        # used and some data points do not make it into Redis via UDP.
        # test_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        test_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            # @modified 20180823 - Bug #2552: seed_data.py testing with UDP does not work
            # Use the settings.HORIZON_IP value
            # test_sock.sendto(packet, ('127.0.0.1', settings.UDP_PORT))
            test_sock.sendto(packet, (settings.HORIZON_IP, settings.UDP_PORT))
            horizon_params_ok = True
            # horizon_use_ip = '127.0.0.1'
            horizon_use_ip = settings.HORIZON_IP
            # print 'notice :: using 127.0.0.1 - OK'
            print('notice :: using %s from settings.HORIZON_IP - OK' % str(settings.HORIZON_IP))
        except Exception as e:
            print('warn   :: there is an issue with the Horizon parameters')
            traceback.print_exc()
            # print 'warn :: Horizon is not available on UDP via 127.0.0.1'
            print('warn :: Horizon is not available on UDP via %s as defined in settings.HORIZON_IP' % str(settings.HORIZON_IP))
            print('error :: %s' % str(e))

    if not horizon_params_ok:
        print('error  :: please check your HORIZON related settings in settings.py and restart the Horizon service')
        sys.exit(1)

    print('notice :: pushing 8665 datapoints over UDP to Horizon')
    print('info   :: this takes a while...')
    metric = 'horizon.test.udp'
    # @added 20190130 - Bug #3266: py3 Redis binary objects not strings
    #                   Branch #3262: py3
    print('info   :: for metric :: %s' % str(metric))

    metric_set = 'unique_metrics'
    # @modified 20180823 - Bug #2552: seed_data.py testing with UDP does not work
    #                      seed_data.py testing with UDP does not work GH77
    # Only use the time series up until the current timestamp to it can be used
    # in testing and triggering an anomaly
    # initial = int(time.time()) - settings.MAX_RESOLUTION
    end_timestamp = int(time.time())
    initial = end_timestamp - settings.MAX_RESOLUTION

    with open(join(__location__, 'data.json'), 'r') as f:
        data = json.loads(f.read())
        series = data['results']
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        datapoints_sent = 0
        update_user_output = 0
        for datapoint in series:
            datapoint[0] = initial
            initial += 1

            # @added 20180823 - Bug #2552: seed_data.py testing with UDP does not work
            #                   seed_data.py testing with UDP does not work GH77
            # Triggering anomaly for testing purposes also NOTE that the data
            # sent via UDP is consistently missing the last 10 data points sent,
            # reason unknown, however to below method works for testing.
            if initial >= (end_timestamp - 14):
                # sleep a little so UDP does not drop the anomalous data points
                time.sleep(0.4)
                # Send an anomalous data point
                add_random = random.randint(500, 2000)
                original_value = int(datapoint[1])
                if initial == (end_timestamp - 10):
                    anomalous_datapoint = int(datapoint[1]) + 8000
                    datapoint[1] = float(anomalous_datapoint)
                    print('notice :: adding final anomalous data point - %s - value was %s and was modified with + 8000' % (
                        str(datapoint), str(original_value)))
                elif initial == end_timestamp:
                    anomalous_datapoint = int(datapoint[1]) + 11100
                    datapoint[1] = float(anomalous_datapoint)
                    print('notice :: adding final anomalous data point - %s - value was %s and was modified with + 11000' % (
                        str(datapoint), str(original_value)))
                else:
                    anomalous_datapoint = original_value + add_random
                    datapoint[1] = float(anomalous_datapoint)
                    print('notice :: adding anomalous data point - %s - value was %s and was modified with + %s' % (
                        str(datapoint), str(original_value), str(add_random)))

            # @modified 20191014 - Task #3272: horizon - listen - py3 handle msgpack bytes
            # packet = msgpack.packb((metric, datapoint))
            packet = msgpack.packb((str(metric), datapoint))

            if not horizon_use_ip:
                sock.sendto(packet, (socket.gethostname(), settings.UDP_PORT))
            else:
                sock.sendto(packet, (horizon_use_ip, settings.UDP_PORT))

            update_user_output += 1
            datapoints_sent += 1
            if update_user_output == 1000:
                update_user_output = 0
                print('notice :: ' + str(datapoints_sent) + ' datapoints sent')
            if initial == end_timestamp:
                break

    print('notice :: last sent data point - %s' % str(datapoint))
    print('notice :: total data points sent - %s' % str(datapoints_sent))
    print('notice :: connecting to Redis to query data and validate Horizon populated Redis with data')

    # @modified 20180519 - Feature #2378: Add redis auth to Skyline and rebrow
    if settings.REDIS_PASSWORD:
        # @modified 20190130 - Bug #3266: py3 Redis binary objects not strings
        #                      Branch #3262: py3
        r = redis.StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH)
        # r = redis.StrictRedis(password=settings.REDIS_PASSWORD, unix_socket_path=settings.REDIS_SOCKET_PATH, charset='utf-8', decode_responses=True)
    else:
        r = redis.StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
    time.sleep(5)

    try:
        x = r.smembers(settings.FULL_NAMESPACE + metric_set)
        if x is None:
            print('error :: failed to get Redis set %s%s' % (
                str(settings.FULL_NAMESPACE), str(metric_set)))
            print('error :: x :: %s' % (str(x)))
            raise NoDataException

        x = r.get(settings.FULL_NAMESPACE + metric)
        if x is None:
            print('error :: failed to get Redis key %s%s' % (
                str(settings.FULL_NAMESPACE), str(metric)))
            print('error :: x :: %s' % (str(x)))
            raise NoDataException
        # @added 20180823 - Bug #2552: seed_data.py testing with UDP does not work
        #                   seed_data.py testing with UDP does not work GH77
        else:
            unpacker = msgpack.Unpacker(use_list=False)
            unpacker.feed(x)
            timeseries = list(unpacker)
            print('info :: %s%s key exists and the time series has %s data points' % (
                settings.FULL_NAMESPACE, metric, str(len(timeseries))))

        # Ignore the mini namespace if OCULUS_HOST isn't set.
        if settings.OCULUS_HOST != "":
            x = r.smembers(settings.MINI_NAMESPACE + metric_set)
            if x is None:
                raise NoDataException

            x = r.get(settings.MINI_NAMESPACE + metric)
            if x is None:
                raise NoDataException

        print('info :: Congratulations! The data made it in. The Horizon pipeline is working.')
        print('info :: If your analyzer and webapp are started you should be able to see a')
        # @modified 20180715 - Task #2446: Optimize Ionosphere
        # print ('info :: at http://%s:%s' % (str(settings.WEBAPP_IP), str(settings.WEBAPP_PORT)))
        # print ('info :: at %s' % str(SKYLINE_URL))
        # @modified 20180719 - Bug #2460: seed_data.py SKYLINE_URL
        #                      seed_data.py SKYLINE_URL #60
        # @modified 20180915 - Feature #2550: skyline.dawn.sh
        # Added metric name and views to the output
        # print ('info :: at %s' % str(settings.SKYLINE_URL))
        print ('info ::  triggered anomaly and data for the horizon.test.udp metric in the')
        print ('info ::  now, then, Panorama and rebrow views at %s' % str(settings.SKYLINE_URL))
    except NoDataException:
        print('error :: Woops, looks like the data did not make it into Horizon. Try again?')
        print('info :: please check your settings.py and ensure that the Horizon and Redis settings are correct.')
        print('info :: ensure Redis is available via socket in your redis.conf')
        print('info :: restart these services and try again')


if __name__ == '__main__':
    seed()
