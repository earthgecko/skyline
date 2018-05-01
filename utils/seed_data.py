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

    print 'notice :: testing the Horizon parameters'

    if not settings.UDP_PORT:
        print 'error  :: could not determine the settings.UDP_PORT, please check you settings.py'
    else:
        print 'info   :: settings.UDP_PORT :: ' + str(settings.UDP_PORT)

    horizon_params_ok = False
    horizon_use_ip = False
    connect_test_metric = 'horizon.test.params'
    connect_test_datapoint = 1
    packet = msgpack.packb((connect_test_metric, connect_test_datapoint))
    test_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    try:
        test_sock.sendto(packet, (socket.gethostname(), settings.UDP_PORT))
        horizon_params_ok = True
        print 'notice :: Horizon parameters OK'
    except Exception as e:
        print 'warning :: there is an issue with the Horizon parameters'
        traceback.print_exc()
        print 'info   :: this is possibly a hostname related issue'
        print 'notice :: trying on 127.0.0.1'

    if not horizon_params_ok:
        try:
            test_sock.sendto(packet, ('127.0.0.1', settings.UDP_PORT))
            horizon_params_ok = True
            horizon_use_ip = '127.0.0.1'
            print 'notice :: using 127.0.0.1 - OK'
        except Exception as e:
            print 'warn   :: there is an issue with the Horizon parameters'
            traceback.print_exc()
            print 'warn :: Horizon is not available on UDP via 127.0.0.1'
            print 'error :: %s' % str(e)

    if not horizon_params_ok:
        print 'error  :: please check your HORIZON related settings in settings.py and restart the Horizon service'
        sys.exit(1)

    print 'notice :: pushing 8665 datapoints over UDP to Horizon'
    print 'info   :: this takes a while...'
    metric = 'horizon.test.udp'
    metric_set = 'unique_metrics'
    initial = int(time.time()) - settings.MAX_RESOLUTION

    with open(join(__location__, 'data.json'), 'r') as f:
        data = json.loads(f.read())
        series = data['results']
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        datapoints_sent = 0
        update_user_output = 0
        for datapoint in series:
            datapoint[0] = initial
            initial += 1
            packet = msgpack.packb((metric, datapoint))

            if not horizon_use_ip:
                sock.sendto(packet, (socket.gethostname(), settings.UDP_PORT))
            else:
                sock.sendto(packet, (horizon_use_ip, settings.UDP_PORT))

            update_user_output += 1
            datapoints_sent += 1
            if update_user_output == 1000:
                update_user_output = 0
                print 'notice :: ' + str(datapoints_sent) + ' datapoints sent'

    print 'notice :: connecting to Redis to query data and validate Horizon populated Redis with data'
    r = redis.StrictRedis(unix_socket_path=settings.REDIS_SOCKET_PATH)
    time.sleep(5)

    try:
        x = r.smembers(settings.FULL_NAMESPACE + metric_set)
        if x is None:
            raise NoDataException

        x = r.get(settings.FULL_NAMESPACE + metric)
        if x is None:
            raise NoDataException

        # Ignore the mini namespace if OCULUS_HOST isn't set.
        if settings.OCULUS_HOST != "":
            x = r.smembers(settings.MINI_NAMESPACE + metric_set)
            if x is None:
                raise NoDataException

            x = r.get(settings.MINI_NAMESPACE + metric)
            if x is None:
                raise NoDataException

        print 'info :: Congratulations! The data made it in. The Horizon pipeline is working.'
        print 'info :: If your analyzer and webapp were started you should be able to see a triggered anomaly for horizon.test.udp'
        print ('info :: at http://%s:%s' % (str(settings.WEBAPP_IP), str(settings.WEBAPP_PORT)))

    except NoDataException:
        print 'error :: Woops, looks like the data did not make it into Horizon. Try again?'
        print 'info :: please check your settings.py and ensure that the Horizon and Redis settings are correct.'
        print 'info :: ensure Redis is available via socket in your redis.conf'
        print 'info :: restart these services and try again'


if __name__ == '__main__':
    seed()
