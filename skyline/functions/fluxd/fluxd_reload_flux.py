# @added 20230929 - Feature #5090: flux.d reload
#                   Feature #4324: flux - reload external_settings
import os
import sys

#sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
#sys.path.insert(0, os.path.dirname(__file__))

use_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
print(use_path)
sys.path.append(os.path.join(use_path, os.pardir))
sys.path.insert(0, use_path)

#sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname((os.path.realpath(__file__)))), os.pardir))
#print(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))), os.pardir))
#sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname((__file__)))))
#print(os.path.dirname(os.path.dirname(os.path.dirname((__file__)))))


if True:
    try:
        from settings import PID_PATH
    except:
        PID_PATH = '/var/run/skyline'
    from skyline_functions import get_redis_conn_decoded

def run():
    """
    Run reload_flux
    """
    print('fluxd_reload_flux.py - running reload_flux')
    PIDFILE = '%s/flux.pid' % PID_PATH
    if not os.path.isfile(PIDFILE):
        print('ERROR pid file %s does not exist, cannot restart flux' % PIDFILE)
        sys.exit(1)
    try:
        redis_conn_decoded = get_redis_conn_decoded('flux')
        redis_conn_decoded.set('skyline.external_settings.update.metrics_manager', 'fluxd_reload_flux')
        print('added Redis key skyline.external_settings.update.metrics_manager')
    except Exception as err:
        print('ERROR failed to add Redis key skyline.external_settings.update.metrics_manager - %s' % err)
        sys.exit(1)
    print('fluxd_reload_flux.py - flux instructed to reload')
    return 0

if __name__ == '__main__':
    retval = run()
    sys.exit(retval)
