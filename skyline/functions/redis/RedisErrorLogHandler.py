"""
RedisErrorLogHandler.py
"""
import logging

from settings import SKYLINE_TMP_DIR
from skyline_functions import get_redis_conn


# @added 20220328 - Feature #4018: thunder - skyline.errors
class RedisErrorLogHandler(logging.StreamHandler):
    """
    For every error logged add an increment to the app Redis errors key which is
    consumed by thunder and creates the
    skyline.<hostname>.<skyline_app>.logged_errors metrics.  If there is a
    problem with Redis itself obviously errors will not be captured in the
    errors metric but the Redis error will be caught by thunder itself.
    """
    def __init__(self, skyline_app):
        logging.StreamHandler.__init__(self)
        self.skyline_app = skyline_app
        try:
            self.redis_conn = get_redis_conn(skyline_app)
        except:
            self.redis_conn = None

    def emit(self, record):
        # Skip traceback entries because most errors report the traceback and a
        # error log entry
        if 'Traceback (most recent call last)' in str(record):
            pass
        else:
            record_in_file = False
            if not self.redis_conn:
                record_in_file = True
            try:
                redis_key = '%s.log.errors.per_minute' % self.skyline_app
                self.redis_conn.incrby(redis_key)
                self.redis_conn.expire(redis_key, 119)
                del record
            except:
                record_in_file = True
            if record_in_file:
                try:
                    log_errors_file = '%s/%s.log_errors.txt' % (
                        SKYLINE_TMP_DIR, self.skyline_app)
                    with open(log_errors_file, 'a') as f:
                        f.write('1\n')
                except:
                    pass
