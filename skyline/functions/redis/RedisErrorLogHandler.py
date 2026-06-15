"""
RedisErrorLogHandler.py
"""
import logging

from settings import SKYLINE_TMP_DIR
from skyline_functions import get_redis_conn
# @added 20240206 - Task #5254: Upgrade Skyline to Python 3.10
#                   Task #5178: Build and test skyline v4.1.0
try:
    from settings import ENABLE_DEBUG_ERROR_LOGS
except:
    ENABLE_DEBUG_ERROR_LOGS = False

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

        # @added 20240202 - Task #5254: Upgrade Skyline to Python 3.10
        #                   Task #5178: Build and test skyline v4.1.0
        # This is for DEVELOPMENT ONLY
        if ENABLE_DEBUG_ERROR_LOGS:
            debug_file = '%s/%s.debug.errors.txt' % (SKYLINE_TMP_DIR, self.skyline_app)
            with open(debug_file, 'a') as fh:
                fh.write(str(record))

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
