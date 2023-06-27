"""
RedisInfoLogHandler.py
"""
import logging

from skyline_functions import get_redis_conn


class RedisInfoLogHandler(logging.StreamHandler):
    """
    For every log event add an increment to the app Redis log info key which is
    consumed by thunder and creates the
    skyline.<hostname>.<skyline_app>.log_count metrics.
    """
    def __init__(self, skyline_app):
        logging.StreamHandler.__init__(self)
        self.skyline_app = skyline_app
        self.redis_conn = get_redis_conn(skyline_app)

    def emit(self, record):
        try:
            redis_key = '%s.log.info.per_minute' % self.skyline_app
            self.redis_conn.incrby(redis_key)
            self.redis_conn.expire(redis_key, 119)
            del record
        except:
            raise
