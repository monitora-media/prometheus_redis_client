import logging
import atexit
import os
import threading
import time
from typing import TYPE_CHECKING

from redis import StrictRedis, from_url

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from prometheus_redis_client.metrics import Metric

METRICS_REDIS_URI = ''
try:
    logger.debug('Getting METRICS_REDIS_URI django.conf')
    from django.conf import settings
    METRICS_REDIS_URI = settings.METRICS_REDIS_URI
except Exception as e:
    logger.debug('Failed to get METRICS_REDIS_URI from django.conf: %s', e)

if not METRICS_REDIS_URI:
    logger.debug('Getting METRICS_REDIS_URI from ENV')
    METRICS_REDIS_URI = os.environ.get('METRICS_REDIS_URI')

assert METRICS_REDIS_URI, 'Expecting METRICS_REDIS_URI being defined either as ENV var or in django.conf'


logger = logging.getLogger(__name__)


class Refresher:
    default_refresh_period_secs = 30.0

    def __init__(self, refresh_period: float = default_refresh_period_secs, timeout_granule=1):
        self._refresh_functions_lock = threading.Lock()
        self._start_thread_lock = threading.Lock()
        self.refresh_period = refresh_period
        self.timeout_granule = timeout_granule
        self._clean()

    def _clean(self):
        self._refresh_functions = []
        self._refresh_enable = False
        self._refresh_cycle_thread = threading.Thread(target=self.refresh_cycle, daemon=True)
        self._should_close = False

    def add_refresh_function(self, func: callable):
        with self._refresh_functions_lock:
            self._refresh_functions.append(func)

        self.ensure_started()

    def ensure_started(self):
        with self._start_thread_lock:
            if self._refresh_enable:
                return

            logger.debug('Starting Redis Prometheus registry refresher')
            self._refresh_cycle_thread.start()
            self._refresh_enable = True

    def stop(self):
        logger.debug('Stopping Redis Prometheus registry refresher')

        self._should_close = True

        if self._refresh_enable:
            self._refresh_cycle_thread.join()

        self._clean()

    def refresh_cycle(self):
        """Check `close` flag every `timeout_granule` and refresh after `refresh_period`."""
        current_time_passed = 0

        while True:
            logger.debug('Running Redis Prometheus registry refresh cycle')
            current_time_passed += self.timeout_granule

            if self._should_close:
                return

            if current_time_passed >= self.refresh_period:
                current_time_passed = 0

                with self._refresh_functions_lock:
                    for refresh_func in self._refresh_functions:
                        refresh_func()

            time.sleep(self.timeout_granule)


class Registry:
    """In-memory storage for metrics."""

    def __init__(self, redis: StrictRedis = None, refresher: Refresher = None, exit_cleanup: bool = False):
        self._metrics: list[Metric] = []
        self.redis = None
        self.refresher = refresher or Refresher()
        self.redis = redis
        self.exit_cleanup = exit_cleanup

    def output(self) -> str:
        """Dump all metrics and their values to a string in Prometheus-compatible format."""
        lines: list[str] = []

        for metric in self._metrics:
            lines.append(metric.doc())
            lines += sorted(metric.collect(), key=lambda x: x.output())

        return '\n'.join(m.output() for m in lines)

    def add_metric(self, *metrics: 'Metric'):
        """Add metric to this registry."""

        added = {m.name for m in self._metrics}
        new = {m.name for m in metrics}

        if conflicting := added.intersection(new):
            raise ValueError('Metrics {} already added'.format(', '.join(conflicting)))

        for m in metrics:
            self._metrics.append(m)

    def stop(self):
        logger.debug('Stopping Redis Prometheus registry')

        if self.refresher:
            self.refresher.stop()

        if self.exit_cleanup:
            for metric in self._metrics:
                logger.debug('Cleaning up metric %s', metric.name)
                metric.cleanup()

        self._metrics = []


REGISTRY = Registry(redis=from_url(METRICS_REDIS_URI))

# Make sure to stop the registry when the program exits.
atexit.register(REGISTRY.stop)
