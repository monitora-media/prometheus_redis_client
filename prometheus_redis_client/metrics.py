import collections
import copy
import json
import logging
import threading
from collections.abc import Callable, Sequence
from functools import partial, wraps

from prometheus_redis_client. base_metric import BaseMetric, MetricRepresentation
from prometheus_redis_client.helpers import timeit


logger = logging.getLogger(__name__)


def _silent_wrapper(func: Callable):
    """Wrap function for process any Exception and write it to log."""
    @wraps(func)
    def silent_function(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception:
            logger.warning('Error pushing a metric to Redis from func %r', func.__name__, exc_info=True)

    return silent_function


class Metric(BaseMetric):
    def collect(self) -> list[MetricRepresentation]:
        """
        Collect all the metrics values from Redis and return them as a
        list of `MetricRepresentation` instances.
        """
        redis = self.registry.redis
        group_key = self.get_metric_group_key()
        members = redis.smembers(group_key)
        results: list[MetricRepresentation] = []

        for metric_key in members:
            name, packed_labels = self.parse_metric_key(metric_key)
            labels = self.unpack_labels(packed_labels)
            value = redis.get(metric_key)

            if value is None:
                redis.srem(group_key, metric_key)
                continue

            results.append(MetricRepresentation(name=name, labels=labels, value=value.decode('utf-8')))

        return results

    def cleanup(self) -> None:
        """Perform optional cleanup of the metric."""

    def remove(self) -> None:
        """Removes metric from the registry."""


class Counter(Metric):
    """
    A metric that can only grow up.

    Prometheus automatically accounts for counter resets, so it is safe to
    use this metric in cases where the counter is reset to zero.
    """

    type = 'counter'
    wrapped_functions_names = ('inc', 'set', 'remove')

    def inc(self, value: int = 1, labels: dict[str, str | int] | None = None) -> None:
        """Increment the counter by the given value."""
        labels = labels or {}
        self._check_labels(labels)

        if not isinstance(value, int):
            raise ValueError(f'Value should be an `int`, got {type(value)}')

        self._inc(value, labels)

    @_silent_wrapper
    def _inc(self, value: int, labels: dict[str, str | int]):
        group_key = self.get_metric_group_key()
        metric_key = self.get_metric_key(labels)

        pipeline = self.registry.redis.pipeline()
        pipeline.sadd(group_key, metric_key)
        pipeline.incrby(metric_key, int(value))

        return pipeline.execute()[1]

    def set(self, value: int = 1, labels: dict[str, str | int] | None = None) -> None:
        """Set the counter to the given value."""
        labels = labels or {}
        self._check_labels(labels)

        if not isinstance(value, int):
            raise ValueError(f'Value should be an `int`, got {type(value)}')

        self._set(value, labels)

    @_silent_wrapper
    def _set(self, value: int, labels: dict):
        group_key = self.get_metric_group_key()
        metric_key = self.get_metric_key(labels)

        pipeline = self.registry.redis.pipeline()
        pipeline.sadd(group_key, metric_key)
        pipeline.set(metric_key, int(value))

        return pipeline.execute()[1]

    def remove(self, labels: dict[str, str | int] | None = None) -> None:
        """Removes metric from registry."""
        labels = labels or {}
        self._check_labels(labels)
        self._remove(labels)

    @_silent_wrapper
    def _remove(self, labels: dict):
        group_key = self.get_metric_group_key()
        metric_key = self.get_metric_key(labels)

        pipeline = self.registry.redis.pipeline()
        pipeline.srem(group_key, metric_key)
        pipeline.delete(metric_key)

        return pipeline.execute()[1]


class Summary(Metric):
    """A metric that calculates the sum of observed values and the count of observations."""

    type = 'summary'
    wrapped_functions_names = ('observe',)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.timeit = partial(timeit, metric_callback=self.observe)

    def observe(self, value: int | float, labels: dict[str, str | int] | None = None) -> None:
        """Observe a value."""
        labels = labels or {}
        self._check_labels(labels)
        self._observe(value, labels)

    @_silent_wrapper
    def _observe(self, value: int | float, labels: dict[str, str | int]):
        group_key = self.get_metric_group_key()
        sum_metric_key = self.get_metric_key(labels, '_sum')
        count_metric_key = self.get_metric_key(labels, '_count')

        pipeline = self.registry.redis.pipeline()
        pipeline.sadd(group_key, count_metric_key, sum_metric_key)
        pipeline.incrbyfloat(sum_metric_key, float(value))
        pipeline.incr(count_metric_key)

        return pipeline.execute()[1]


class Gauge(Metric):
    """A metric that can go up and down."""

    type = 'gauge'
    wrapped_functions_names = ('inc', 'set')
    default_expire_secs = 60

    def __init__(
        self, *args,
        expire_secs: int = default_expire_secs,
        autorefresh: bool = True,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.refresh_enable = autorefresh
        self._refresher_added = False
        self.lock = threading.Lock()
        self.gauge_values = collections.defaultdict(float)
        self.expire_secs = expire_secs
        self.index = None

    def add_refresher(self):
        if self.refresh_enable and not self._refresher_added:
            self.registry.refresher.add_refresh_function(self.refresh_values)
            self._refresher_added = True

    def _set_internal(self, key: str, value: float):
        self.gauge_values[key] = value

    def _inc_internal(self, key: str, value: float):
        self.gauge_values[key] += value

    def inc(self, value: float, labels: dict[str, str | int] | None = None):
        """Increment the gauge by the given value."""
        labels = labels or {}
        self._check_labels(labels)
        self._inc(value, labels)

    def dec(self, value: float, labels: dict[str, str | int] | None = None):
        """Decrement the gauge by the given value."""
        labels = labels or {}
        self._check_labels(labels)
        self._inc(-value, labels)

    @_silent_wrapper
    def _inc(self, value: float, labels: dict):
        with self.lock:
            group_key = self.get_metric_group_key()
            metric_key = self.get_metric_key(labels)

            pipeline = self.registry.redis.pipeline()
            pipeline.sadd(group_key, metric_key)
            pipeline.incrbyfloat(metric_key, float(value))
            pipeline.expire(metric_key, self.expire_secs)
            self._inc_internal(metric_key, float(value))
            result = pipeline.execute()

        self.add_refresher()
        return result

    def set(self, value: float, labels: dict[str, str | int] | None = None):
        """Set the gauge to the given value."""
        labels = labels or {}
        self._check_labels(labels)
        self._set(value, labels)

    @_silent_wrapper
    def _set(self, value: float, labels: dict):
        with self.lock:
            group_key = self.get_metric_group_key()
            metric_key = self.get_metric_key(labels)

            pipeline = self.registry.redis.pipeline()
            pipeline.sadd(group_key, metric_key)
            pipeline.set(metric_key, float(value), ex=self.expire_secs)
            self._set_internal(metric_key, float(value))
            result = pipeline.execute()

        self.add_refresher()

        return result

    def refresh_values(self):
        with self.lock:
            for key, value in self.gauge_values.items():
                self.registry.redis.set(key, value, ex=self.expire_secs)

    def cleanup(self):
        with self.lock:
            group_key = self.get_metric_group_key()
            keys = list(self.gauge_values.keys())

            if len(keys) == 0:
                return

            pipeline = self.registry.redis.pipeline()
            pipeline.srem(group_key, *keys)
            pipeline.delete(*keys)
            pipeline.execute()


class Histogram(Metric):
    """A metric that calculates the sum of observed values and the count of observations."""

    type = 'histogram'
    wrapped_functions_names = ('observe',)

    def __init__(self, *args, buckets: Sequence[int | float], **kwargs):
        super().__init__(*args, **kwargs)
        self.buckets = sorted(buckets, reverse=True)
        self.timeit = partial(timeit, metric_callback=self.observe)

    def observe(self, value: int | float, labels: dict[str, str | int] | None = None):
        labels = labels or {}
        self._check_labels(labels)
        self._observe(value, labels)

    @_silent_wrapper
    def _observe(self, value: int | float, labels: dict[str, str | int]):
        group_key = self.get_metric_group_key()
        sum_key = self.get_metric_key(labels, '_sum')
        counter_key = self.get_metric_key(labels, '_count')
        pipeline = self.registry.redis.pipeline()

        for bucket in self.buckets:
            if value > bucket:
                break

            labels['le'] = bucket

            bucket_key = self.get_metric_key(labels, '_bucket')
            pipeline.sadd(group_key, bucket_key)
            pipeline.incr(bucket_key)

        pipeline.sadd(group_key, sum_key, counter_key)
        pipeline.incr(counter_key)
        pipeline.incrbyfloat(sum_key, float(value))

        return pipeline.execute()

    def _get_missing_metric_values(self, redis_metric_values):
        missing_metrics_values = {json.dumps({'le': b}) for b in self.buckets}
        groups = set('{}')

        # If flag is raised then we should add
        # *_sum and *_count values for empty labels.
        sc_flag = True

        for mv in redis_metric_values:
            key = json.dumps(mv.labels, sort_keys=True)
            labels = copy.copy(mv.labels)

            if 'le' in labels:
                del labels['le']

            group = json.dumps(labels, sort_keys=True)

            if group == '{}':
                sc_flag = False

            if group not in groups:
                for b in self.buckets:
                    labels['le'] = b
                    missing_metrics_values.add(json.dumps(labels, sort_keys=True))

                groups.add(group)

            if key in missing_metrics_values:
                missing_metrics_values.remove(key)

        return missing_metrics_values, sc_flag

    def collect(self) -> list[MetricRepresentation]:
        existing_values = super().collect()
        missing_values, sc_flag = self._get_missing_metric_values(existing_values)
        missing_values = [
            MetricRepresentation(self.name + '_bucket', labels=json.loads(ls), value=0)
            for ls in missing_values
        ]

        if sc_flag:
            missing_values.append(MetricRepresentation(self.name + '_sum', labels={}, value=0))
            missing_values.append(MetricRepresentation(self.name + '_count', labels={}, value=0))

        return existing_values + missing_values
