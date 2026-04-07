"""
End-to-end tests for prometheus_redis_client metrics.

Each test creates real metrics backed by a real Redis (via testcontainers),
writes values through the public API, then parses ``registry.output()``
with the official Prometheus Python client parser.
"""

import pytest
from prometheus_client.parser import text_string_to_metric_families

from prometheus_redis_client.metrics import Counter, Gauge, Histogram, Summary

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _samples(registry) -> dict[tuple[str, tuple], float]:
    """Parse registry output and flatten all samples into {(sample_name, frozen_labels): value}."""
    text = registry.output()
    return {
        (s.name, tuple(sorted(s.labels.items()))): s.value
        for family in text_string_to_metric_families(text)
        for s in family.samples
    }


# ---------------------------------------------------------------------------
# Counter
# ---------------------------------------------------------------------------

class TestCounter:
    def test_inc(self, registry):
        c = Counter('requests_total', 'Total requests', registry=registry)
        c.inc()
        c.inc(5)

        samples = _samples(registry)
        assert samples[('mb_requests_total', ())] == pytest.approx(6)

    def test_set(self, registry):
        c = Counter('set_counter', 'A settable counter', registry=registry)
        c.set(42)

        samples = _samples(registry)
        assert samples[('mb_set_counter_total', ())] == pytest.approx(42)

    def test_labels_independent(self, registry):
        c = Counter(
            'http_requests',
            'HTTP requests',
            labelnames=['method', 'status'],
            registry=registry,
        )
        c.labels(method='GET', status='200').inc(3)
        c.labels(method='POST', status='201').inc(1)
        c.labels(method='GET', status='200').inc(2)

        samples = _samples(registry)

        assert samples[('mb_http_requests_total', (('method', 'GET'), ('status', '200')))] \
               == pytest.approx(5)
        assert samples[('mb_http_requests_total', (('method', 'POST'), ('status', '201')))] \
               == pytest.approx(1)


# ---------------------------------------------------------------------------
# Gauge
# ---------------------------------------------------------------------------

class TestGauge:
    def test_set_and_inc(self, registry):
        g = Gauge(
            'temperature',
            'Current temperature',
            registry=registry,
            autorefresh=False,
        )
        g.set(20.5)
        g.inc(1.5)

        samples = _samples(registry)
        assert samples[('mb_temperature', ())] == pytest.approx(22.0)

    def test_dec(self, registry):
        g = Gauge('connections', 'Active connections', registry=registry, autorefresh=False)
        g.set(10.0)
        g.dec(3.0)

        samples = _samples(registry)
        assert samples[('mb_connections', ())] == pytest.approx(7.0)

    def test_labels_independent(self, registry):
        g = Gauge(
            'queue_size',
            'Queue size',
            labelnames=['queue'],
            registry=registry,
            autorefresh=False,
        )
        g.labels(queue='high').set(5.0)
        g.labels(queue='low').set(100.0)

        samples = _samples(registry)
        assert samples[('mb_queue_size', (('queue', 'high'),))] == pytest.approx(5.0)
        assert samples[('mb_queue_size', (('queue', 'low'),))] == pytest.approx(100.0)


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

class TestSummary:
    def test_observe(self, registry):
        s = Summary('request_duration', 'Request duration', registry=registry)
        s.observe(0.3)
        s.observe(0.5)
        s.observe(0.2)

        samples = _samples(registry)
        assert samples[('mb_request_duration_count', ())] == pytest.approx(3.0)
        assert samples[('mb_request_duration_sum', ())] == pytest.approx(1.0)

    def test_labels_independent(self, registry):
        s = Summary(
            'latency',
            'Latency',
            labelnames=['endpoint'],
            registry=registry,
        )
        s.labels(endpoint='/api').observe(0.1)
        s.labels(endpoint='/api').observe(0.2)
        s.labels(endpoint='/health').observe(0.01)

        samples = _samples(registry)

        assert samples[('mb_latency_count', (('endpoint', '/api'),))] == pytest.approx(2.0)
        assert samples[('mb_latency_sum', (('endpoint', '/api'),))] == pytest.approx(0.3)
        assert samples[('mb_latency_count', (('endpoint', '/health'),))] == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# Histogram
# ---------------------------------------------------------------------------

class TestHistogram:
    def test_observe(self, registry):
        h = Histogram(
            'response_time',
            'Response time',
            buckets=[0.1, 0.5, 1.0, 5.0],
            registry=registry,
        )
        h.observe(0.05)
        h.observe(0.8)
        h.observe(3.0)

        samples = _samples(registry)

        def bucket_val(le):
            return samples[('mb_response_time_bucket', (('le', str(le)),))]

        assert bucket_val(0.1) == pytest.approx(1)
        assert bucket_val(0.5) == pytest.approx(1)
        assert bucket_val(1.0) == pytest.approx(2)
        assert bucket_val(5.0) == pytest.approx(3)
        assert samples[('mb_response_time_count', ())] == pytest.approx(3)
        assert samples[('mb_response_time_sum', ())] == pytest.approx(3.85)

    def test_labels_independent(self, registry):
        h = Histogram(
            'db_query_time',
            'DB query time',
            buckets=[0.01, 0.1, 1.0],
            labelnames=['query'],
            registry=registry,
        )
        h.labels(query='select').observe(0.005)
        h.labels(query='insert').observe(0.5)

        samples = _samples(registry)

        assert samples[('mb_db_query_time_bucket', (('le', '0.01'), ('query', 'select')))] \
               == pytest.approx(1.0)
        assert samples[('mb_db_query_time_bucket', (('le', '0.1'), ('query', 'select')))] \
               == pytest.approx(1.0)
        assert samples[('mb_db_query_time_bucket', (('le', '0.1'), ('query', 'insert')))] \
               == pytest.approx(0.0)
        assert samples[('mb_db_query_time_bucket', (('le', '1.0'), ('query', 'insert')))] \
               == pytest.approx(1.0)
