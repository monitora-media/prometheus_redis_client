import base64
import json
from collections.abc import Sequence
from functools import partial
from typing import Self

from prometheus_redis_client.registry import REGISTRY, Registry


class BaseRepresentation:
    def output(self) -> str:
        raise NotImplementedError


class MetricRepresentation(BaseRepresentation):
    """Representation of a metric value."""

    def __init__(self, name, labels, value):
        self.name = name
        self.labels = labels
        self.value = value

    def output(self) -> str:
        if self.labels is None:
            labels_str = ''
        elif labels_str := ','.join(f'{key}=\"{self.labels[key]}\"' for key in sorted(self.labels.keys())):
            labels_str = '{' + labels_str + '}'

        return '%(name)s%(labels)s %(value)s' % {
            'name': self.name,
            'labels': labels_str,
            'value': self.value,
        }


class DocRepresentation(BaseRepresentation):
    """Reperesentation of a metric documentation."""

    def __init__(self, name: str, type: str, documentation: str):
        self.doc = documentation
        self.name = name
        self.type = type

    def output(self):
        return f'# HELP {self.name} {self.doc}\n# TYPE {self.name} {self.type}'


class WithLabels:
    """Wrap functions and put 'labels' argument to it."""

    __slots__ = (
        'instance',
        'labels',
        'wrapped_functions_names',
    )

    def __init__(self, instance: 'BaseMetric', labels: dict[str, str], wrapped_functions_names: list[str]):
        self.instance = instance
        self.labels = labels
        self.wrapped_functions_names = wrapped_functions_names

    def __getattr__(self, attr: str):
        if attr not in self.wrapped_functions_names:
            raise TypeError(f'Labels work with functions {self.wrapped_functions_names} only')

        wrapped_function = getattr(self.instance, attr)
        return partial(wrapped_function, labels=self.labels)


class BaseMetric:
    type: str = ''
    wrapped_functions_names: Sequence[str] = []

    def __init__(
        self,
        name: str,
        documentation: str,
        labelnames: Sequence[str] = None,
        registry: Registry = REGISTRY,
        prefix: str = 'mb_',
    ):
        self.documentation = documentation
        self.labelnames = labelnames or []
        self.name = prefix + name
        self.registry = registry
        self.registry.add_metric(self)

    def doc(self) -> DocRepresentation:
        """Get a documentation for the metric."""
        return DocRepresentation(self.name, self.type, self.documentation)

    def get_metric_group_key(self):
        """Get a Redis key for the metric grouping."""
        return f'{self.name}_group'

    def get_metric_key(self, labels: dict[str, str | int], suffix: str = None):
        """
        Get a Redis key for the metric.

        :param labels: Labels for the metric.
        :param suffix: Optional suffix for the key. Useful in case of composite metrics (`_total`, `_sum`, etc.)
        """
        return '{}{}:{}'.format(self.name, suffix or '', self.pack_labels_b64(labels).decode('utf-8'))

    def parse_metric_key(self, key: bytes) -> tuple[str, str]:
        """
        Given a Redis key, return the metric name and encoded labels as a base64 string.

        Labels can then be decoded with `unpack_labels`.

        :param key: Redis key
        :return: tuple (metric name, packed labels)
        """
        return key.decode('utf-8').split(':', maxsplit=1)

    def pack_labels_b64(self, labels: dict[str, str | int]) -> bytes:
        """Encode labels to base64 string in UTF8 bytes."""
        return base64.b64encode(json.dumps(labels, sort_keys=True).encode('utf-8'))

    def unpack_labels(self, labels: bytes) -> dict[str, str | int]:
        """Decode base64 string to labels dict."""
        return json.loads(base64.b64decode(labels).decode('utf-8'))

    def _check_labels(self, labels: dict[str, str | int]):
        """Make sure given labels match defined label names."""
        if set(labels.keys()) != set(self.labelnames):
            raise ValueError('Expect define all labels: {}. Got only: {}'.format(
                ', '.join(self.labelnames),
                ', '.join(labels.keys()),
            ))

    # Return `Self` to disguise WithLabels and keep this metric's methods such as `inc` and `set` available.
    def labels(self, *args, **kwargs) -> Self:
        """Bind labels to the metric when caputring a value."""
        labels = dict(zip(self.labelnames, args))
        labels.update(kwargs)
        self._check_labels(labels)
        return WithLabels(instance=self, labels=labels, wrapped_functions_names=self.wrapped_functions_names)
