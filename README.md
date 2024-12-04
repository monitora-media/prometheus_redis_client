## prometheus_redis_client

A simple prometheus "client" and registry implemented in Redis. You need to provide Redis connection string `METRICS_REDIS_URI` as ENV variable or define it in `django.settings` if you're using the lib within in Django.

You can expose metrics via HTTP either using `METRICS_REDIS_URI='redis://127.0.0.1:6379' prometheus_metrics_http --metrics-module=myproject.metrics` or implement your own command f.e. like this:


```
import logging

from django.conf import settings
from django.core.management.base import CommandParser
from prometheus_redis_client import http_server

from monitora.utils.management import MediaboardBaseCommand


logger = logging.getLogger(__name__)


class Command(MediaboardBaseCommand):

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument('-b', '--bind_ip', type=str, default='127.0.0.1')
        parser.add_argument('-p', '--bind_port', type=int, default=settings.METRICS_PORT)

    def handle(self, *args, **options):
        http_server.run_server(options['bind_ip'], options['bind_port'], 'monitora.metrics')

```
