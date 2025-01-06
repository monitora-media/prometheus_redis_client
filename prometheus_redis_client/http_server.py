import argparse
import importlib
import logging
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

from prometheus_redis_client import REGISTRY

logger = logging.getLogger(__name__)


class MetricsHandler(BaseHTTPRequestHandler):
    metrics_module = ''

    def do_GET(self):
        # neeed to import this on the fly to get fresh stuff
        try:
            importlib.import_module(self.metrics_module)
        except ImportError as e:
            raise ImportError(f'Could not import the metrics module "{self.metrics_module}". Are you sure it exists?') from e

        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()
        metrics = REGISTRY.output()
        self.wfile.write(bytes(metrics, 'utf-8'))


def run_server(ip: str, port: int, metrics_module: str):
    print(f'Starting Prometheus metrics server on {ip}:{port}')
    logger.debug('Current metrics:\n%s', REGISTRY.output())
    MetricsHandler.metrics_module = metrics_module
    httpd = HTTPServer((ip, port), MetricsHandler)
    try:
        httpd.serve_forever()
    except (KeyboardInterrupt, SystemExit):
        print('Interrupted. Closing server.')
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description='Starts metrics server')

    parser.add_argument('--metrics-module', type=str, required=True)
    parser.add_argument('--bind-ip', type=str, default='127.0.0.1')
    parser.add_argument('--bind-port', type=int, default=9690)
    options = parser.parse_args()

    run_server(options.bind_ip, options.bind_port, options.metrics_module)


if __name__ == '__main__':
    main()
