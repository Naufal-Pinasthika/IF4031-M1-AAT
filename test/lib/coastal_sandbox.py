import json
from pathlib import Path

from sandbox import Sandbox


class CoastalSandbox(Sandbox):
    def __init__(self, h):
        super().__init__(h)
        self.critical_topic = self.name + '-critical'
        self.critical_created = False

    def configure_credentials(self, values):
        values['PANTAI_AUTH_TOKEN'] = self.token

    def configure(self, config):
        config.update(COASTAL_URL='http://' + self.name + '-source:8080',
                      COASTAL_MAX_STALENESS_MS='1000', COASTAL_HTTP_TIMEOUT_MS='500',
                      COASTAL_POLL_INTERVAL_MS='100', KAFKA_CRITICAL_TOPIC=self.critical_topic)
        self.h.kube('exec', 'kafka-0', '--', 'env', 'KAFKA_HEAP_OPTS=-Xms32m -Xmx128m',
                    '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'localhost:9092',
                    '--create', '--topic', self.critical_topic, '--partitions', '3', '--replication-factor', '1')
        self.critical_created = True

    def source_script(self):
        return (Path(__file__).parent / 'coastal_source.py').read_text()

    def __enter__(self):
        super().__enter__()
        h = self.h
        try:
            config = json.loads(h.kube('get', 'configmap/notifier-consumer-config', '-o', 'json'))['data']
            config.update(KAFKA_TOPIC=self.name, KAFKA_CRITICAL_TOPIC=self.critical_topic,
                          NOTIFIER_CONSUMER_GROUP=self.name, NOTIFIER_STORE_PATH='/data/notifier.db')
            image = 'aat/notifier-consumer:' + h.args.notifier_image_tag
            notifier, service = self.deployment(self.name + '-notifier', 'consumer', image, 8086, [],
                                               envFrom=[{'configMapRef': {'name': self.name + '-notifier'}}],
                                               volumeMounts=[{'name': 'data', 'mountPath': '/data'}])
            notifier['spec']['template']['spec']['volumes'] = [{'name': 'data', 'emptyDir': {}}]
            self.apply([self.resource('ConfigMap', self.name + '-notifier', data=config), notifier, service])
            h.kube('rollout', 'status', 'deployment/' + self.name + '-notifier', '--timeout=180s')
            self.tunnel.terminate()
            self.tunnel.wait(timeout=10)
            services = json.loads(h.kube('get', 'services', '-l', 'aat-e2e=' + self.suffix, '-o', 'json'))['items']
            ips = {s['metadata']['name']: s['spec']['clusterIP'] for s in services}
            self.open_tunnel({'client': ips[self.name + '-api']+':8080', 'aggregator': ips[self.name + '-agg']+':8083',
                              'fixture': ips[self.name + '-source']+':8080', 'p5notifier': ips[self.name + '-notifier']+':8086'})
            return self
        except BaseException:
            import sys
            self.__exit__(*sys.exc_info())
            raise

    def __exit__(self, *exc):
        try:
            super().__exit__(*exc)
        finally:
            if self.critical_created:
                self.h.kube('exec', 'kafka-0', '--', 'env', 'KAFKA_HEAP_OPTS=-Xms32m -Xmx128m',
                            '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'localhost:9092',
                            '--delete', '--topic', self.critical_topic, '--if-exists')
