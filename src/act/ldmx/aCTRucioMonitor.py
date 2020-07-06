# Prometheus exporter of Rucio RSE information
# Should eventually move to Rucio probes

import time
from rucio.client import Client
from prometheus_client import start_http_server
from prometheus_client.core import GaugeMetricFamily, REGISTRY

from act.ldmx.aCTLDMXProcess import aCTLDMXProcess

class aCTRucioCollector:

    def __init__(self):
        self.metrics = {} # {RSE: {'used': 12324, 'files': 34}}

    def collect(self):

        rucio_rse_used = GaugeMetricFamily('rucio_rse_used',
                                           'Used space per RSE',
                                           labels=['rse_name'])

        rucio_rse_files = GaugeMetricFamily('rucio_rse_files',
                                            'Number of files per RSE',
                                            labels=['rse_name'])

        for rse, metric in self.metrics.items():
            rucio_rse_used.add_metric([rse], metric['used'])
            rucio_rse_files.add_metric([rse], metric['files'])

        yield rucio_rse_used
        yield rucio_rse_files


class aCTRucioMonitor(aCTLDMXProcess):

    def __init__(self):
        aCTLDMXProcess.__init__(self)
        self.rucio = Client()
        self.rucio_prometheus_port = int(self.arcconf.get(['monitor', 'rucioprometheusport']) or 0)

        if self.rucio_prometheus_port:
            start_http_server(self.rucio_prometheus_port)
            self.collector = aCTRucioCollector()
            REGISTRY.register(self.collector)
        else:
            self.log.info('Prometheus monitoring not enabled')

    def process(self):
        '''Actual metric gathering from Rucio is done at a low frequency here'''

        if not self.rucio_prometheus_port:
            return

        rses = self.rucio.list_rses()
        metrics = {}
        for rse in rses:
            info = self.rucio.get_rse_usage(rse['rse'], filters={'source': 'rucio'})
            metrics[rse['rse']] = next(info)

        self.collector.metrics = metrics
        time.sleep(120)


if __name__ == '__main__':
    am = aCTRucioMonitor()
    am.run()
    am.finish()
