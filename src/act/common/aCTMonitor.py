# Monitor process to export prometheus data
from prometheus_client import start_http_server
from prometheus_client.core import GaugeMetricFamily, REGISTRY
from act.common.aCTProcess import aCTProcess

class aCTPrometheusCollector:

    def __init__(self, arcdb):
        self.db = arcdb

    def collect(self):
        queued_arc_jobs = GaugeMetricFamily('arc_queued_jobs',
                                            'Queued jobs per ARC CE',
                                            labels=['ce_endpoint'])

        running_arc_jobs = GaugeMetricFamily('arc_running_jobs',
                                             'Running jobs per ARC CE',
                                             labels=['ce_endpoint'])

        jobs = self.db.getGroupedJobs('cluster, arcstate')

        for job in jobs:
            count, cluster, state = (job['count(*)'], job['cluster'], job['state'])
            if state == 'submitted':
                queued_arc_jobs.add_metric([cluster], count)
            if state == 'running':
                running_arc_jobs.add_metric([cluster], count)

        yield queued_arc_jobs
        yield running_arc_jobs


class aCTMonitor(aCTProcess):

    def __init__(self):
        aCTProcess.__init__(self)
        self.prometheus_port = int(self.conf.get(['monitor', 'prometheusport']) or 0)

        if self.prometheus_port:
            start_http_server(self.prometheus_port)
            REGISTRY.register(aCTPrometheusCollector(self.db))
        else:
            self.log.info('Prometheus monitoring not enabled')

    def process(self):
        pass


if __name__ == '__main__':
    am = aCTMonitor()
    am.run()
    am.finish()
