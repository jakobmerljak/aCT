from prometheus_client.core import GaugeMetricFamily
from act.ldmx.aCTDBLDMX import aCTDBLDMX

def collect(log):

    waiting = GaugeMetricFamily('ldmx_waiting_jobs',
                                'Waiting jobs in LDMX',
                                labels=['batchid'])

    queued = GaugeMetricFamily('ldmx_queued_jobs',
                               'Queued jobs in LDMX',
                               labels=['batchid'])

    running = GaugeMetricFamily('ldmx_running_jobs',
                                'Running jobs in LDMX',
                                labels=['batchid'])

    finished = GaugeMetricFamily('ldmx_finished_jobs',
                                 'Finished jobs in LDMX',
                                 labels=['batchid'])

    failed = GaugeMetricFamily('ldmx_failed_jobs',
                               'Failed jobs in LDMX',
                               labels=['batchid'])
    # Get current jobs
    db = aCTDBLDMX(log)
    rows = db.getGroupedJobs('ldmxstatus, batchid')

    for row in rows:
        if row['ldmxstatus'] == 'waiting':
            log.debug(f"{row['count(*)']} waiting jobs for batch {row['batchid']}")
            waiting.add_metric([row['batchid']], row['count(*)'])

        if row['ldmxstatus'] == 'queueing':
            log.debug(f"{row['count(*)']} queued jobs for batch {row['batchid']}")
            queued.add_metric([row['batchid']], row['count(*)'])

        if row['ldmxstatus'] == 'running':
            log.debug(f"{row['count(*)']} running jobs for batch {row['batchid']}")
            running.add_metric([row['batchid']], row['count(*)'])

        if row['ldmxstatus'] == 'finished':
            log.debug(f"{row['count(*)']} finished jobs for batch {row['batchid']}")
            finished.add_metric([row['batchid']], row['count(*)'])

        if row['ldmxstatus'] == 'failed':
            log.debug(f"{row['count(*)']} failed jobs for batch {row['batchid']}")
            failed.add_metric([row['batchid']], row['count(*)'])

    yield waiting
    yield queued
    yield running
    yield finished
    yield failed
