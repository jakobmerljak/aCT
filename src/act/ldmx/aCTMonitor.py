from prometheus_client.core import GaugeMetricFamily
from act.ldmx.aCTDBLDMX import aCTDBLDMX

def collect(log):

    # Get current jobs
    db = aCTDBLDMX(log)
    rows = db.getGroupedJobs('ldmxstatus')

    for row in rows:
        if row['ldmxstatus'] == 'waiting':
            log.debug(f"{row['count(*)']} waiting jobs")
            yield GaugeMetricFamily('ldmx_waiting_jobs',
                                    'Waiting jobs in LDMX',
                                    value=row['count(*)'])

        if row['ldmxstatus'] == 'queueing':
            log.debug(f"{row['count(*)']} queueing jobs")
            yield GaugeMetricFamily('ldmx_queued_jobs',
                                    'Queued jobs in LDMX',
                                    value=row['count(*)'])

        if row['ldmxstatus'] == 'running':
            log.debug(f"{row['count(*)']} running jobs")
            yield GaugeMetricFamily('ldmx_running_jobs',
                                    'Running jobs in LDMX',
                                    value=row['count(*)'])

        if row['ldmxstatus'] == 'finished':
            log.debug(f"{row['count(*)']} finished jobs")
            yield GaugeMetricFamily('ldmx_finished_jobs',
                                    'Finished jobs in LDMX',
                                    value=row['count(*)'])

        if row['ldmxstatus'] == 'failed':
            log.debug(f"{row['count(*)']} failed jobs")
            yield GaugeMetricFamily('ldmx_failed_jobs',
                                    'Failed jobs in LDMX',
                                    value=row['count(*)'])

    raise StopIteration
