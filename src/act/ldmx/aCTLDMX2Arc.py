import os

from act.ldmx.aCTLDMXProcess import aCTLDMXProcess

class aCTLDMX2Arc(aCTLDMXProcess):
    '''
    Pick up new jobs in the db and create ARC jobs
    '''

    def __init__(self):
        aCTLDMXProcess.__init__(self)

    def processNewJobs(self):
        '''
        Get new jobs in the DB
        '''
        # Set all new jobs to waiting
        newjobs = self.dbldmx.getJobs("ldmxstatus='new'")
        for job in newjobs:
            self.dbldmx.updateJobLazy(job['id'], {'ldmxstatus': 'waiting'})
        if newjobs:
            self.dbldmx.Commit()

    def processWaitingJobs(self):

        # Thottle submission to not have too big a queue of unsubmitted jobs
        maxsubmitted = self.conf.get(['jobs', 'maxsubmitted']) or 50
        nsubmitted = self.dbldmx.getNJobs("ldmxstatus='submitted'")
        if nsubmitted >= int(maxsubmitted):
            self.log.info(f'{nsubmitted} jobs already submitted, not submitting more')
            return

        # Get CE endpoints, filter by status and maxjobs
        onlinesites = [s for s, i in self.sites.items() if i['status'] == 'online' and i['maxjobs'] > 0]
        sitejobs = {s: self.dbldmx.getNJobs(f"ldmxstatus in ('submitted', 'queueing', 'running') AND sitename='{s}'") for s in onlinesites}
        if sitejobs:
            self.log.debug(', '.join([f'{s}: submitted jobs {n}, maxjobs {self.sites[s]["maxjobs"]}' for (s, n) in sitejobs.items()]))
        onlinesites = [s for s in onlinesites if sitejobs[s] < self.sites[s]['maxjobs']]
        onlineendpoints = [ep for ep, s in self.endpoints.items() if s in onlinesites]

        if not onlineendpoints:
            self.log.debug('No available CEs')
            return

        self.log.debug(f'Available CEs: {", ".join([f"{self.endpoints[ce]}: {ce}" for ce in onlineendpoints])}')

        # Submit waiting jobs
        waitingjobs = self.dbldmx.getJobs("ldmxstatus='waiting' order by modified limit 10")
        for job in waitingjobs:

            xrsl = self.createXRSL(job['description'], job['template'])
            if not xrsl:
                self.log.warning(f'Could not create xrsl for {job["id"]}')
                # Set back to new to put at the back of the queue
                self.dbldmx.updateJobLazy(job['id'], {'ldmxstatus': 'new'})
                continue

            self.log.info(f'Inserting job {job["id"]} with xrsl {xrsl}')
            arcid = self.dbarc.insertArcJobDescription(xrsl,
                                                       proxyid=job['proxyid'],
                                                       clusterlist=','.join(onlineendpoints),
                                                       downloadfiles='stdout;gmlog/errors;rucio.metadata',
                                                       appjobid=str(job['id']))

            desc = {'ldmxstatus': 'submitted', 'arcjobid': arcid['LAST_INSERT_ID()']}
            self.dbldmx.updateJobLazy(job['id'], desc)

            nsubmitted += 1
            if nsubmitted >= int(maxsubmitted):
                self.log.info(f'Reached maximum {maxsubmitted} submitted jobs')
                break

        if waitingjobs:
            self.dbldmx.Commit()


    def createXRSL(self, descriptionfile, templatefile):

        xrsl = {}

        # Parse some requirements from descriptionfile
        with open(descriptionfile) as f:
            config = {l.split('=')[0]: l.split('=')[1].strip() for l in f}
            xrsl['memory'] = int(config.get('JobMemory', 2)) * 1000
            xrsl['walltime'] = int(config.get('JobWallTime', 240))
            xrsl['cputime'] = xrsl['walltime']
            if 'RunTimeEnvironment' in config: # TODO multiple RTE!
                xrsl['runtimeenvironment'] = f"APPS/{config.get('RunTimeEnvironment')}"

        wrapper = self.conf.get(['executable', 'wrapper'])
        xrsl['executable'] = os.path.basename(wrapper)
        xrsl['inputfiles'] = f'({os.path.basename(wrapper)} {wrapper}) \
                               (ldmxjob.config {descriptionfile}) \
                               (ldmx.mac.template {templatefile}) \
                               (ldmx-prod-helper.py {self.conf.get(["executable", "ruciohelper"])})'
        xrsl['stdout'] = 'stdout'
        xrsl['gmlog'] = 'gmlog'
        xrsl['join'] = 'yes'
        xrsl['rerun'] = '2'
        xrsl['outputfiles'] = '("rucio.metadata" "")'
        xrsl['jobName'] = '"LDMX Prod Simulation"'

        return '&' + '\n'.join(f'({k} = {v})' for k,v in xrsl.items())


    def process(self):

        self.processNewJobs()
        self.processWaitingJobs()


if __name__ == '__main__':

    ar = aCTLDMX2Arc()
    ar.run()
    ar.finish()
