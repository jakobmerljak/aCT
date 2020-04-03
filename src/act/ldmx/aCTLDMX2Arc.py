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

        # Thottle submission
        maxsubmitted = self.conf.get(['jobs', 'maxsubmitted']) or 999999
        nsubmitted = self.dbldmx.getNJobs("ldmxstatus in ('submitted', 'queueing', 'running')")
        if nsubmitted >= int(maxsubmitted):
            self.log.info(f'{nsubmitted} jobs already submitted, not submitting more')
            return

        # Get CE endpoints, filter by status and maxjobs
        onlinesites = [s for s, i in self.sites.items() if i['status'] == 'online' and i['maxjobs'] > 0]
        sitejobs = {s: self.dbldmx.getNJobs(f"ldmxstatus in ('submitted', 'queueing', 'running') AND sitename='{s}'") for s in onlinesites}
        self.log.debug(', '.join([f'{s}: submitted jobs {n}, maxjobs {self.sites[s]["maxjobs"]}' for (s, n) in sitejobs.items()]))
        onlinesites = [s for s in onlinesites if sitejobs[s] < self.sites[s]['maxjobs']]
        onlineendpoints = [ep for ep, s in self.endpoints.items() if s in onlinesites]
        self.log.debug(f'Available CEs: {", ".join([f"{self.endpoints[ce]}: {ce}" for ce in onlineendpoints])}')

        if not onlineendpoints:
            return

        # Submit waiting jobs
        waitingjobs = self.dbldmx.getJobs("ldmxstatus='waiting' limit 10")
        for job in waitingjobs:

            xrsl = self.createXRSL(job['description'])
            arcid = self.dbarc.insertArcJobDescription(xrsl,
                                                       proxyid=job['proxyid'],
                                                       clusterlist=','.join(onlineendpoints),
                                                       downloadfiles='stdout;gmlog/errors;rucio.metadata',
                                                       appjobid=str(job['id']))

            desc = {'ldmxstatus': 'submitted', 'arcjobid': arcid['LAST_INSERT_ID()']}
            self.dbldmx.updateJobLazy(job['id'], desc)
            self.log.info(f'Inserted job {job["id"]} with xrsl {xrsl}')

            if nsubmitted >= int(maxsubmitted):
                self.log.info(f'Reached maximum {maxsubmitted} submitted jobs')
                break

        if waitingjobs:
            self.dbldmx.Commit()


    def createXRSL(self, descriptionfile):
        xrsl = {}
        wrapper = self.conf.get(['executable', 'wrapper'])
        xrsl['executable'] = os.path.basename(wrapper)
        xrsl['inputfiles'] = f'({os.path.basename(wrapper)} {wrapper})'
        xrsl['stdout'] = 'stdout'
        xrsl['gmlog'] = 'gmlog'
        xrsl['join'] = 'yes'
        xrsl['rerun'] = '2'
        xrsl['walltime'] = 30
        xrsl['cputime'] = 30
        xrsl['outputfiles'] = '("rucio.metadata" "")'

        return '&' + '\n'.join(f'({k} = {v})' for k,v in xrsl.items())


    def process(self):

        self.processNewJobs()
        self.processWaitingJobs()


if __name__ == '__main__':

    ar = aCTLDMX2Arc()
    ar.run()
    ar.finish()
