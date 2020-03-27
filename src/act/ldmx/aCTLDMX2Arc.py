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

        # Get CE endpoints
        sites = self.conf.getListCond(['sites' ,'site'], 'status=online', ['endpoints', 'item'])
        self.log.debug(f'Using sites {",".join(sites)}')

        # Submit waiting jobs
        waitingjobs = self.dbldmx.getJobs("ldmxstatus='waiting'")
        for job in waitingjobs:

            xrsl = self.createXRSL(job['description'])
            arcid = self.dbarc.insertArcJobDescription(xrsl,
                                                       proxyid=job['proxyid'],
                                                       clusterlist=','.join(sites),
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
        xrsl['outputfiles'] = '("rucio.metadata" "")'

        return '&' + '\n'.join(f'({k} = {v})' for k,v in xrsl.items())


    def process(self):

        self.processNewJobs()
        self.processWaitingJobs()


if __name__ == '__main__':

    ar = aCTLDMX2Arc()
    ar.run()
    ar.finish()
