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
                                                       downloadfiles='gmlog/errors;stdout;rucio.metadata',
                                                       appjobid=str(job['id']))

            desc = {'ldmxstatus': 'submitted', 'arcjobid': arcid['LAST_INSERT_ID()']}
            self.dbldmx.updateJobLazy(job['id'], desc)

            # Dump job description
            logdir = os.path.join(self.conf.get(["joblog", "dir"]),
                                  job['created'].strftime('%Y-%m-%d'))
            os.makedirs(logdir, 0o755, exist_ok=True)
            xrslfile = os.path.join(logdir, f'{job["id"]}.xrsl')
            with open(xrslfile, 'w') as f:
                f.write(xrsl)
                self.log.debug(f'Wrote description to {xrslfile}')

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
            xrsl['memory'] = f"(memory = {float(config.get('JobMemory', 2)) * 1000})"
            xrsl['walltime'] = f"(walltime = {int(config.get('JobWallTime', 240))})"
            xrsl['cputime'] = f"(cputime = {int(config.get('JobWallTime', 240))})"
            # LDMX RTE must be before SIMPROD one
            xrsl['runtimeenvironment'] = ''
            if 'RunTimeEnvironment' in config:
                xrsl['runtimeenvironment'] = f"(runtimeenvironment = APPS/{config.get('RunTimeEnvironment')})"
            xrsl['runtimeenvironment'] += "(runtimeenvironment = APPS/LDMX-SIMPROD-1.0)"

        wrapper = self.conf.get(['executable', 'wrapper'])
        xrsl['executable'] = f"(executable = {os.path.basename(wrapper)})"
        xrsl['inputfiles'] = f'(inputfiles = ({os.path.basename(wrapper)} {wrapper}) \
                                             (ldmxjob.config {descriptionfile}) \
                                             (ldmxsim.mac {templatefile}) \
                                             (ldmxsim.mac.template {templatefile}) \
                                             (ldmx-simprod-rte-helper.py {self.conf.get(["executable", "ruciohelper"])}))'
        xrsl['stdout'] = '(stdout = stdout)'
        xrsl['gmlog'] = '(gmlog = gmlog)'
        xrsl['join'] = '(join = yes)'
        xrsl['rerun'] = '(rerun = 2)'
        xrsl['count'] = '(count = 1)'
        xrsl['outputfiles'] = '(outputfiles = ("rucio.metadata" ""))'
        xrsl['jobName'] = '(jobname = "LDMX Prod Simulation")'

        return '&' + '\n'.join(xrsl.values())


    def process(self):

        self.processNewJobs()
        self.processWaitingJobs()


if __name__ == '__main__':

    ar = aCTLDMX2Arc()
    ar.run()
    ar.finish()
