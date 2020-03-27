import os
import shutil

from act.ldmx.aCTLDMXProcess import aCTLDMXProcess

class aCTLDMXStatus(aCTLDMXProcess):
    '''
    Check the status of submitted and running jobs, handle resubmission or
    cleanup of failed jobs.
    '''

    def __init__(self):
        aCTLDMXProcess.__init__(self)


    def checkSubmittedJobs(self):
        '''
        Look for newly submitted or running jobs
        '''

        select = "ldmxstatus='submitted' and arcstate in ('submitted', 'running')"
        columns = ['arcstate', 'cluster', 'ldmxjobs.id']
        submittedjobs = self.dbarc.getArcJobsInfo(select, columns, tables='arcjobs,ldmxjobs')

        for job in submittedjobs:
            self.log.info(f"Job {job['id']} now in state {job['arcstate']}")
            desc = {'ldmxstatus': 'queueing' if job['arcstate'] == 'submitted' else 'running',
                    'computingelement': job['cluster']}
            self.dbldmx.updateJobLazy(job['id'], desc)

        select = "ldmxstatus='queueing' and arcstate = 'running'"
        queueingjobs = self.dbarc.getArcJobsInfo(select, columns, tables='arcjobs,ldmxjobs')

        for job in queueingjobs:
            self.log.info(f"Job {job['id']} now in state {job['arcstate']}")
            desc = {'ldmxstatus': 'running',
                    'computingelement': job['cluster']}
            self.dbldmx.updateJobLazy(job['id'], desc)

        if submittedjobs or queueingjobs:
            self.dbldmx.Commit()


    def checkFailedJobs(self):
        '''
        Look for failed jobs and set them to clean
        TODO: whether to try and keep logs
        '''
        # Get outputs to download for failed jobs
        select = "arcstate='failed'"
        columns = ['id']
        arcjobs = self.dbarc.getArcJobsInfo(select, columns)
        if arcjobs:
            for aj in arcjobs:
                select = "id='{}'".format(str(aj["id"]))
                desc = {"arcstate": "tofetch", "tarcstate": self.dbarc.getTimeStamp()}
                self.dbarc.updateArcJobsLazy(desc, select)
            self.dbarc.Commit()

        # Look for failed final states in ARC which are still starting or running in LDMX
        select = "arcstate in ('donefailed', 'cancelled', 'lost')"
        columns = ['arcstate', 'id', 'cluster', 'JobID', 'created', 'stdout']

        jobstoupdate = self.dbarc.getArcJobsInfo(select, columns=columns)

        if not jobstoupdate:
            return

        failedjobs = [job for job in jobstoupdate if job['arcstate'] == 'donefailed']
        if failedjobs:
            self.log.debug(f"Found {len(failedjobs)} failed jobs ({','.join([str(j['id']) for j in failedjobs])})")
        lostjobs = [job for job in jobstoupdate if job['arcstate'] == 'lost']
        if lostjobs:
            self.log.debug(f"Found {len(lostjobs)} lost jobs ({','.join([str(j['id']) for j in lostjobs])})")
        cancelledjobs = [job for job in jobstoupdate if job['arcstate'] == 'cancelled']
        if cancelledjobs:
            self.log.debug(f"Found {len(cancelledjobs)} cancelled jobs ({','.join([str(j['id']) for j in cancelledjobs])})")

        desc = {"arcstate": "toclean", "tarcstate": self.dbarc.getTimeStamp()}
        for aj in failedjobs:
            self.copyOutputFiles(aj)
            select = f"id={aj['id']}"
            self.dbarc.updateArcJobsLazy(desc, select)
            self.dbldmx.updateJobsLazy(f"arcjobid={aj['id']}", {'ldmxstatus': 'failed',
                                                                'computingelement': aj['cluster']})

        for aj in lostjobs:
            select = f"id={aj['id']}"
            self.dbarc.updateArcJobsLazy(desc, select)
            self.dbldmx.updateJobsLazy(f"arcjobid={aj['id']}", {'ldmxstatus': 'failed',
                                                                'computingelement': aj['cluster']})

        for aj in cancelledjobs:
            select = f"id={aj['id']}"
            self.dbarc.updateArcJobsLazy(desc, select)
            self.dbldmx.updateJobsLazy(f"arcjobid={aj['id']}", {'ldmxstatus': 'cancelled',
                                                                'computingelement': aj['cluster']})

        self.dbarc.Commit()
        self.dbldmx.Commit()


    def copyOutputFiles(self, arcjob):
        '''
        Copy job stdout and errors log to final location
        '''
        sessionid = arcjob['JobID'][arcjob['JobID'].rfind('/')+1:]
        date = arcjob['created'].strftime('%Y-%m-%d')
        outd = os.path.join(self.conf.get(['joblog','dir']), date)
        os.makedirs(outd, 0o755, exist_ok=True)

        localdir = os.path.join(self.tmpdir, sessionid)
        gmlogerrors = os.path.join(localdir, "gmlog", "errors")
        arcjoblog = os.path.join(outd, "%s.log" % arcjob['id'])
        try:
            shutil.move(gmlogerrors, arcjoblog)
            os.chmod(arcjoblog, 0o644)
        except Exception as e:
            self.log.error(f'Failed to copy {gmlogerrors}: {e}')

        jobstdout = arcjob['stdout']
        if jobstdout:
            try:
                shutil.move(os.path.join(localdir, jobstdout),
                            os.path.join(outd, '%s.out' % arcjob['id']))
                os.chmod(os.path.join(outd, '%s.out' % arcjob['id']), 0o644)
            except Exception as e:
                self.log.error(f'Failed to copy file {os.path.join(localdir, jobstdout)}, {str(e)}')

    def process(self):

        self.checkSubmittedJobs()
        self.checkFailedJobs()


if __name__ == '__main__':

    ar = aCTLDMXStatus()
    ar.run()
    ar.finish()
