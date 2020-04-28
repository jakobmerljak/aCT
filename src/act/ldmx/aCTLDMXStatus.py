import os
import shutil

from act.ldmx.aCTLDMXProcess import aCTLDMXProcess

class aCTLDMXStatus(aCTLDMXProcess):
    '''
    Check the status of submitted and running jobs, handle resubmission or
    cleanup of failed and cancelled jobs.
    '''

    def __init__(self):
        aCTLDMXProcess.__init__(self)


    def checkSubmittedJobs(self):
        '''
        Look for newly submitted or running jobs
        '''

        select = "ldmxstatus='waiting' and arcstate in ('submitted', 'running') and arcjobs.id=ldmxjobs.arcjobid"
        columns = ['arcstate', 'cluster', 'ldmxjobs.id']
        submittedjobs = self.dbarc.getArcJobsInfo(select, columns, tables='arcjobs,ldmxjobs')

        for job in submittedjobs:
            self.log.info(f"Job {job['id']} now in state {job['arcstate']}")
            desc = {'ldmxstatus': 'queueing' if job['arcstate'] == 'submitted' else 'running',
                    'computingelement': job['cluster'],
                    'sitename': self.endpoints[job['cluster']]}
            self.dbldmx.updateJobLazy(job['id'], desc)

        select = "ldmxstatus='queueing' and arcstate = 'running' and arcjobs.id=ldmxjobs.arcjobid"
        queueingjobs = self.dbarc.getArcJobsInfo(select, columns, tables='arcjobs,ldmxjobs')

        for job in queueingjobs:
            self.log.info(f"Job {job['id']} now in state {job['arcstate']}")
            desc = {'ldmxstatus': 'running',
                    'computingelement': job['cluster'],
                    'sitename': self.endpoints[job['cluster']]}
            self.dbldmx.updateJobLazy(job['id'], desc)

        if submittedjobs or queueingjobs:
            self.dbldmx.Commit()


    def checkToCancelJobs(self):
        '''
        Look for jobs marked to cancel and cancel the arc jobs
        '''
        select = "ldmxstatus='tocancel'"
        columns = ['id', 'arcjobid', 'description', 'template']
        cancelledjobs = self.dbldmx.getJobs(select, columns)

        if not cancelledjobs:
            return

        for job in cancelledjobs:
            self.log.info(f"Job {job['id']} requested to cancel, killing arc job")

            # Check if there is an arc job
            columns = ['id']
            arcjob = self.dbarc.getArcJobInfo(job['arcjobid'], columns)
            if arcjob:
                self.log.info(f"Cancelling arc job {arcjob['id']}")
                select = "id='{}'".format(job['arcjobid'])
                desc = {"arcstate": "tocancel", "tarcstate": self.dbarc.getTimeStamp()}
                self.dbarc.updateArcJobs(desc, select)
                self.dbldmx.updateJobLazy(job['id'], {'ldmxstatus': 'cancelling'})
            else:
                self.log.info(f"Job {job['id']} has no arc job, marking cancelled")
                self.dbldmx.updateJobLazy(job['id'], {'ldmxstatus': 'cancelled'})

            self.cleanInputFiles(job)

        self.dbldmx.Commit()


    def checkToResubmitJobs(self):
        '''
        Look for jobs marked to resubmit, cancel the arc jobs and set to waiting
        '''
        select = "ldmxstatus='toresubmit'"
        columns = ['id', 'arcjobid']
        toresubmitjobs = self.dbldmx.getJobs(select, columns)

        if not toresubmitjobs:
            return

        for job in toresubmitjobs:
            self.log.info(f"Job {job['id']} requested to resubmit, killing arc job")

            # Check if there is an arc job
            columns = ['id']
            arcjob = self.dbarc.getArcJobInfo(job['arcjobid'], columns)
            if arcjob:
                self.log.info(f"Cancelling arc job {arcjob['id']}")
                select = "id='{}'".format(job['arcjobid'])
                desc = {"arcstate": "tocancel", "tarcstate": self.dbarc.getTimeStamp()}
                self.dbarc.updateArcJobs(desc, select)
            else:
                self.log.info(f"Job {job['id']} has no arc job")

            ldmxdesc = {'ldmxstatus': 'waiting', 'arcjobid': None,
                        'sitename': None, 'computingelement': None}
            self.dbldmx.updateJobLazy(job['id'], ldmxdesc)

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
        select = "arcstate in ('donefailed', 'cancelled', 'lost') and arcjobs.id=ldmxjobs.arcjobid"
        columns = ['arcstate', 'arcjobs.id', 'cluster', 'JobID', 'ldmxjobs.created', 'stdout',
                   'description', 'template', 'sitename']

        jobstoupdate = self.dbarc.getArcJobsInfo(select, columns=columns, tables='arcjobs,ldmxjobs')

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
                                                                'computingelement': aj.get('cluster'),
                                                                'sitename': self.endpoints.get(aj.get('cluster'))})
            self.cleanInputFiles(aj)

        for aj in lostjobs:
            select = f"id={aj['id']}"
            self.dbarc.updateArcJobsLazy(desc, select)
            self.dbldmx.updateJobsLazy(f"arcjobid={aj['id']}", {'ldmxstatus': 'failed',
                                                                'computingelement': aj.get('cluster'),
                                                                'sitename': self.endpoints.get(aj.get('cluster'))})
            self.cleanInputFiles(aj)

        for aj in cancelledjobs:
            select = f"id={aj['id']}"
            self.dbarc.updateArcJobsLazy(desc, select)
            self.dbldmx.updateJobsLazy(f"arcjobid={aj['id']}", {'ldmxstatus': 'cancelled',
                                                                'computingelement': aj.get('cluster'),
                                                                'sitename': self.endpoints.get(aj.get('cluster'))})
            self.cleanInputFiles(aj)

        self.dbarc.Commit()
        self.dbldmx.Commit()


    def copyOutputFiles(self, arcjob):
        '''
        Copy job stdout and errors log to final location and make a copy
        in the failed folder
        '''

        if not arcjob.get('JobID'):
            self.log.info('Job did not run, no output to copy')
            return

        sessionid = arcjob['JobID'][arcjob['JobID'].rfind('/')+1:]
        date = arcjob['created'].strftime('%Y-%m-%d')
        localdir = os.path.join(self.tmpdir, sessionid)
        gmlogerrors = os.path.join(localdir, "gmlog", "errors")
        jobstdout = arcjob['stdout']

        outdir = os.path.join(self.conf.get(['joblog','dir']), date)
        outdfailed = os.path.join(outdir, 'failed', arcjob['sitename'] or 'None')
        os.makedirs(outdir, 0o755, exist_ok=True)
        os.makedirs(outdfailed, 0o755, exist_ok=True)

        try:
            arcjoblog = os.path.join(outdir, "%s.log" % arcjob['id'])
            shutil.copy(gmlogerrors, arcjoblog)
            os.chmod(arcjoblog, 0o644)
            arcjoblog = os.path.join(outdfailed, "%s.log" % arcjob['id'])
            shutil.move(gmlogerrors, arcjoblog)
            os.chmod(arcjoblog, 0o644)
        except Exception as e:
            self.log.error(f'Failed to copy {gmlogerrors}: {e}')

        if jobstdout:
            try:
                shutil.copy(os.path.join(localdir, jobstdout),
                            os.path.join(outdir, '%s.out' % arcjob['id']))
                os.chmod(os.path.join(outdir, '%s.out' % arcjob['id']), 0o644)
                shutil.move(os.path.join(localdir, jobstdout),
                            os.path.join(outdfailed, '%s.out' % arcjob['id']))
                os.chmod(os.path.join(outdfailed, '%s.out' % arcjob['id']), 0o644)
            except Exception as e:
                self.log.error(f'Failed to copy file {os.path.join(localdir, jobstdout)}, {str(e)}')


    def cleanInputFiles(self, job):
        '''
        Clean job input files in tmp dir
        '''
        try:
            os.remove(job['description'])
            os.remove(job['template'])
            self.log.debug(f'Removed {job["description"]} and {job["template"]}')
        except:
            pass


    def process(self):

        self.checkToCancelJobs()
        self.checkToResubmitJobs()
        self.checkSubmittedJobs()
        self.checkFailedJobs()


if __name__ == '__main__':

    ar = aCTLDMXStatus()
    ar.run()
    ar.finish()
