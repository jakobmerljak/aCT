# aCTStatus.py
#
# Process to check the status of running ARC jobs
#
import time
import arc

from aCTProcess import aCTProcess

class aCTStatus(aCTProcess):
    '''
    Class for checking the status of submitted ARC jobs and updating their
    status in the DB.
    '''
    
    def __init__(self):
        
        aCTProcess.__init__(self) 

        # store the last checkJobs time to avoid overloading of GIIS
        self.checktime=time.time()


    def resetJobs(self, jobstoreset):
        '''
        Empty all StringLists in jobs so that when they are updated they do not
        contain duplicate values, since ARC always appends to these lists.
        '''
        emptylist = arc.StringList()
        j = arc.Job()
        attrstoreset = [attr for attr in dir(j) if type(getattr(j, attr)) == arc.StringList]
             
        for jobs in jobstoreset.values():   
            for job in jobs:
                for attr in attrstoreset:
                    setattr(jobs[job], attr, emptylist)
        
        
    def processJobErrors(self, id, failedjob):
        '''
        Examine errors of failed job and decide whether to resubmit or not
        '''
        self.log.info("Job failure for %s: %s", failedjob.JobID, ";".join([joberr for joberr in failedjob.Error]))
        
        # First check if it was a data staging problem
        if failedjob.RestartState == arc.JobState.PREPARING or \
           failedjob.RestartState == arc.JobState.FINISHING:
            self.log.info("Will rerun job %s", failedjob.JobID)
            # Reset arc job state so that next time new state will be picked up
            failedjob.State = arc.JobState('Undefined')
            return "torerun"
        
        newstate = "failed"
        # Check if any job runtime error matches any error in the toresubmit list
        resub = [err for err in self.conf.getList(['errors','toresubmit','arcerrors','item']) if ";".join([joberr for joberr in failedjob.Error]).find(err) != -1]
        attemptsleft = int(self.db.getArcJobInfo(id, ['attemptsleft'])['attemptsleft']) - 1
        if attemptsleft < 0:
            attemptsleft = 0
        self.db.updateArcJob(id, {'attemptsleft': str(attemptsleft)})
        if resub:
            if not attemptsleft:
                self.log.info("Job %s out of retries", failedjob.JobID)
            else:
                self.log.info("Will resubmit job %s, %i attempts left", failedjob.JobID, attemptsleft)
                failedjob.State = arc.JobState('Undefined')
                newstate = "toresubmit"
        
        else:
            self.log.info("Job %s has fatal errors, cannot resubmit", failedjob.JobID)
        return newstate
        
    def checkJobs(self):
        '''
        Query all running jobs
        '''
        
        # minimum time between checks
        if time.time()<self.checktime+int(self.conf.get(['jobs','checkmintime'])):
            self.log.debug("mininterval not reached")
            return
        self.checktime=time.time()

        # check jobs which were last checked more than checkinterval ago
        jobstocheck=self.db.getArcJobs("(arcstate='submitted' or arcstate='running' or arcstate='cancelling') and " \
                                       "cluster='"+self.cluster+"' and "+ \
                                       self.db.timeStampLessThan("tarcstate", self.conf.get(['jobs','checkinterval'])) + \
                                       " limit 100000")

        # total number of jobs
        njobs=self.db.getNArcJobs()

        # Do not check too little jobs at once (at least 1% of running jobs)
        njobstocheck = sum(len(v) for v in jobstocheck.itervalues())
        if njobstocheck < njobs/1000:
            #self.log.debug("too few to check %d" % len(jobs))
            return
        if njobstocheck:
            self.log.info("%d jobs to check" % njobstocheck)
        else:
            return
        
        self.resetJobs(jobstocheck)
        
        # Loop over proxies
        for proxyid, jobs in jobstocheck.items():
            self.uc.CredentialString(self.db.getProxy(proxyid))
    
            job_supervisor = arc.JobSupervisor(self.uc, jobs.values())
            job_supervisor.Update()
            jobsupdated = job_supervisor.GetAllJobs()
            jobsnotupdated = job_supervisor.GetIDsNotProcessed()
            
            for (id, originaljob, updatedjob) in zip(jobs.keys(), jobs.values(), jobsupdated):
                if updatedjob.JobID in jobsnotupdated:
                    self.log.error("Failed to find information on %s", updatedjob.JobID)
                    continue
                if updatedjob.JobID != originaljob.JobID:
                    # something went wrong with list order
                    self.log.warn("Bad job id (%s), expected %s", updatedjob.JobID, originaljob.JobID)
                    continue
                # compare strings here to get around limitations of JobState API
                if originaljob.State.GetGeneralState() == updatedjob.State.GetGeneralState():
                    # just update timestamp
                    self.db.updateArcJob(id, {'tarcstate': self.db.getTimeStamp()})
                    continue
                
                self.log.debug("Job %s: %s -> %s", originaljob.JobID, originaljob.State.GetGeneralState(), updatedjob.State.GetGeneralState())
                
                # state changed, update whole Job object
                arcstate = 'submitted'
                if updatedjob.State == arc.JobState.FINISHED:
                    if updatedjob.ExitCode == -1:
                        # Missing exit code, but assume success
                        self.log.warn("Job %s FINISHED but has missing exit code, setting to zero" % updatedjob.JobID)
                        updatedjob.ExitCode = 0
                    arcstate = 'finished'
                elif updatedjob.State == arc.JobState.FAILED:
                    arcstate = self.processJobErrors(id, updatedjob)
                elif updatedjob.State == arc.JobState.KILLED:
                    arcstate = 'cancelled'
                elif updatedjob.State == arc.JobState.RUNNING or \
                     updatedjob.State == arc.JobState.FINISHING:
                    arcstate = 'running'
                elif updatedjob.State == arc.JobState.DELETED or \
                     updatedjob.State == arc.JobState.OTHER:
                    # unexpected
                    arcstate = 'failed'
                    
                self.db.updateArcJob(id, {'arcstate': arcstate, 'tarcstate': self.db.getTimeStamp()}, updatedjob)
                    
        self.log.info('Done')
        
    def checkLostJobs(self):
        '''
        Move jobs with a long time since status update to lost
        '''

        # 2 days limit. TODO: configurable?
        jobs=self.db.getArcJobsInfo("(arcstate='submitted' or arcstate='running' or arcstate='cancelling' or arcstate='finished') and " \
                                    "cluster='"+self.cluster+"' and "+self.db.timeStampLessThan("tarcstate", 172800),
                                    ['id', 'JobID'])
        
        for job in jobs:
            self.log.warn("Job %s lost from information system, marking as lost", job['JobID'])
            self.db.updateArcJob(job['id'], {'arcstate': 'lost', 'tarcstate': self.db.getTimeStamp()})
            
    
    def process(self):
        # check job status
        self.checkJobs()
        # check for lost jobs
        self.checkLostJobs()

if __name__ == '__main__':
    st=aCTStatus()
    st.run()
    st.finish()
