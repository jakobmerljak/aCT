import sys
import time
import arc
from urlparse import urlparse
from threading import Thread
from aCTProcess import aCTProcess

class SubmitThr(Thread):
    def __init__ (self,func,id,jobdescs,uc,logger):
        Thread.__init__(self)
        self.func=func
        self.id=id
        self.jobdescs = jobdescs
        self.uc = uc
        self.log=logger
        self.job = None
    def run(self):
        self.job=self.func(self.jobdescs,self.uc,self.log)


def Submit(jobdescs,uc,log):

    global queuelist

    if len(queuelist) == 0  :
        log.error("no cluster free for submission")
        return None
    
    # Do brokering among the available queues
    jobdesc = jobdescs[0]
    broker = arc.Broker(uc, jobdesc, "Random")
    targetsorter = arc.ExecutionTargetSorter(broker)
    for target in queuelist:
        log.debug("considering target %s:%s" % (target.ComputingService.Name, target.ComputingShare.Name))

        # Adding an entity performs matchmaking and brokering
        targetsorter.addEntity(target)
    
    if len(targetsorter.getMatchingTargets()) == 0:
        log.error("no clusters satisfied job description requirements")
        return None
        
    targetsorter.reset() # required to reset iterator, otherwise we get a seg fault
    selectedtarget = targetsorter.getCurrentTarget()
    # Job object will contain the submitted job
    job = arc.Job()
    submitter = arc.Submitter(uc)
    if submitter.Submit(selectedtarget, jobdesc, job) != arc.SubmissionStatus.NONE:
        log.error("Submission failed")
        return None

    return job

class aCTSubmitter(aCTProcess):

    def RunThreadsSplit(self,plist,nthreads=1):
        it=0
        while it < len(plist):
            tl=[]
            for i in range(0,nthreads):
                try:
                    t=plist[it]
                    tl.append(t)
                    t.start()
                except:
                    pass
                it+=1
            errfl=False
            for t in tl:
                t.join(60.0)
                if t.isAlive() :
                    # abort due to timeout and try again
                    self.log.error("submission timeout: exit and try again")
                    errfl=True
                    continue
                # updatedb
                if t.job is None:
                    #self.log.error("no jobname")
                    self.log.error("no job defined for %d" % t.id)
                    errfl=True
                    continue
                jd={}
                jd['arcstate']='submitted'
                # initial offset to 1 minute to force first status check
                jd['tarcstate']=self.db.getTimeStamp(time.time()-int(self.conf.get(['jobs','checkinterval']))+300)
                # extract hostname of cluster (depends on JobID being a URL)
                self.log.info("job id %s", t.job.JobID)
                jd['cluster']=urlparse(t.job.JobID).hostname
                self.db.updateArcJobLazy(t.id,jd,t.job)
            if errfl:
                break


    def submit(self):
        """
        Main function to submit jobs.
        """

        global queuelist

        # check for stopsubmission flag
        if self.conf.get(['downtime','stopsubmission']) == "true":
            return 0

        if self.cluster:
            # Lock row for update in case multiple clusters are specified
            jobs=self.db.getArcJobsInfo("arcstate='tosubmit' and clusterlist like '%"+self.cluster+"%' limit 1",
                                        columns=["id", "jobdesc"], lock=True)
        else:
            jobs=self.db.getArcJobsInfo("arcstate='tosubmit' and clusterlist='' limit 1", ["id", "jobdesc"])

        # mark submitting in db
        for j in jobs:
            jd={'cluster': self.cluster, 'arcstate': 'submitting', 'tarcstate': self.db.getTimeStamp()}
            self.db.updateArcJobLazy(j['id'],jd)
        self.db.Commit()

        if len(jobs) == 0:
            #self.log.debug("No jobs to submit")
            return 0
        self.log.info("Submitting %d jobs:" % len(jobs))

        # Query infosys - either local or index
        if self.cluster:
            # Endpoint and type will come from cluster table eventually
            aris = 'ldap://'+self.cluster+'/mds-vo-name=local,o=grid'
            infoendpoints = [arc.Endpoint(aris, arc.Endpoint.COMPUTINGINFO, 'org.nordugrid.ldapng')]
                          
        else:
            giises = self.conf.getList(['atlasgiis','item'])
            infoendpoints = []
            for g in giises:
                # Specify explicitly EGIIS
                infoendpoints.append(arc.Endpoint(str(g), arc.Endpoint.REGISTRY, "org.nordugrid.ldapegiis"))

        # retriever contains a list of CE endpoints
        retriever = arc.ComputingServiceRetriever(self.uc, infoendpoints)
        retriever.wait()
        # targets is the list of queues
        # target.ComputingService.Name is the CE hostname
        # target.ComputingShare.Name is the queue name
        targets = retriever.GetExecutionTargets()
        
        # Filter only sites for this process
        queuelist=[]
        for target in targets:
            if self.cluster and target.ComputingService.Name != self.cluster:
                continue
            s = self.db.getSchedconfig(target.ComputingService.Name)
            status = 'online'
            if s is not None:
                status=s['status']
            if target.ComputingShare.Name in self.conf.getList(['queuesreject','item']):
                pass
            elif target.ComputingService.Name in self.conf.getList(['clustersreject','item']):
                pass
            elif status == "XXXoffline":
                pass
            #elif target.ComputingService.Name == "jeannedarc.hpc2n.umu.se" and target.ComputingShare.Name == "atlas-t1-repro":
            #    pass
            else:
                # tmp hack
                target.ComputingShare.LocalWaitingJobs = 0
                target.ComputingShare.PreLRMSWaitingJobs = 0
                target.ExecutionEnvironment.CPUClockSpeed = 2000
                qjobs=self.db.getArcJobsInfo("cluster='" +str(target.ComputingService.Name)+ "' and  arcstate='submitted'", ['id'])
                rjobs=self.db.getArcJobsInfo("cluster='" +str(target.ComputingService.Name)+ "' and  arcstate='running'", ['id'])
                #jlimit = max ( len(rjobs)*0.20, 50)
                #jlimit = len(rjobs)*0.15 + 30
                jlimit = len(rjobs)*0.15 + 20
                #jlimit = 30000
                target.ComputingShare.PreLRMSWaitingJobs=len(qjobs)
                #if ( len(qjobs) < 200 ) :
                if len(qjobs) < jlimit:
                    queuelist.append(target)
                    self.log.debug("Adding target %s:%s" % (queuelist[-1].ComputingService.Name, target.ComputingShare.Name))
                #if target.ComputingService.Name == "lcg-lrz-ce2.grid.lrz.de":
                #   queuelist.append(target)
                if target.ComputingService.Name == "vm009.gla.scotgrid.ac.uk":
                    target.ExecutionEnvironment.CPUClockSpeed = 3000

        tlist=[]
        # check if any queues are available, if not leave and try again next time
        if not queuelist:
            self.log.info("No free queues available")
            self.db.Commit()
            return

        self.log.info("start submitting")

        for j in jobs:
            self.log.debug("preparing: %s" % j['id'])
            jobdescstr = str(j['jobdesc'])
            jobdescs = arc.JobDescriptionList()
            if not jobdescstr or not arc.JobDescription_Parse(jobdescstr, jobdescs):
                self.log.error("Failed to prepare job description %d" % j['id'])
                continue
            t=SubmitThr(Submit,j['id'],jobdescs,self.uc,self.log)
            tlist.append(t)
            #t.start()

        #aCTUtils.RunThreadsSplit(tlist,10)
        self.RunThreadsSplit(tlist,1)
        self.log.info("threads finished")
        # commit transaction to release row locks
        self.db.Commit()

        self.log.info("end submitting")


    def checkFailedSubmissions(self):

        jobs=self.db.getArcJobsInfo("arcstate='submitting' and cluster='"+self.cluster+"'", ["id"])

        # TODO query GIIS for job name specified in description to see if job
        # was really submitted or not
        for j in jobs:
            # set to toresubmit and the application should figure out what to do
            self.db.updateArcJob(j['id'], {"arcstate": "toresubmit",
                                           "tarcstate": self.db.getTimeStamp()})

    def processToCancel(self):
        
        jobs = self.db.getArcJobs("arcstate='tocancel' and cluster='"+self.cluster+"'")
        if not jobs:
            return
        
        self.log.info("Cancelling %i jobs", len(jobs.values()))
        job_supervisor = arc.JobSupervisor(self.uc, jobs.values())
        job_supervisor.Update()
        job_supervisor.Cancel()
        
        notcancelled = job_supervisor.GetIDsNotProcessed()

        for (id, job) in jobs.items():
            if job.JobID in notcancelled:
# State comparison only works with ARC 4.0, comment out until this version is used
#                if job.State == arc.JobState.UNDEFINED:
#                    # Job has not yet reached info system
#                    self.log.warning("Job %s is not yet in info system so cannot be cancelled", job.JobID)
#                else:
                    self.log.error("Could not cancel job %s", job.JobID)
                    # Just to mark as cancelled so it can be cleaned
                    self.db.updateArcJob(id, {"arcstate": "cancelled",
                                              "tarcstate": self.db.getTimeStamp()})
            else:
                self.db.updateArcJob(id, {"arcstate": "cancelling",
                                               "tarcstate": self.db.getTimeStamp()})

    def processToResubmit(self):
        
        jobs = self.db.getArcJobs("arcstate='toresubmit' and cluster='"+self.cluster+"'")
 
        # Clean up jobs which were submitted
        jobstoclean = [job for job in jobs.values() if job.JobID]
        
        if jobstoclean:
            
            # Put all jobs to cancel, however the supervisor will only cancel
            # cancellable jobs and remove the rest so there has to be 2 calls
            # to Clean()
            job_supervisor = arc.JobSupervisor(self.uc, jobstoclean)
            job_supervisor.Update()
            self.log.info("Cancelling %i jobs", len(jobstoclean))
            job_supervisor.Cancel()
            
            processed = job_supervisor.GetIDsProcessed()
            notprocessed = job_supervisor.GetIDsNotProcessed()
            # Clean the successfully cancelled jobs
            if processed:
                job_supervisor.SelectByID(processed)
                self.log.info("Cleaning %i jobs", len(processed))
                if not job_supervisor.Clean():
                    self.log.warning("Failed to clean some jobs")
            
            # New job supervisor with the uncancellable jobs
            if notprocessed:
                notcancellable = [job for job in jobs.values() if job.JobID in notprocessed]
                job_supervisor = arc.JobSupervisor(self.uc, notcancellable)
                job_supervisor.Update()
                
                self.log.info("Cleaning %i jobs", len(notcancellable))
                if not job_supervisor.Clean():
                    self.log.warning("Failed to clean some jobs")
        
        # Empty job to reset DB info
        j = arc.Job()
        for (id, job) in jobs.items():
            self.db.updateArcJob(id, {"arcstate": "tosubmit",
                                           "tarcstate": self.db.getTimeStamp()}, j)

    def processToRerun(self):
        
        jobs = self.db.getArcJobs("arcstate='torerun' and cluster='"+self.cluster+"'")
        if not jobs:
            return

        job_supervisor = arc.JobSupervisor(self.uc, jobs.values())
        job_supervisor.Update()
        # Renew proxy to be safe
        job_supervisor.Renew()
        self.log.info("Resuming %i jobs", len(jobs.values()))
        job_supervisor = arc.JobSupervisor(self.uc, jobs.values())
        job_supervisor.Update()
        job_supervisor.Resume()
        
        notresumed = job_supervisor.GetIDsNotProcessed()

        for (id, job) in jobs.items():
            if job.JobID in notresumed:
                self.log.error("Could not resume job %s", job.JobID)
                self.db.updateArcJob(id, {"arcstate": "failed",
                                               "tarcstate": self.db.getTimeStamp()})
            else:
                self.db.updateArcJob(id, {"arcstate": "submitted",
                                               "tarcstate": self.db.getTimeStamp()})


    def process(self):

        # check jobs which failed to submit the previous loop
        self.checkFailedSubmissions()
        # process jobs which have to be cancelled
        self.processToCancel()
        # process jobs which have to be resubmitted
        self.processToResubmit()
        # process jobs which have to be rerun
        self.processToRerun()
        # submit new jobs
        self.submit()


# Main
if __name__ == '__main__':
    asb=aCTSubmitter('submitter', sys.argv[1])
    asb.run()
    asb.finish()
    
