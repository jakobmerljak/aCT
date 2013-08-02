#!/usr/bin/python

import os
import time
import arc

import aCTDBArc
import aCTConfig
import aCTLogger
import aCTUtils
import aCTSignal

class aCTStatus:
    
    def __init__(self):
        
        # logger
        self.logger=aCTLogger.aCTLogger("status")
        self.log=self.logger()

        # config
        self.conf=aCTConfig.aCTConfig()
        # database
        self.dbarc=aCTDBArc.aCTDBArc(self.log,self.conf.get(["db","file"]))
        
        # ARC Configuration
        self.uc = arc.UserConfig()
        self.uc.ProxyPath("/tmp/x509up_u%s" % os.getuid())
        self.uc.CACertificatesDirectory("/etc/grid-security/certificates")
        timeout=int(self.conf.get(['atlasgiis','timeout']))
        self.uc.Timeout(timeout)

        # store the last checkJobs time to avoid overloading of GIIS
        self.checktime=time.time()
        self.checktimep=time.time()
        # start time for periodic restart
        self.starttime=time.time()
        self.log.info("Started")

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
        jobs=self.dbarc.getArcJobs("(arcstate='submitted' or arcstate='running' or arcstate='cancelling') and tarcstate<strftime('%s','now')-"+str(self.conf.get(['jobs','checkinterval'])) + " limit 100000")

        # TODO: make function for this in aCTDBPanda
        # number of total jobs
        njobs=self.dbarc.getNArcJobs()

        # Do not check too little jobs at once (at least 1% of running jobs)
        if len(jobs) < njobs/1000:
            #self.log.debug("too few to check %d" % len(jobs))
            return
        if len(jobs):
            self.log.info("%d jobs to check" % len(jobs))
        else:
            return
        
        job_supervisor = arc.JobSupervisor(self.uc, jobs.values())
        job_supervisor.Update()
        jobsupdated = job_supervisor.GetAllJobs()
        jobsnotupdated = job_supervisor.GetIDsNotProcessed()
        
        for (pandaid, originaljob, updatedjob) in zip(jobs.keys(), jobs.values(), jobsupdated):
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
                self.dbarc.updateArcJob(pandaid, {'tarcstate': time.time()})
                continue
            
            self.log.debug("Job %s: %s -> %s", originaljob.JobID, originaljob.State.GetGeneralState(), updatedjob.State.GetGeneralState())
            # state changed, update whole Job object
            arcstate = 'submitted'
            if updatedjob.State == arc.JobState.FINISHED and updatedjob.ExitCode == 0:
                arcstate = 'finished'
            elif updatedjob.State == arc.JobState.FINISHED or \
                 updatedjob.State == arc.JobState.FAILED:
                arcstate = 'failed'
            elif updatedjob.State == arc.JobState.KILLED:
                arcstate = 'cancelled'
            elif updatedjob.State == arc.JobState.RUNNING:
                arcstate = 'running'
            elif updatedjob.State == arc.JobState.DELETED or \
                 updatedjob.State == arc.JobState.OTHER:
                # unexpected
                arcstate = 'failed'
                
            self.dbarc.updateArcJob(pandaid, {'arcstate': arcstate, 'tarcstate': time.time()}, updatedjob)
                
        self.log.info('Done')
    
    def run(self):
        """
        Main loop
        """
        self.log.info("Start")
        try:
            while 1:
                # reparse config file
                self.conf.parse()
                # check job status
                # more frequent checks for panda update
                self.checkJobs()
                aCTUtils.sleep(10)
                # restart periodically
                ip=int(self.conf.get(['periodicrestart','status']))
                if time.time()-self.starttime > ip and ip != 0 :
                    self.log.info("Status exited for periodic restart")
                    return
        except aCTSignal.ExceptInterrupt,x:
            self.log.error( x )


    def finish(self):
        """
        clean termination handled by signal
        """
        self.log.info("Cleanup")      

if __name__ == '__main__':
    st=aCTStatus()
    st.run()
    st.finish()
