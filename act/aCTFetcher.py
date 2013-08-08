# aCTFetcher.py
#
# Fetches output from finished jobs
#

import time
import os
import arc

import aCTConfig
import aCTUtils
import aCTSignal
import aCTLogger
import aCTDBArc

class aCTFetcher:
  
    def __init__(self):
        
        # logger
        self.logger=aCTLogger.aCTLogger("fetcher")
        self.log=self.logger()

        # config
        self.conf=aCTConfig.aCTConfig()
        # database
        self.db=aCTDBArc.aCTDBArc(self.log,self.conf.get(["db","file"]))
        
        # ARC Configuration
        self.uc = arc.UserConfig()
        self.uc.ProxyPath("/tmp/x509up_u%s" % os.getuid())
        self.uc.CACertificatesDirectory("/etc/grid-security/certificates")
        timeout=int(self.conf.get(['atlasgiis','timeout']))
        self.uc.Timeout(timeout)

        # start time for periodic restart
        self.starttime=time.time()
        self.log.info("Started")
  
    def fetchFailed(self):
        
        jobs = self.db.getArcJobs("arcstate='failed'")
        
        if not jobs:
            return

        self.log.info("Fetching %i jobs", len(jobs.values()))
        job_supervisor = arc.JobSupervisor(self.uc, jobs.values())
        dirs = arc.StringList()
        job_supervisor.Retrieve(str(self.conf.get(['tmp','dir'])), False, False, dirs)
        
        notfetched = job_supervisor.GetIDsNotProcessed()

        for (pandaid, job) in jobs.items():
            if job.JobID in notfetched:
                # TODO: Try again?
                self.log.error("Could not get output from job %s", job.JobID)

            self.db.updateArcJob(pandaid, {"arcstate": "donefailed",
                                           "tarcstate": time.time()})
  
    def fetchFinished(self):
        
        jobs = self.db.getArcJobs("arcstate='finished'")
        
        if not jobs:
            return

        self.log.info("Fetching %i jobs", len(jobs.values()))
        job_supervisor = arc.JobSupervisor(self.uc, jobs.values())
        dirs = arc.StringList()
        job_supervisor.Retrieve(str(self.conf.get(['tmp','dir'])), False, False, dirs)
        
        notfetched = job_supervisor.GetIDsNotProcessed()

        for (pandaid, job) in jobs.items():
            if job.JobID in notfetched:
                # TODO: Try again?
                self.log.error("Could not get output from job %s", job.JobID)

            self.db.updateArcJob(pandaid, {"arcstate": "done",
                                           "tarcstate": time.time()})
  
  
    def run(self):
        """
        Main loop
        """
        self.log.info("Start")
        try:
            while 1:
                # reparse config file
                self.conf.parse()
                # download failed job outputs
                self.fetchFailed()
                # download finished job outputs
                self.fetchFinished()
                aCTUtils.sleep(10)
                # restart periodically
                ip=int(self.conf.get(['periodicrestart','fetcher']))
                if time.time()-self.starttime > ip and ip != 0 :
                    self.log.info("Fetcher exited for periodic restart")
                    return
        except aCTSignal.ExceptInterrupt,x:
            self.log.error( x )


    def finish(self):
        """
        clean termination handled by signal
        """
        self.log.info("Cleanup")      

if __name__ == '__main__':
    st=aCTFetcher()
    st.run()
    st.finish()