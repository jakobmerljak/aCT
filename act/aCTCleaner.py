# aCTCleaner.py
#
# Cleans jobs from CE and ARC DB
#

import time
import os
import arc

import aCTConfig
import aCTUtils
import aCTSignal
import aCTLogger
import aCTDBArc

class aCTCleaner:
  
    def __init__(self):
        
        # logger
        self.logger=aCTLogger.aCTLogger("cleaner")
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
  
    def processToClean(self):
        
        jobs = self.db.getArcJobs("arcstate='toclean'")
        
        if not jobs:
            return

        self.log.info("Cleaning %i jobs", len(jobs.values()))
        job_supervisor = arc.JobSupervisor(self.uc, jobs.values())
        job_supervisor.Clean()
        
        notcleaned = job_supervisor.GetIDsNotProcessed()

        for (pandaid, job) in jobs.items():
            if job.JobID in notcleaned:
                self.log.error("Could not clean job %s", job.JobID)

            self.db.removeJobs(pandaid)  
  
    def run(self):
        """
        Main loop
        """
        self.log.info("Start")
        try:
            while 1:
                # reparse config file
                self.conf.parse()
                # clean jobs
                self.processToClean()
                aCTUtils.sleep(10)
                # restart periodically
                ip=int(self.conf.get(['periodicrestart','cleaner']))
                if time.time()-self.starttime > ip and ip != 0 :
                    self.log.info("Cleaner exited for periodic restart")
                    return
        except aCTSignal.ExceptInterrupt,x:
            self.log.error( x )


    def finish(self):
        """
        clean termination handled by signal
        """
        self.log.info("Cleanup")      

if __name__ == '__main__':
    st=aCTCleaner()
    st.run()
    st.finish()
