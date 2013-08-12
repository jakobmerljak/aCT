# aCTFetcher.py
#
# Fetches output from finished jobs
#

import time
import sys
import arc

from aCTProcess import aCTProcess

class aCTFetcher(aCTProcess):
    '''
    Downloads output data for finished ARC jobs.
    '''
  
    def fetchFailed(self):
        
        jobs = self.db.getArcJobs("arcstate='failed' and cluster='"+self.cluster+"'")
        
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
        
        jobs = self.db.getArcJobs("arcstate='finished' and cluster='"+self.cluster+"'")
        
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
  
  
    def process(self):

        # download failed job outputs
        self.fetchFailed()
        # download finished job outputs
        self.fetchFinished()


if __name__ == '__main__':
    st=aCTFetcher('fetcher', sys.argv[1])
    st.run()
    st.finish()
