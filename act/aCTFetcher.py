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
  
    def fetchJobs(self, arcstate, nextarcstate):
        
        jobs = self.db.getArcJobs("arcstate='"+arcstate+"' and cluster='"+self.cluster+"'")
        
        if not jobs:
            return

        self.log.info("Fetching %i jobs", len(jobs.values()))
        job_supervisor = arc.JobSupervisor(self.uc, jobs.values())
        dirs = arc.StringList()
        job_supervisor.Retrieve(str(self.conf.get(['tmp','dir'])), False, False, dirs)
        
        fetched = job_supervisor.GetIDsProcessed()
        notfetched = job_supervisor.GetIDsNotProcessed()

        # Check for massive failure, and back off before trying again
        # TODO: downtime awareness
        if len(notfetched) > 10 and len(notfetched) == len(jobs):
            self.log.error("Failed to get any jobs from %s, sleeping for 5 mins", self.cluster)
            time.sleep(300)
            return
        
        for (id, job) in jobs.items():
            if job.JobID in notfetched:
                # Try again next time
                self.log.error("Could not get output from job %s", job.JobID)
            else:
                self.db.updateArcJob(id, {"arcstate": nextarcstate,
                                          "tarcstate": self.db.getTimeStamp()})
  

    def process(self):

        # download failed job outputs
        self.fetchJobs('failed', 'donefailed')
        # download finished job outputs
        self.fetchJobs('finished', 'done')


if __name__ == '__main__':
    st=aCTFetcher('fetcher', sys.argv[1])
    st.run()
    st.finish()
