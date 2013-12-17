# aCTCleaner.py
#
# Cleans jobs from CE and ARC DB
#

import arc

from aCTProcess import aCTProcess

class aCTCleaner(aCTProcess):
    
    def processToClean(self):
        
        jobstoclean = self.db.getArcJobs("arcstate='toclean' and cluster='"+self.cluster+"'")
        
        if not jobstoclean:
            return

        self.log.info("Cleaning %i jobs", sum(len(v) for v in jobstoclean.values()))
        for proxyid, jobs in jobstoclean.items():
            self.uc.CredentialString(self.db.getProxy(proxyid))

            job_supervisor = arc.JobSupervisor(self.uc, jobs.values())
            job_supervisor.Update()
            job_supervisor.Clean()
            
            notcleaned = job_supervisor.GetIDsNotProcessed()
    
            for (id, job) in jobs.items():
                if job.JobID in notcleaned:
                    self.log.error("Could not clean job %s", job.JobID)
    
                self.db.deleteArcJob(id)  
  
    def process(self):

        # clean jobs
        self.processToClean()


if __name__ == '__main__':
    st=aCTCleaner()
    st.run()
    st.finish()
