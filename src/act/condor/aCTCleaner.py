# aCTCleaner.py
#
# Cleans jobs from Condor table
#

from act.common.aCTProcess import aCTProcess

class aCTCleaner(aCTProcess):
    
    def processToClean(self):

        select = "condorstate='toclean' and cluster='%s' limit 100" % self.cluster
        columns = ['id', 'ClusterId', 'appjobid']
        jobstoclean = self.dbcondor.getCondorJobsInfo(select, columns)

        if not jobstoclean:
            return

        self.log.info("Cleaning %d jobs" % len(jobstoclean))

        for job in jobstoclean:
            self.log.info("%s: Cleaning job %s" % (job['appjobid'], job['ClusterId']))
            self.dbcondor.deleteCondorJob(job['id'])

    def process(self):

        # clean jobs
        self.processToClean()


if __name__ == '__main__':
    st = aCTCleaner()
    st.run()
    st.finish()
