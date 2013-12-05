# aCTFetcher.py
#
# Fetches output from finished jobs
#

import time
import os
import errno
import arc

from aCTProcess import aCTProcess

class aCTFetcher(aCTProcess):
    '''
    Downloads output data for finished ARC jobs.
    '''
    
    def fetchAll(self, jobs):
        
        # Get all outputs using Job Supervisor
        job_supervisor = arc.JobSupervisor(self.uc, jobs.values())
        job_supervisor.Update()
        dirs = arc.StringList()
        job_supervisor.Retrieve(str(self.conf.get(['tmp','dir'])), False, False, dirs)
        
        return (list(job_supervisor.GetIDsProcessed()), list(job_supervisor.GetIDsNotProcessed()))
  
    def fetchSome(self, jobs, downloadfiles):
        
        # Get specified files for the jobs in downloadfiles
        # jobs: id: Job object
        # downloadfiles: id: list of files relative to session dir, with wildcards
        if not jobs or not downloadfiles:
            return ([], [])
        
        # construct datapoint object, initialising connection. Use the same
        # object until base URL changes. TODO group by base URL.
        dp = arc.datapoint_from_url(jobs.values()[0].JobID, self.uc)
        dm = arc.DataMover()
        dm.retry(False)
        dm.passive(True)
        dm.secure(False)
        
        fetched = []
        notfetched = []
        
        for (id, job) in jobs.items():
            if id not in downloadfiles:
                continue
            jobid = job.JobID
            
            # If connection URL is different reconnect
            if arc.URL(jobid).ConnectionURL() != dp:
                dp = arc.datapoint_from_url(jobid, self.uc)
            localdir = str(self.conf.get(['tmp','dir'])) + jobid[jobid.rfind('/'):] + '/'
            files = downloadfiles[id].split(',')
            
            # TODO wildcards. List dir and choose matching files
            for f in files:
                localfile = jobid + '/' + f
                localfiledir = localfile[:localfile.rfind('/')]
                # create required local dirs
                try:
                    os.makedirs(localfiledir)
                except OSError as e:
                    if e.errno != errno.EEXIST or not os.path.isdir(localfiledir):
                        self.log.warning('Failed to create directory %s: %s', localfiledir, os.strerror(e.errno))
                        notfetched.append(jobid)
                        break

                dp.SetURL(arc.URL(str(jobid + '/' + f)))
                localdp = arc.datapoint_from_url(str(localdir + f))
                # do the copy
                status = dm.Transfer(dp, localdp, arc.FileCache(), arc.URLMap())
                if not status:
                    self.log.warning('Failed to download %s: %s', dp.GetURL().str(), str(status))
                    notfetched.append(jobid)
                    break
                self.log.info('Downloaded %s', dp.GetURL().str())
            if jobid not in notfetched:
                fetched.append(jobid)
        return (fetched, notfetched)
                
                
    def fetchJobs(self, arcstate, nextarcstate):
        
        # Get list of jobs in the right state
        jobstofetch = self.db.getArcJobs("arcstate='"+arcstate+"' and cluster='"+self.cluster+"'")
        
        if not jobstofetch:
            return
        self.log.info("Fetching %i jobs", sum(len(v) for v in jobstofetch.values()))
        
        fetched = notfetched = []
        for proxyid, jobs in jobstofetch.items():
            # TODO: with ARC 4.0 use CredentialString()
            credentials = self.db.getProxyPath(proxyid)
            self.uc.ProxyPath(str(credentials))
            # Get list of downloadable files for these jobs
            filestodl = self.db.getArcJobsInfo("arcstate='"+arcstate+"' and cluster='"+self.cluster+"' and proxyid='"+str(proxyid)+"'", ['id', 'downloadfiles'])
            # id: downloadfiles
            downloadfiles = dict((row['id'], row['downloadfiles']) for row in filestodl)
            # jobs to download all files
            jobs_downloadall = dict((j, jobs[j]) for j in jobs if not downloadfiles[j])
            # jobs to download specific files
            jobs_downloadsome = dict((j, jobs[j]) for j in jobs if downloadfiles[j])
            
            (f, n) = self.fetchAll(jobs_downloadall)
            fetched.extend(f)
            notfetched.extend(n)

            (f, n) = self.fetchSome(jobs_downloadsome, downloadfiles)
            fetched.extend(f)
            notfetched.extend(n)

        # Check for massive failure, and back off before trying again
        # TODO: downtime awareness
        if len(notfetched) > 10 and len(notfetched) == len(jobs):
            self.log.error("Failed to get any jobs from %s, sleeping for 5 mins", self.cluster)
            time.sleep(300)
            return
        
        for (id, job) in jobs.items():
            if job.JobID in notfetched:
                # Try again next time
                self.log.warning("Could not get output from job %s", job.JobID)
            else:
                self.db.updateArcJob(id, {"arcstate": nextarcstate,
                                          "tarcstate": self.db.getTimeStamp()})

    def process(self):

        # download failed job outputs that should be fetched
        self.fetchJobs('tofetch', 'donefailed')
        # download finished job outputs
        self.fetchJobs('finished', 'done')


if __name__ == '__main__':
    st=aCTFetcher()
    st.run()
    st.finish()
