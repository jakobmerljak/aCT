import os
import shutil
import tempfile
import time

from act.ldmx.aCTLDMXProcess import aCTLDMXProcess

class aCTLDMXGetJobs(aCTLDMXProcess):
    '''
    Pick up new jobs and register them in the LDMX db
    '''

    def __init__(self):
        aCTLDMXProcess.__init__(self)

    def getNewJobs(self):
        '''
        Read new job files in buffer dir
        '''

        bufferdir = self.conf.get(['jobs', 'bufferdir'])
        jobs = os.listdir(bufferdir)
        now = time.time()
        try:
            proxyid = self.dbarc.getProxiesInfo("attribute=''", ['id'], expect_one=True)['id']
        except Exception:
            self.log.error('No proxies found in DB')
            return

        for job in jobs:
            jobfile = os.path.join(bufferdir, job)
            # Avoid partially written files by delaying the read
            if now - os.path.getmtime(jobfile) < 5:
                self.log.debug(f'Job {jobfile} is too new')
                continue

            self.log.info(f'Picked up job at {jobfile}')
            newjobfile = os.path.join(self.tmpdir, job)
            with tempfile.NamedTemporaryFile(prefix=f'{newjobfile}.', delete=False) as f:
                newjobfile = f.name
                shutil.move(jobfile, newjobfile)

            self.dbldmx.insertJob(newjobfile, proxyid)
            self.log.info(f'Inserted job at {newjobfile} into DB')


    def process(self):

        self.getNewJobs()


if __name__ == '__main__':

    ar = aCTLDMXGetJobs()
    ar.run()
    ar.finish()
