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

    def generateJobs(self, config):

        randomseed1 = int(config['RandomSeed1SequenceStart'])
        randomseed2 = int(config['RandomSeed2SequenceStart'])
        njobs = int(config['NumberofJobs'])
        self.log.info(f'Creating {njobs} jobs')
        for n in range(njobs):
            config['RandomSeed1'] = randomseed1
            config['RandomSeed2'] = randomseed2
            yield config
            randomseed1 += 1
            randomseed2 += 1

    def getNewJobs(self):
        '''
        Read new job files in buffer dir and create necessary job descriptions
        '''

        bufferdir = self.conf.get(['jobs', 'bufferdir'])
        configsdir = os.path.join(bufferdir, 'configs')
        jobs = [os.path.join(configsdir, j) for j in os.listdir(configsdir) if os.path.isfile(os.path.join(configsdir, j))]
        now = time.time()
        try:
            proxyid = self.dbarc.getProxiesInfo("attribute=''", ['id'], expect_one=True)['id']
        except Exception:
            self.log.error('No proxies found in DB')
            return

        for jobfile in jobs:

            # Avoid partially written files by delaying the read
            if now - os.path.getmtime(jobfile) < 5:
                self.log.debug(f'Job {jobfile} is too new')
                continue

            self.log.info(f'Picked up job at {jobfile}')

            with open(jobfile) as f:
                try:
                    config = {l.split('=')[0]: l.split('=')[1].strip() for l in f}
                except Exception as e:
                    self.log.error(f'Failed to parse job config file {jobfile}: probably badly-formed lines or missing "Template": {e}')
                    os.remove(jobfile)
                    continue

                try:
                    templatefile = os.path.join(bufferdir, 'templates', config['Template'])
                    with open(templatefile) as tf:
                        pass
                except Exception as e:
                    self.log.error(f'Bad template file or template not defined in {jobfile}: {e}')
                    os.remove(jobfile)
                    continue

                try:
                    for jobconfig in self.generateJobs(config):
                        newjobfile = os.path.join(self.tmpdir, os.path.basename(jobfile))
                        with tempfile.NamedTemporaryFile(mode='w', prefix=f'{newjobfile}.', delete=False, encoding='utf-8') as f:
                            newjobfile = f.name
                            f.write('\n'.join(f'{k}={v}' for k,v in jobconfig.items()))

                        self.dbldmx.insertJob(newjobfile, templatefile, proxyid)
                        self.log.info(f'Inserted job from {newjobfile} into DB')
                except Exception as e:
                    raise
                    self.log.error(f'Failed to create jobs from {jobfile}: {e}')
            os.remove(jobfile)


    def process(self):

        self.getNewJobs()


if __name__ == '__main__':

    ar = aCTLDMXGetJobs()
    ar.run()
    ar.finish()
