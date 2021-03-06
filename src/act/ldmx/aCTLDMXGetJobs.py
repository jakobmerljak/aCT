from collections import defaultdict
import os
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
            config['RandomSeed1'] = randomseed1+n
            config['RandomSeed2'] = randomseed2+n
            config['runNumber']   = randomseed1+n
            yield config

    def getOutputBase(self, config):

        output_rse = config.get('FinalOutputDestination')
        if not output_rse:
            return None

        # Get RSE URL with basepath. Exceptions will be caught by the caller
        rse_info = self.rucio.get_protocols(output_rse)
        if not rse_info or len(rse_info) < 1:
            raise f"Empty info returned by Rucio for RSE {output_rse}"

        return '{scheme}://{hostname}:{port}{prefix}'.format(**rse_info[0])

    def getNewJobs(self):
        '''
        Read new job files in buffer dir and create necessary job descriptions
        '''

        bufferdir = self.conf.get(['jobs', 'bufferdir'])
        configsdir = os.path.join(bufferdir, 'configs')
        os.makedirs(configsdir, 0o755, exist_ok=True)
        jobs = [os.path.join(configsdir, j) for j in os.listdir(configsdir) if os.path.isfile(os.path.join(configsdir, j))]
        now = time.time()
        try:
            # Take the first proxy available
            proxyid = self.dbarc.getProxiesInfo('TRUE', ['id'], expect_one=True)['id']
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
                    config = {l.split('=')[0]: l.split('=')[1].strip() for l in f if '=' in l}
                    batchid = config.get('BatchID', f'Batch-{time.strftime("%Y-%m-%dT%H:%M:%S")}')
                except Exception as e:
                    self.log.error(f'Failed to parse job config file {jobfile}: {e}')
                    os.remove(jobfile)
                    continue

            try:
                templatefile = os.path.join(bufferdir, 'templates', config['JobTemplate'])
                with open(templatefile) as tf:
                    template = tf.readlines()
            except Exception as e:
                self.log.error(f'Bad template file or template not defined in {jobfile}: {e}')
                os.remove(jobfile)
                continue

            try:
                # Get base path for output storage if necessary
                output_base = self.getOutputBase(config)

                # Generate copies of config and template
                for jobconfig in self.generateJobs(config):
                    newjobfile = os.path.join(self.tmpdir, os.path.basename(jobfile))
                    with tempfile.NamedTemporaryFile(mode='w', prefix=f'{newjobfile}.', delete=False, encoding='utf-8') as njf:
                        newjobfile = njf.name
                        njf.write('\n'.join(f'{k}={v}' for k,v in config.items()))
                        if output_base:
                            njf.write(f'\nFinalOutputBasePath={output_base}')
 
                        nouploadsites = self.arcconf.getListCond(["sites", "site"], "noupload=1", ["endpoint"])
                        self.log.debug(nouploadsites)
                        if nouploadsites:
                            njf.write(f'\nNoUploadSites={",".join(nouploadsites)}')

                    newtemplatefile = os.path.join(self.tmpdir, os.path.basename(templatefile))
                    with tempfile.NamedTemporaryFile(mode='w', prefix=f'{newtemplatefile}.', delete=False, encoding='utf-8') as ntf:
                        newtemplatefile = ntf.name
                        for l in template:
                            if l.startswith('sim.runNumber'):
                                ntf.write(f'sim.runNumber = {jobconfig["runNumber"]}\n')
                            elif l.startswith('p.run'):
                                ntf.write(f'p.run = {jobconfig["runNumber"]}\n')
                            elif l.startswith('sim.randomSeeds'):
                                ntf.write(f'sim.randomSeeds = [ {jobconfig["RandomSeed1"]}, {jobconfig["RandomSeed2"]} ]\n')
                            else:
                                ntf.write(l)

                    self.dbldmx.insertJob(newjobfile, newtemplatefile, proxyid, batchid=batchid)
                    self.log.info(f'Inserted job from {newjobfile} into DB')
            except Exception as e:
                self.log.error(f'Failed to create jobs from {jobfile}: {e}')
            os.remove(jobfile)


    def archiveBatches(self):
        '''Move completed batches to the archive table'''

        # Find out batch statuses
        batches = self.dbldmx.getGroupedJobs('batchid, ldmxstatus')
        batchdict = defaultdict(lambda: defaultdict(str))
        for batch in batches:
            batchdict[batch['batchid']][batch['ldmxstatus']] = batch['count(*)']

        for batchid, statuses in batchdict.items():
            if [s for s in statuses if s not in ['finished', 'failed', 'cancelled']]:
                continue

            # All jobs are finished, so archive
            select = f"batchid='{batchid}'" if batchid else 'batchid is NULL'
            columns = ['id', 'sitename', 'ldmxstatus', 'starttime', 'endtime', 'batchid']
            jobs = self.dbldmx.getJobs(select, columns)
            if not jobs:
                continue

            self.log.info(f'Archiving {len(jobs)} jobs for batch {batchid}')
            for job in jobs:
                self.log.debug(f'Archiving LDMX job {job["id"]}')
                self.dbldmx.insertJobArchiveLazy(job)
                self.dbldmx.deleteJob(job['id']) # commit is called here


    def process(self):

        self.getNewJobs()

        # Move old jobs to archive - every hour
        if time.time() - self.starttime > 3600:
            self.log.info("Checking for jobs to archive")
            self.archiveBatches()
            self.starttime = time.time()


if __name__ == '__main__':

    ar = aCTLDMXGetJobs()
    ar.run()
    ar.finish()
