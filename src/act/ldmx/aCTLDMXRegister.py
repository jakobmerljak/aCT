import json
import time
import os
import shutil
import sys
import traceback

from rucio.client import Client
from rucio.common.exception import RucioException, DataIdentifierNotFound

from act.common import aCTLogger
from act.common import aCTConfig
from act.common import aCTUtils
from act.common import aCTSignal
from act.arc import aCTDBArc

class aCTLDMXRegister:
    '''
    Post-processing for LDMX jobs. Reads metadata json file and registers output
    files in Rucio.
    '''

    def __init__(self):

        # Get agent name from /path/to/aCTAgent.py
        self.name = os.path.basename(sys.argv[0])[:-3]

        # logger
        self.logger = aCTLogger.aCTLogger(self.name)
        self.log = self.logger()
        self.criticallogger = aCTLogger.aCTLogger('aCTCritical', arclog=False)
        self.criticallog = self.criticallogger()

        # config
        self.conf = aCTConfig.aCTConfigAPP()
        self.arcconf = aCTConfig.aCTConfigARC()
        self.tmpdir = str(self.arcconf.get(['tmp', 'dir']))
        # database
        self.dbarc = aCTDBArc.aCTDBArc(self.log)

        self.rucio = Client()

        # start time for periodic restart
        self.starttime = time.time()
        self.log.info("Started %s", self.name)


    def cleanFailedJobs(self):
        '''
        Look for failed jobs and set them to clean
        TODO: whether to try and keep logs
        '''
        # Get outputs to download for failed jobs
        select = "arcstate='failed'"
        columns = ['id']
        arcjobs = self.dbarc.getArcJobsInfo(select, columns)
        if arcjobs:
            for aj in arcjobs:
                select = "id='{}'".format(str(aj["id"]))
                desc = {"arcstate": "tofetch", "tarcstate": self.dbarc.getTimeStamp()}
                self.dbarc.updateArcJobsLazy(desc, select)
            self.dbarc.Commit()

        # Look for failed final states in ARC which are still starting or running in panda
        select = "arcstate in ('donefailed', 'cancelled', 'lost')"
        columns = ['arcstate', 'id', 'appjobid']

        jobstoupdate = self.dbarc.getArcJobsInfo(select, columns=columns)

        if not jobstoupdate:
            return

        failedjobs = [job for job in jobstoupdate if job['arcstate']=='donefailed']
        if len(failedjobs) != 0:
            self.log.debug("Found %d failed jobs (%s)" % (len(jobstoupdate), ','.join([j['appjobid'] for j in jobstoupdate])))
        lostjobs = [job for job in jobstoupdate if job['arcstate']=='lost']
        if len(lostjobs) != 0:
            self.log.debug("Found %d lost jobs (%s)" % (len(jobstoupdate), ','.join([j['appjobid'] for j in jobstoupdate])))
        cancelledjobs = [job for job in jobstoupdate if job['arcstate']=='cancelled']
        if len(cancelledjobs) != 0:
            self.log.debug("Found %d cancelled jobs (%s)" % (len(jobstoupdate), ','.join([j['appjobid'] for j in jobstoupdate])))

        desc = {"arcstate": "toclean", "tarcstate": self.dbarc.getTimeStamp()}
        for aj in failedjobs:
            select = f"id={aj['id']}"
            self.dbarc.updateArcJobsLazy(desc, select)

        for aj in lostjobs:
            select = f"id={aj['id']}"
            self.dbarc.updateArcJobsLazy(desc, select)

        for aj in cancelledjobs:
            select = f"id={aj['id']}"
            self.dbarc.updateArcJobsLazy(desc, select)

        self.dbarc.Commit()


    def processDoneJobs(self):
        '''
        Look for done jobs, and register output metadata in Rucio
        '''

        # Get the done jobs, set to clean
        select = "arcstate='done'"
        columns = ['id', 'JobID', 'appjobid', 'cluster', 'UsedTotalWallTime',
                   'EndTime', 'ExecutionNode', 'stdout', 'fairshare', 'created']
        arcjobs = self.dbarc.getArcJobsInfo(select, columns=columns)
        if not arcjobs:
            return

        # Set the arcjob to toclean
        for aj in arcjobs:
            select = f"id={aj['id']}"
            desc = {"arcstate": "toclean", "tarcstate": self.dbarc.getTimeStamp()}
            self.dbarc.updateArcJobsLazy(desc, select)
        self.dbarc.Commit()

        for aj in arcjobs:
            jobid = aj.get('JobID')
            if not jobid:
                self.log.error(f'No JobID in arcjob {aj["id"]}')
                continue

            # copy to joblog dir files downloaded for the job: gmlog errors and job stdout
            self.copyOutputFiles(aj)
 
            # Read the metadata and insert into rucio
            self.insertMetadata(aj)

            # Clean tmp dir
            self.cleanDownloadedJob(jobid)


    def copyOutputFiles(self, arcjob):
        '''
        Copy job stdout and errors log to final location
        '''
        sessionid = arcjob['JobID'][arcjob['JobID'].rfind('/')+1:]
        date = arcjob['created'].strftime('%Y-%m-%d')
        outd = os.path.join(self.conf.get(['joblog','dir']), date)
        os.makedirs(outd, 0o755, exist_ok=True)

        localdir = os.path.join(self.tmpdir, sessionid)
        gmlogerrors = os.path.join(localdir, "gmlog", "errors")
        arcjoblog = os.path.join(outd, "%s.log" % arcjob['appjobid'])
        try:
            shutil.move(gmlogerrors, arcjoblog)
            os.chmod(arcjoblog, 0o644)
        except Exception as e:
            self.log.error(f'Failed to copy {gmlogerrors}: {e}')

        jobstdout = arcjob['stdout']
        if jobstdout:
            try:
                shutil.move(os.path.join(localdir, jobstdout),
                            os.path.join(outd, '%s.out' % arcjob['appjobid']))
                os.chmod(os.path.join(outd, '%s.out' % arcjob['appjobid']), 0o644)
            except Exception as e:
                self.log.error(f'Failed to copy file {os.path.join(localdir, jobstdout)}, {str(e)}')


    def insertMetadata(self, arcjob):
        '''
        Read metadata file and insert into Rucio
        '''
        sessionid = arcjob['JobID'][arcjob['JobID'].rfind('/')+1:]
        metadatafile = os.path.join(self.tmpdir, sessionid, 'metadata.json')
        try:
            with open(metadatafile) as f:
                metadata = json.load(f)
        except Exception as e:
            self.log.error(f'Failed to read metadata.json file at {metadatafile}: {e}')
            return

        try:
            scope = metadata['scope']
            name = metadata['name']
            dscope = metadata['datasetscope']
            dname = metadata['datasetname']
            self.log.info(f'Inserting metadata info for {scope}:{name}: {metadata}')
            # Add replica
            self.rucio.add_replica(metadata['rse'], scope, name, metadata['bytes'],
                                   metadata['adler32'], metadata['pfn'])
            try:
                # Attach to dataset
                self.rucio.attach_dids(dscope, dname, [{'scope': scope, 'name': name}])
            except DataIdentifierNotFound:
                try:
                    self.rucio.add_dataset(dscope, dname)
                except RucioException as e:
                    self.log.error(f'Dataset {dscope}:{dname} does not exist and failed to create it: {e}')
                else:
                    self.rucio.attach_dids(dscope, dname, [{'scope': scope, 'name': name}])

            # Add metadata, removing all rucio "native" metadata
            native_metadata = ['scope', 'name', 'bytes', 'adler32', 'rse', 'pfn',
                               'datasetscope', 'datasetname']
            # Metadata values must be strings to be searchable
            self.rucio.add_did_meta(scope, name,
                                    {x: str(y) for x, y in metadata.items() if x not in native_metadata})
        except KeyError as e:
            self.log.info(f'key missing in metadata json: {e}')


    def cleanDownloadedJob(self, arcjobid):
        '''
        Remove directory to which job was downloaded.
        '''

        sessionid = arcjobid[arcjobid.rfind('/')+1:]
        localdir = os.path.join(self.tmpdir, sessionid)
        self.log.debug(f'Removing directory {localdir}')
        shutil.rmtree(localdir, ignore_errors=True)


    def process(self):

        # Look for failed jobs and set the to clean
        # Look for done jobs and process the metadata
        # Clean done jobs in ARC
        self.cleanFailedJobs()
        self.processDoneJobs()


    def run(self):
        '''
        Main loop
        '''
        try:
            while 1:
                # parse config file
                self.conf.parse()
                self.arcconf.parse()
                # do class-specific things
                self.process()
                # sleep
                aCTUtils.sleep(2)
        except aCTSignal.ExceptInterrupt as x:
            self.log.info("Received interrupt %s, exiting", str(x))
        except:
            self.log.critical("*** Unexpected exception! ***")
            self.log.critical(traceback.format_exc())
            self.log.critical("*** Process exiting ***")
            self.criticallog.critical(traceback.format_exc())

    def finish(self):
        '''
        Clean up code when process exits
        '''
        self.log.info("Cleanup for %s", self.name)

if __name__ == '__main__':

    ar = aCTLDMXRegister()
    ar.run()
    ar.finish()
