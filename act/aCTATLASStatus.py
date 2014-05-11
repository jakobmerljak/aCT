# Handler for filling pandajobs information from arcjobs information. Also
# deals with post-processing of jobs and error handling.

import time
import datetime
import pickle
import re
import os
import shutil

import aCTSignal
import aCTUtils

from aCTATLASProcess import aCTATLASProcess

class aCTATLASStatus(aCTATLASProcess):
                 
    def checkJobstoKill(self):
        """
        Check for jobs with pandastatus tobekilled and cancel them in ARC.
        """
        
        jobs = self.dbpanda.getJobs("actpandastatus='tobekilled'")
        if not jobs:
            return
        
        self.log.info("Found %d jobs to cancel" % len(jobs))
        for job in jobs:
            self.log.info("Cancelling job %d", job['pandaid'])
            # check if arcjobid is set before cancelling the job
            if job['arcjobid']:
                self.dbarc.updateArcJob(job['arcjobid'], {'arcstate': 'tocancel'})
        
        self.dbpanda.updateJobs("actpandastatus='tobekilled'", {'actpandastatus': 'cancelled'})
           
    def getStartTime(self, endtime, walltime):
        """
        Get starttime from endtime-walltime where endtime is datetime.datetime and walltime is in seconds
        If endtime is none then use current time
        """
        if not endtime:
            return datetime.datetime.utcnow() - datetime.timedelta(0, walltime)
        return endtime-datetime.timedelta(0, walltime)
           
           
    def updateStartingJobs(self):
        """
        Check for sent jobs that have been submitted to ARC and update
        actpandastatus to starting, and also for jobs that were requeued
        from running.
        """

        select = "arcjobs.id=pandajobs.arcjobid and (arcjobs.arcstate='submitted' or arcjobs.arcstate='holding')"
        select += " and (pandajobs.actpandastatus='sent' or pandajobs.actpandastatus='running')"
        select += " limit 100000"
        columns = ["arcjobs.id", "arcjobs.cluster"]
        jobstoupdate=self.dbarc.getArcJobsInfo(select, columns=columns, tables="arcjobs,pandajobs")

        if len(jobstoupdate) == 0:
            return
        else:
            self.log.debug("Found %d submitted jobs", len(jobstoupdate))

        for aj in jobstoupdate:
            select = "arcjobid='"+str(aj["id"])+"'"
            desc = {}
            desc["pandastatus"] = "starting"
            desc["actpandastatus"] = "starting"
            desc["computingElement"] = aj["cluster"].split('/')[0]
            self.dbpanda.updateJobsLazy(select, desc)
        self.dbpanda.Commit()

        
    def updateRunningJobs(self):
        """
        Check for new running jobs and update pandajobs with
        - pandastatus
        - node
        - computingElement
        - startTime
        """

        # do an inner join to pick up all jobs that should be set to running
        # todo: pandajobs.starttime will not be updated if a job is resubmitted 
        # internally by the ARC part.
        select = "arcjobs.id=pandajobs.arcjobid and arcjobs.arcstate='running' and pandajobs.actpandastatus='starting'"
        select += " limit 100000"
        columns = ["arcjobs.id", "arcjobs.UsedTotalWalltime", "arcjobs.ExecutionNode", "arcjobs.cluster"]
        jobstoupdate=self.dbarc.getArcJobsInfo(select, columns=columns, tables="arcjobs,pandajobs")

        if len(jobstoupdate) == 0:
            return
        else:
            self.log.debug("Found %d running jobs", len(jobstoupdate))

        for aj in jobstoupdate:
            select = "arcjobid='"+str(aj["id"])+"'"
            desc = {}
            desc["pandastatus"] = "running"
            desc["actpandastatus"] = "running"
            desc["node"] = aj["ExecutionNode"]
            desc["computingElement"] = aj["cluster"].split('/')[0]
            desc["startTime"] = self.getStartTime(datetime.datetime.utcnow(), aj['UsedTotalWalltime'])
            self.dbpanda.updateJobsLazy(select, desc)
        self.dbpanda.Commit()

        
    def updateFinishedJobs(self):
        """
        Check for new finished jobs, update pandajobs with
        - pandastatus
        - startTime
        - endTime
        """
        # don't get jobs already having actpandastatus tovalidate or finished to avoid race conditions with validator
        select = "arcjobs.id=pandajobs.arcjobid and arcjobs.arcstate='done' and pandajobs.actpandastatus not like 'tovalidate'"
        select += " and pandajobs.actpandastatus not like 'finished'"
        select += " limit 100000"
        columns = ["arcjobs.id", "arcjobs.UsedTotalWallTime", "arcjobs.EndTime"]
        jobstoupdate=self.dbarc.getArcJobsInfo(select, tables="arcjobs,pandajobs", columns=columns)
        
        if len(jobstoupdate) == 0:
            return
        else:
            self.log.debug("Found %d finished jobs", len(jobstoupdate))


        for aj in jobstoupdate:
            select = "arcjobid='"+str(aj["id"])+"'"
            desc = {}
            desc["pandastatus"] = "transferring"
            desc["actpandastatus"] = "tovalidate"
            desc["startTime"] = self.getStartTime(aj['EndTime'], aj['UsedTotalWallTime'])
            desc["endTime"] = aj["EndTime"]
            self.dbpanda.updateJobsLazy(select, desc)
        self.dbpanda.Commit()


    def checkFailed(self, arcjobs):
        failedjobs = []
        resubmitting=False
        for aj in arcjobs:
            resubmit=False
            # todo: errors part of aCTConfigARC should probably be moved to aCTConfigATLAS.
            for error in self.arcconf.getList(['errors','toresubmit','arcerrors','item']):
                if aj['Error'].find(error) != -1:
                    resubmit=True
            if resubmit:
                self.log.info("%s: Resubmitting %d %s %s" % (aj['appjobid'],aj['arcjobid'],aj['JobID'],aj['Error']))
                select = "arcjobid='"+str(aj["arcjobid"])+"'"
                jd={}
                # Validator processes this state before setting back to starting
                jd['pandastatus'] = 'starting'
                jd['actpandastatus'] = 'toresubmit'
                self.dbpanda.updateJobsLazy(select,jd)
                resubmitting=True
            else:
                failedjobs += [aj]
        if resubmitting:
            self.dbpanda.Commit()
            self.dbarc.Commit()
        return failedjobs

    def processFailed(self, arcjobs):
        """
        process jobs failed for other reasons than athena (log_extracts was not created by pilot)
        """
        if len(arcjobs):
            self.log.info("processing %d failed jobs" % len(arcjobs))
        else:
            return


        for aj in arcjobs:
            xml=""
            log=""
            cluster=aj['cluster'].split('/')[0]
            jobid=aj['JobID']
            sessionid=jobid[jobid.rfind('/'):]
            date = time.strftime('%Y%m%d')
            try:
                os.mkdir(self.conf.get(['joblog','dir']) + "/" + date)
            except:
                pass
            try:
                os.mkdir(self.conf.get(['joblog','dir']) + "/" + date + "/" + cluster )
            except:
                pass
            outd = self.conf.get(['joblog','dir']) + "/" + date + "/" + cluster + "/" + sessionid
            try:
                shutil.rmtree(outd)
            except:
                pass
            # copy from tmp to outd.
            localdir = str(self.arcconf.get(['tmp','dir'])) + sessionid
            # Sometimes fetcher fails to get output, so just make empty dir
            try:
                shutil.copytree(localdir, outd)
            except OSError, e:
                self.log.warning("%s: Failed to copy job output for %s: %s" % (aj['appjobid'], jobid, str(e)))
                try:
                    os.makedirs(outd, 0755)
                except OSError, e:
                    self.log.warning("%s: Failed to create %s: %s. Job logs will be missing" % (aj['appjobid'], outd, str(e)))
            else:
                shutil.rmtree(localdir, ignore_errors=True)
                
            # set right permissions
            aCTUtils.setFilePermissionsRecursive(outd)

            # prepare extracts
            nlines=20
            log=""
            try:
                f=open(outd+"/gmlog/failed","r")
                log+="---------------------------------------------------------------\n"
                log+="GMLOG: failed\n"
                log+="---------------------------------------------------------------\n"
                log+=''.join(f.readlines())
                f.close()
            except:
                pass
            

            import glob
            lf=glob.glob(outd+"/log*")
            try:
                f=open(lf[0],"r")
                lines=f.readlines()
                log+="---------------------------------------------------------------\n"
                log+="LOGFILE: tail\n"
                log+="---------------------------------------------------------------\n"
                lns=[]
                for l in lines:
                    if re.match('.*error',l,re.IGNORECASE):
                        lns.append(l)
                    if re.match('.*warning',l,re.IGNORECASE):
                        lns.append(l)
                    if re.match('.*failed',l,re.IGNORECASE):
                        lns.append(l)
                log+=''.join(lns[:nlines])
                # copy logfiles to failedlogs dir
                failedlogsd = self.conf.get(["tmp","dir"])+"/failedlogs"
                try:
                    os.mkdir(failedlogsd)
                except:
                    pass
                try:
                    f=open(os.path.join(failedlogsd, str(aj['pandaid'])+".log","w"))
                    f.write(log)
                    f.close()
                except:
                    pass

            except:
                pass

            #print log

            xml=""
            # xml and log

            # set update, pickle from pilot is not available
            # some values might not be properly set
            # TODO synchronize error codes with the rest of production
            pupdate={}
            pupdate['xml']=str(xml)
            pupdate['siteName']='ARC'
            pupdate['computingElement']=aj['cluster'].split('/')[0]
            pupdate['schedulerID']=self.conf.get(['panda','schedulerid'])
            pupdate['pilotID']=self.conf.get(["joblog","urlprefix"])+"/"+date+"/"+cluster+sessionid+"|Unknown|Unknown|Unknown|Unknown"
            try:
                pupdate['node']=aj['ExecutionNode']
            except:
                pass
            pupdate['pilotLog']=log
            pupdate['cpuConsumptionTime']=aj['UsedTotalCPUTime']
            pupdate['cpuConsumptionUnit']='seconds'
            pupdate['cpuConversionFactor']=1
            pupdate['pilotTiming']="0|0|%s|0" % aj['UsedTotalWallTime']
            pupdate['exeErrorCode']=aj['ExitCode']
            pupdate['exeErrorDiag']=aj['Error']
            pupdate['pilotErrorCode']=1008
            codes=[]
            codes.append("Job timeout")
            codes.append("qmaster enforced h_rt limit")
            codes.append("job killed: wall")
            codes.append("Job exceeded time limit")
            for errcode in codes:
                res=re.match(".*"+errcode+".*",aj['Error'])
                if res is not None:
                    pupdate['pilotErrorCode']=1213
                    #print pupdate['pilotErrorCode'],aj['Error']
            codes=[]
            codes.append("Job probably exceeded memory limit")
            codes.append("job killed: vmem")
            codes.append("pvmem exceeded")
            for errcode in codes:
                res=re.match(".*"+errcode+".*",aj['Error'])
                if res is not None:
                    pupdate['pilotErrorCode']=1212
                    #print pupdate['pilotErrorCode'],aj['Error']
            pupdate['pilotErrorDiag']=aj['Error']
            # set start/endtime
            pupdate['startTime']=self.getStartTime(aj['EndTime'], aj['UsedTotalWallTime'])
            pupdate['endTime']=aj['EndTime']
            # save the pickle file to be used by aCTMain panda update
            select="arcjobid='"+str(aj["arcjobid"])+"'"
            j = self.dbpanda.getJobs(select, ["pandaid"])
            if j: # panda job could be missing for whatever reason
                try:
                    os.mkdir(self.conf.get(['tmp','dir'])+"/pickle")
                except:
                    pass
                f=open(self.conf.get(['tmp','dir'])+"/pickle/"+str(j[0]['pandaid'])+".pickle","w")
                pickle.dump(pupdate,f)
                f.close()

    
    def updateFailedJobs(self):
        """
        Query jobs in arcstate failed, set to tofetch
        Query jobs in arcstate donefailed, cancelled and lost, set to toclean.
        If they should be resubmitted, set arcjobid to null in pandajobs
        If not do post-processing and fill status in pandajobs
        """
        # Get outputs to download for failed jobs
        select = "arcstate='failed'"
        columns = ['id', 'stdout', 'logdir']
        arcjobs = self.dbarc.getArcJobsInfo(select, columns)
        if arcjobs:
            for aj in arcjobs:
                downloadfiles = 'jobSmallFiles.tgz'
                if aj['stdout']:
                    downloadfiles += ';' + aj['stdout']
                if aj['logdir']:
                    downloadfiles += ';' + aj['logdir'] + '/*'
                select = "id='"+str(aj["id"])+"'"
                desc = {"arcstate":"tofetch", "tarcstate": self.dbarc.getTimeStamp(), "downloadfiles": downloadfiles}
                self.dbarc.updateArcJobsLazy(desc, select)
            self.dbarc.Commit()
        
        # Look for failed final states
        select = "(arcstate='donefailed' or arcstate='cancelled' or arcstate='lost')"
        select += " and actpandastatus!='toclean' and actpandastatus!='toresubmit'"
        select += " and pandajobs.arcjobid = arcjobs.id limit 100000"

        jobstoupdate=self.dbarc.getArcJobsInfo(select, tables='arcjobs,pandajobs')

        if len(jobstoupdate) == 0:
            return
        
        failedjobs = [job for job in jobstoupdate if job['arcstate']=='donefailed']
        if len(failedjobs) != 0:
            self.log.debug("Found %d failed jobs", len(failedjobs))
        lostjobs = [job for job in jobstoupdate if job['arcstate']=='lost']
        if len(lostjobs) != 0:
            self.log.debug("Found %d lost jobs", len(lostjobs))
        cancelledjobs = [job for job in jobstoupdate if job['arcstate']=='cancelled']
        if len(cancelledjobs) != 0:
            self.log.debug("Found %d cancelled jobs", len(cancelledjobs))
                
        failedjobs=self.checkFailed(failedjobs)
        # process all failed jobs that couldn't be resubmitted
        self.processFailed(failedjobs)

        for aj in failedjobs:
            select = "arcjobid='"+str(aj["arcjobid"])+"'"
            desc = {}
            desc["pandastatus"] = "transferring"
            desc["actpandastatus"] = "toclean" # to clean up any output
            desc["endTime"] = aj["EndTime"]
            self.dbpanda.updateJobsLazy(select, desc)

        for aj in lostjobs:
            self.log.info("%s: Resubmitting lost job %d %s %s" % (aj['appjobid'], aj['arcjobid'],aj['JobID'],aj['Error']))
            select = "arcjobid='"+str(aj["arcjobid"])+"'"
            desc={}
            # Validator processes this state before setting back to starting
            desc['pandastatus'] = 'starting'
            desc['actpandastatus'] = 'toresubmit'
            self.dbpanda.updateJobsLazy(select,desc)

        for aj in cancelledjobs:
            # For jobs that panda cancelled, don't do anything, they already
            # have actpandastatus=cancelled. For jobs that were killed in arc,
            # resubmit
            select = "arcjobid='"+str(aj["arcjobid"])+"' and actpandastatus!='cancelled'"
            desc = {}
            desc["pandastatus"] = "starting"
            desc["actpandastatus"] = "starting"
            desc["arcjobid"] = None
            self.dbpanda.updateJobsLazy(select, desc)
        
        if len(failedjobs)+len(lostjobs)+len(cancelledjobs)!=0:
            self.dbpanda.Commit()

        # set arcjobs state toclean for cancelled jobs
        desc = {"arcstate":"toclean", "tarcstate": self.dbarc.getTimeStamp()}
        for aj in cancelledjobs:
            self.dbarc.updateArcJobLazy(aj["arcjobid"], desc)
        if len(cancelledjobs)!=0:
            self.dbarc.Commit()            
    
    def process(self):
        """
        Main loop
        """        
        try:
            self.log.info("Running")
            # Check for jobs that panda told us to kill and cancel them in ARC
            self.checkJobstoKill()
            # Check status of arcjobs
            # Query jobs that were submitted since last time
            self.updateStartingJobs()
            # Query jobs in running arcstate with tarcstate sooner than last run
            self.updateRunningJobs()
            # Query jobs in arcstate done and update pandajobs
            # Set to toclean
            self.updateFinishedJobs()
            # Query jobs in arcstate failed, set to tofetch
            # Query jobs in arcstate donefailed, cancelled and lost, set to toclean.
            # If they should be resubmitted, set arcjobid to null in pandajobs
            # If not do post-processing and fill status in pandajobs
            self.updateFailedJobs()
            
        except aCTSignal.ExceptInterrupt,x:
            print x
            return

        
if __name__ == '__main__':
    aas=aCTATLASStatus()
    aas.run()
