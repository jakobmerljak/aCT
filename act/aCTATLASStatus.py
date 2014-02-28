# Handler for filling pandajobs information from arcjobs information. Also
# deals with post-processing of jobs and error handling.

import time
import datetime
import pickle
import re
import os
import shutil

import aCTSignal

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
            self.dbarc.updateArcJob(job['arcjobid'], {'arcstate': 'tocancel'})
        
        self.dbpanda.updateJobs("actpandastatus='tobekilled'", {'actpandastatus': 'cancelled'})
           
    def getStartTime(self, endtime, walltime):
        """
        Get starttime from endtime-walltime where endtime is datetime.datetime and walltime is in seconds
        """
        return endtime-datetime.timedelta(0, walltime)
           
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
            desc["computingElement"] = aj["cluster"]
            desc["startTime"] = self.getStartTime(datetime.datetime.utcnow(), aj['UsedTotalWalltime'])
            self.dbpanda.updateJobsLazy(select, desc)
        if len(jobstoupdate)>0:
            self.dbpanda.Commit()

        
    def updateFinishedJobs(self):
        """
        Check for new finished jobs, update pandajobs with
        - pandastatus
        - startTime
        - endTime
        """
        select = "arcstate='done'"
        select += " limit 100000"

        jobstoupdate=self.dbarc.getArcJobsInfo(select, columns=["id", "UsedTotalWallTime", "EndTime"])
        
        if len(jobstoupdate) == 0:
            return
        else:
            self.log.debug("Found %d finished jobs", len(jobstoupdate))


        for aj in jobstoupdate:
            select = "arcjobid='"+str(aj["id"])+"'"
            desc = {}
            desc["pandastatus"] = "running" # Will be set to finished by validator
            desc["actpandastatus"] = "tovalidate"
            desc["startTime"] = self.getStartTime(aj['EndTime'], aj['UsedTotalWallTime'])
            desc["endTime"] = aj["EndTime"]
            self.dbpanda.updateJobsLazy(select, desc)
        if len(jobstoupdate)>0:
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
                self.log.info("Resubmitting %d %s %s" % (aj['id'],aj['JobID'],aj['Error']))
                select = "arcjobid='"+str(aj["id"])
                jd={}
                jd['arcjobid'] = None
                jd['pandastatus'] = 'starting'
                # Validator processes this state before setting back to starting
                jd['actpandastatus'] = 'toresubmit'
                self.dbpanda.updateJobsLazy(select,jd)
                resubmitting=True
            else:
                failedjobs += [aj]
        if resubmitting:
            self.dbpanda.Commit()
        return failedjobs

    def processFailed(self, arcjobs):
        """
        process jobs failed for other reasons than athena (log_extracts was not created by pilot)
        """
        if len(arcjobs):
            self.log.info("%d" % len(arcjobs))
        else:
            return


        for aj in arcjobs:
            xml=""
            log=""
            cluster=aj['cluster']
            jobid=aj['JobID']
            sessionid=jobid[jobid.rfind('/'):]
            if cluster in self.conf.getList(['downtime','cluster','item']):
                continue

            try:
                os.mkdir(self.conf.get(['joblog','dir']) + "/" + cluster )
            except:
                pass
            outd = self.conf.get(['joblog','dir']) + "/" + cluster + "/" + sessionid
            try:
                shutil.rmtree(outd)
            except:
                pass
            # copy from tmp to outd.
            localdir = str(self.arcconf.get(['tmp','dir'])) + sessionid
            shutil.copytree(localdir, outd)
            
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

            print log

            xml=""
            # xml and log

            # set update, pickle from pilot is not available
            # some values might not be properly set
            # TODO synchronize error codes with the rest of production
            pupdate={}
            pupdate['xml']=str(xml)
            pupdate['siteName']='ARC'
            pupdate['computingElement']=aj['cluster']
            pupdate['schedulerID']=self.conf.get(['panda','schedulerid'])
            pupdate['pilotID']=self.conf.get(["joblog","urlprefix"])+"/"+cluster+"/"+sessionid+"|Unknown|Unknown"
            pupdate['node']=aj['ExecutionNode']
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
                    print pupdate['pilotErrorCode'],aj['Error']
            codes=[]
            codes.append("Job probably exceeded memory limit")
            codes.append("job killed: vmem")
            codes.append("pvmem exceeded")
            for errcode in codes:
                res=re.match(".*"+errcode+".*",aj['Error'])
                if res is not None:
                    pupdate['pilotErrorCode']=1212
                    print pupdate['pilotErrorCode'],aj['Error']
            pupdate['pilotErrorDiag']=aj['Error']
            # set start/endtime
            pupdate['startTime']=self.getStartTime(aj['EndTime'], aj['UsedTotalWallTime'])
            pupdate['endTime']=aj['EndTime']
            # save the pickle file to be used by aCTMain panda update
            select="arcjobid='"+str(aj["id"])+"'"
            j = self.dbpanda.getJobs(select, ["pandaid"])[0]
            try:
                os.mkdir(self.conf.get(['tmp','dir'])+"/pickle")
            except:
                pass
            f=open(self.conf.get(['tmp','dir'])+"/pickle/"+str(j['pandaid'])+".pickle","w")
            pickle.dump(pupdate,f)
            f.close()

    
    def updateFailedJobs(self):
        """
        Query jobs in arcstate failed, set to tofetch
        Query jobs in arcstate donefailed, cancelled and lost, set to toclean.
        If they should be resubmitted, set arcjobid to null in pandajobs
        If not do post-processing and fill status in pandajobs
        """
        select = "arcstate='failed'"
        desc = {"arcstate":"tofetch", "tarcstate": self.dbarc.getTimeStamp()}
        self.dbarc.updateArcJobs(desc, select)
        select = "(arcstate='donefailed' or arcstate='cancelled' or arcstate='lost')"
        select += " limit 100000"

        jobstoupdate=self.dbarc.getArcJobsInfo(select)

        if len(jobstoupdate) == 0:
            return
        else:
            self.log.debug("Found %d failed jobs", len(jobstoupdate))
        
        failedjobs = [job for job in jobstoupdate if job['arcstate']=='donefailed']
        lostjobs = [job for job in jobstoupdate if job['arcstate']=='lost']
        cancelledjobs = [job for job in jobstoupdate if job['arcstate']=='cancelled']
                
        failedjobs=self.checkFailed(failedjobs)
        # process all failed jobs that couldn't be resubmitted
        self.processFailed(failedjobs)

        for aj in failedjobs:
            select = "arcjobid='"+str(aj["id"])+"'"
            desc = {}
            desc["pandastatus"] = "failed"
            desc["actpandastatus"] = "tovalidate" # to clean up any output
            desc["endTime"] = aj["EndTime"]
            self.dbpanda.updateJobsLazy(select, desc)

        for aj in lostjobs:
            self.log.info("Resubmitting lost job %d %s %s" % (aj['id'],aj['JobID'],aj['Error']))
            select = "arcjobid='"+str(aj["id"])+"'"
            desc={}
            desc['arcjobid'] = None
            desc['pandastatus'] = 'starting'
            # Validator processes this state before setting back to starting
            desc['actpandastatus'] = 'toresubmit'
            self.dbpanda.updateJobsLazy(select,desc)

        for aj in cancelledjobs:
            select = "arcjobid='"+str(aj["id"])+"'"
            desc = {}
            # TODO: Jobs cancelled unexpectedly - report error to panda
            desc["actpandastatus"] = "cancelled"
            desc["endTime"] = aj["EndTime"]
            self.dbpanda.updateJobsLazy(select, desc)
            
        
        if len(failedjobs)+len(lostjobs)+len(cancelledjobs)!=0:
            self.dbpanda.Commit()

        # set arcjobs state toclean for cancelled jobs
        desc = {"arcstate":"toclean", "tarcstate": self.dbarc.getTimeStamp()}
        for aj in cancelledjobs:
            self.dbarc.updateArcJobLazy(aj["id"], desc)
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