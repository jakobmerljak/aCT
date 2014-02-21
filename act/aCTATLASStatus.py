# Handler for filling pandajobs information from arcjobs information. Also
# deals with post-processing of jobs and error handling.

import time
import pickle
import re
import os
import shutil

import aCTSignal

from aCTATLASProcess import aCTATLASProcess

class aCTATLASStatus(aCTATLASProcess):

    def __init__(self):
        
        aCTATLASProcess.__init__(self) 

        # store the last check time to avoid overloading of DB. 
        # should be zero first time to check all states
        self.trunning=0.    
 
    def updateRunningJobs(self):
        """
        Check for new running jobs and update pandajobs with
        - pandastatus
        - node
        - computingElement
        - startTime
        """

        select = "arcstate='running' and "+ self.dbarc.timeStampLessThan("tarcstate", self.trunning)
        select += " limit 100000"
        jobstoupdate=self.dbarc.getArcJobsInfo(select, columns=["id", "StartTime", "ExecutionNode", "cluster"])
        self.trunning=time.time()

        if len(jobstoupdate) == 0:
            return
        else:
            self.log.debug("Found %d running jobs", len(jobstoupdate))

        for aj in jobstoupdate:
            select = "arcjobid='"+str(aj["id"])+"'"
            desc = {}
            desc["pandastatus"] = "running"
            desc["node"] = aj["ExecutionNode"]
            desc["computingElement"] = aj["cluster"]
            desc["startTime"] = aj["StartTime"]
            self.dbpanda.updateJobsLazy(select, desc)
        if len(jobstoupdate)>0:
            self.dbpanda.Commit()

        
    def updateFinishedJobs(self):
        """
        Check for new finished jobs, update pandajobs with
        - pandastatus
        - endTime
        Set arcstate to toclean
        """
        select = "arcstate='done'"
        select += " limit 100000"

        jobstoupdate=self.dbarc.getArcJobsInfo(select, columns=["id", "StartTime", "EndTime"])
        
        if len(jobstoupdate) == 0:
            return
        else:
            self.log.debug("Found %d finished jobs", len(jobstoupdate))


        for aj in jobstoupdate:
            select = "arcjobid='"+str(aj["id"])+"'"
            desc = {}
            desc["pandastatus"] = "finished"
            desc["startTime"] = aj["StartTime"]
            desc["endTime"] = aj["EndTime"]
            self.dbpanda.updateJobsLazy(select, desc)
        if len(jobstoupdate)>0:
            self.dbpanda.Commit()
            
        # set arcjobs state toclean
        desc = {"arcstate":"toclean", "tarcstate": self.dbarc.getTimeStamp()}
        for aj in jobstoupdate:
            self.dbarc.updateArcJobLazy(aj["id"], desc)
        if len(jobstoupdate)!=0:
            self.dbarc.Commit()            


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
                jd['arcjobid'] = 'NULL'
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
            pupdate['startTime']=aj['StartTime']
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
            desc["endTime"] = aj["EndTime"]
            self.dbpanda.updateJobsLazy(select, desc)

        for aj in lostjobs:
            self.log.info("Resubmitting lost job %d %s %s" % (aj['id'],aj['JobID'],aj['Error']))
            select = "arcjobid='"+str(aj["id"])
            desc={}
            desc['arcjobid'] = 'NULL'
            self.dbpanda.updateJobsLazy(select,desc)

        for aj in cancelledjobs:
            select = "arcjobid='"+str(aj["id"])+"'"
            desc = {}
            desc["pandastatus"] = "cancelled"
            desc["endTime"] = aj["EndTime"]
            self.dbpanda.updateJobsLazy(select, desc)
        
        if len(failedjobs)+len(lostjobs)+len(cancelledjobs)!=0:
            self.dbpanda.Commit()

        # set arcjobs state toclean
        desc = {"arcstate":"toclean", "tarcstate": self.dbarc.getTimeStamp()}
        for aj in jobstoupdate:
            self.dbarc.updateArcJobLazy(aj["id"], desc)
        if len(jobstoupdate)!=0:
            self.dbarc.Commit()            
    
    def process(self):
        """
        Main loop
        """        
        try:
            self.log.info("Running")
    
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