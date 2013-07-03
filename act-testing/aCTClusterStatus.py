import arc
import aCTDB

import aCTConfig
import aCTLogger

from datetime import datetime as dt
from datetime import timedelta
import sys
import time
import os

cluster = sys.argv[1]

class aCTClusterStatus:

    def __init__(self,cluster):
        self.cluster=cluster
        self.logger=aCTLogger.aCTLogger("clusterstatus")
        self.log=self.logger()

        # config
        self.conf=aCTConfig.aCTConfig()

        self.updateinterval=int(self.conf.get(["jobs","updateinterval"]))

        self.db=aCTDB.aCTDB("")

        self.usercfg = arc.UserConfig('', '')
        #self.usercfg.ClearSelectedServices()
        self.usercfg.ProxyPath("/tmp/x509up_u%s" % os.getuid())
        self.usercfg.CACertificatesDirectory("/etc/grid-security/certificates")

        #self.clusters=arc.StringList()
        #self.clusters.append("ARC0:ldap://"+self.cluster+":2135/nordugrid-cluster-name="+self.cluster+",Mds-Vo-name=local,o=grid")

        #self.usercfg.AddServices(self.clusters, arc.COMPUTING)
        #self.supervisor = None


    def updateArcJob(self,id,job,aj):
        jb={}

        jb['usedcputime']=job.UsedTotalCPUTime.GetPeriod()
        jb['usedwalltime']=job.UsedTotalWallTime.GetPeriod()
        jb['usedmainmemory']=job.UsedMainMemory
        jb['submissiontime']=dt.fromtimestamp(job.SubmissionTime.GetTime())
        jb['completiontime']=dt.fromtimestamp(job.ComputingManagerEndTime.GetTime())
        jb['exitcode']=job.ExitCode;
        jb['starttime']=dt.fromtimestamp(job.StartTime.GetTime())
        jb['endtime']=dt.fromtimestamp(job.EndTime.GetTime())
        jb['proxyexpirationtime']=dt.fromtimestamp(job.ProxyExpirationTime.GetTime())
        jb['restartstate']=job.RestartState.GetGeneralState()
        jb['generalstate']=job.State.GetGeneralState()
        jb['state']=job.State()
        jb['proxyexpirationtime']=dt.fromtimestamp(job.ProxyExpirationTime.GetTime())
        jb['owner']=job.Owner
        jb['queue']=job.Queue

        if job.State() != aj['state']:
            jb['tstate']=dt.fromtimestamp(time.time())
        #jb['modified']=dt.fromtimestamp(time.time())

        #en=""
        #for i in range(0,job.Error.size()):
        #for i in job.Error:
        #    #en+=job.Error[i]
        #    en+=i
        jb['error']="\n",join(job.Error)
    
        #en=""
        #for i in range(0,job.ExecutionNode.size()):
        #for i in job.ExecutionNode:
        #    #en+=job.ExecutionNode[i]
        #    en+=i
        jb['executionnodes']="\n".join(job.ExecutionNode)
    
        try:
            self.db.updateArcJob(id,jb)
        except Exception,e:
            self.log.info("e")

    def checkJobs(self):


        targen=arc.TargetGenerator(self.usercfg)
        targen.RetrieveJobs()

        supervisor = arc.JobSupervisor(self.usercfg, targen.GetJobs())
        jobcont = supervisor.GetJobControllers()

        try:
            jobcont[0].GetJobInformation()
        except:
            return 0

        self.log.info ("cluster %s jobs %d" % (self.cluster,len(jobcont[0].GetJobs())))

        count=0

        str="select jobid from arcjobs where jobid like '%%%s%%'" % self.cluster
        jbs=self.db.Execute(str);

        jobs=[]
        for j in jbs:
            jobs.append(j['jobid'])

        for job in jobcont[0].GetJobs():
            if job.JobID.str() not in jobs:
                next
            try:
                aj=self.db.getArcJobs("jobid='%s'" % job.JobID.str())[0]
                if (job.State() != aj['state'] ) or \
                 (dt.fromtimestamp(time.time())-aj['modified'] > timedelta(0,self.updateinterval) ) :
                    self.updateArcJob(aj['id'],job,aj)
                    count+=1
            except Exception,e:
                pass

        self.log.info("cluster %s updated %d jobs" % (self.cluster,count))

    def run(self):
        import time
        while 1:
            self.checkJobs()
            time.sleep(10)
        
actstatus=aCTClusterStatus(cluster)
actstatus.run()
