import ssl
import os
import subprocess
import time
import datetime
import aCTPanda
import aCTDBPanda
import sys
import aCTConfig
import aCTProxy
import aCTLogger
import sys
from threading import Thread
import aCTSignal
import aCTUtils
import pickle
import re


class PandaThr(Thread):
    """
    Helper function for threaded panda status update calls.
    func is generic, but it is only used for aCTPanda.updateStatus call.
    """
    def __init__ (self,func,id,status,args={}):
        Thread.__init__(self)
        self.func=func
        self.id = id
        self.status = status
        self.args = args
    def run(self):
        self.status=self.func(self.id,self.status,self.args)

class PandaGetThr(Thread):
    """
    Similar to previous but for aCTPanda.getJob
    """
    def __init__ (self,func,siteName,prodSourceLabel=None):
        Thread.__init__(self)
        self.func=func
        self.siteName=siteName
        self.prodSourceLabel=prodSourceLabel
        self.status = (None,None)
    def run(self):
        self.status=self.func(self.siteName,self.prodSourceLabel)


        
class aCTAutopilot:

    """
    Main class for Panda interaction. Three major functions: init, run, finish
    """

    def __init__(self):

        # xml config file
        self.conf=aCTConfig.aCTConfigATLAS()
        self.arcconf=aCTConfig.aCTConfigARC()
        # logger
        # Get agent name from /path/to/aCTAgent.py
        self.name = os.path.basename(sys.argv[0])[:-3]
        self.logger=aCTLogger.aCTLogger(self.name)
        self.log=self.logger()
        self.log.info("Start")

        # database
        self.db=aCTDBPanda.aCTDBPanda(self.log,self.arcconf.get(["db","file"]))
        # panda
        # proxy path?????
        self.panda=aCTPanda.aCTPanda(self.log)

        # queue interval
        self.queuestamp=0

        self.sites={}

    def getEndTime(self):
        return time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime())


    def setSites(self):
        for sitename in self.conf.getList(["sites","site","name"]):
            self.sites[sitename] = {}
            self.sites[sitename]['endpoints'] = self.conf.getListCond(["sites","site"],"name=" + sitename ,["endpoints","item"])
            self.sites[sitename]['schedconfig'] = self.conf.getListCond(["sites","site"],"name=" + sitename ,["schedconfig"])[0]

    def updatePandaInitStatus(self,trfstatus='topandainit'):
        """
        Send running status for jobs which have not yet been submitted
        to avoid possible long delay. sent->running toggle must be done
        within 30 minutes.
        """
        nthreads=int(self.conf.get(["panda","threads"]))
        jobs=self.db.getJobs("pstatus='sent' and trfstatus='%s'" % trfstatus)

        if len(jobs):
            self.log.info("%d" % len(jobs))

        tlist=[]
        for j in jobs:
            jd={}
            #jd['endTime']=self.getEndTime()
            t=PandaThr(self.panda.updateStatus,j['pandaid'],'starting',jd)
            tlist.append(t)
        aCTUtils.RunThreadsSplit(tlist,nthreads)
        for t in tlist:
            if t.status == None:
                continue
            #self.log.info("response: %s" % t.status )
            jd={}
            jd['trfstatus']='tolfclfns'
            jd['theartbeat']=self.db.getTimeStamp()
            self.db.updateJob(t.id,jd)
          
        #self.db.Commit()
        if len(jobs):
            self.log.info("Threads finished")



    def updatePandaSubmitted(self,trfstatus='inarc'):
        """
        status update for submitted jobs. computingElement is known and sent
        """
        nthreads=int(self.conf.get(["panda","threads"]))
        jobs=self.db.getJobs("pstatus='sent' and trfstatus='%s'" % trfstatus)

        if len(jobs):
            self.log.info("%d" % len(jobs))

        tlist=[]
        for j in jobs:
            jd={}
            #jd['endTime']=self.getEndTime()
            try:
                reg=re.search('.+//([^:]+)',str(j['arcjobid']))
                cluster=reg.group(1)
                jd['computingElement']=cluster
            except:
                pass
            t=PandaThr(self.panda.updateStatus,j['pandaid'],'starting',jd)
            tlist.append(t)
        aCTUtils.RunThreadsSplit(tlist,nthreads)
        #self.db.Commit()
        for t in tlist:
            if t.status == None:
                continue
            jd={}
            jd['pstatus']='starting'
            jd['theartbeat']=self.db.getTimeStamp()
            self.db.updateJob(t.id,jd)
          
        if len(jobs):
            self.log.info("Threads finished")



    def updatePandaHeartbeat(self,pstatus):
        """
        Heartbeat status updates.
        """
        nthreads=int(self.conf.get(["panda","threads"]))
        jobs=self.db.getJobs("pandastatus='"+pstatus+"' and "+self.db.timeStampLessThan("theartbeat", self.conf.get(['panda','heartbeattime'])))
        #print "PandaHeartbeat ",len(jobs)
        if len(jobs):
            self.log.info("%d" % len(jobs))
        tlist=[]
        for j in jobs:
            jd={}
            if pstatus == 'transferring':
                jd['endTime']=self.getEndTime()
            if pstatus == 'sent':
                t=PandaThr(self.panda.updateStatus,j['pandaid'],'starting',jd)
            else:
                t=PandaThr(self.panda.updateStatus,j['pandaid'],pstatus,jd)
            tlist.append(t)
        aCTUtils.RunThreadsSplit(tlist,nthreads)
        for t in tlist:
            if t.status == None:
                continue
            if t.status['command'][0] != "NULL":
                self.log.info("response: %s %s" % (t.id,t.status) )
            #AF   pstatus = t.status['command'][0] 
            jd={}
            if pstatus == 'sent':
                jd['pandastatus']='starting'
            else:
                jd['pandastatus']=pstatus
            jd['theartbeat']=self.db.getTimeStamp()
            if (t.status['command'][0] == "tobekilled"):
                jd['pandastatus']="tobekilled"
            if (t.status['command'][0] == "badattemptnr"):
                jd['pandastatus']="tobekilled"
            self.db.updateJob(t.id,jd)
          
        #self.db.Commit()
        if len(jobs):
            self.log.info("Threads finished")


    def updatePandaFinishedPilot(self):
        """
        Final status update for completed jobs (finished or failed in athena)
        """
        nthreads=int(self.conf.get(["panda","threads"]))
        jobs=self.db.getJobs("trfstatus='topanda'");

        if len(jobs):
            self.log.info("%d" % len(jobs))
        
        tlist=[]
        for j in jobs:

            jd={}
            try:
                # pilot status update is stored in pickle format
                fname=self.conf.get(['tmp','dir'])+"/pickle/"+str(j['pandaid'])+".pickle"
                f=open(fname,"r")
                jd=pickle.load(f)
                f.close()
            except Exception,x:
                self.log.error(x)
                # TODO push back to download (tofinished)
                continue

            t=PandaThr(self.panda.updateStatus,j['pandaid'],j['pstatus'],jd)
            tlist.append(t)
        aCTUtils.RunThreadsSplit(tlist,nthreads)

        for t in tlist:
            if t.status == None:
                continue
            jd={}
            jd['trfstatus']='toremove'
            jd['pstatus']='done'
            jd['theartbeat']=self.db.getTimeStamp()
            self.db.updateJob(t.id,jd)
           
        #self.db.Commit()
        if len(jobs):
            self.log.info("Threads finished")


    def getJobs(self,num):

        """
        Get at most num panda jobs from panda server. Store fetched jobs in database.
        """
       
        count=0
        if num == 0:
            return count

        # check max running
        # jobs=self.db.getJobs("pstatus like '%'")

        for site in self.sites.keys():        

            nsubmitting = self.db.getNJobs("pandastatus like 'sent' and pandastatus like 'submitting' and siteName='%s'" %  site )
            nall = self.db.getNJobs("siteName='%s'" % site)
            print site,nsubmitting,nall

            if nsubmitting > int(self.conf.get(["panda","minjobs"])) :
                continue

            nthreads=int(self.conf.get(['panda','threads']))

            # if no jobs available
            stopflag=False
       
            for nc in range(0,max(int(num/nthreads),1)):
                if stopflag:
                    continue

                tlist=[]

                for i in range(0,nthreads):
                    t=PandaGetThr(self.panda.getJob,site)
                    tlist.append(t)
                    t.start()
                for t in tlist:
                    t.join()
                    (pandaid,pandajob)=t.status
                    print t.status
                    if pandaid == None:
                        stopflag=True
                        continue
                    n={}

                    n['pandastatus']='sent'
                    n['siteName']=site
                    self.db.insertJob(pandaid,pandajob,n)
                    count+=1
        return count




    def checkJobs(self):

        """
        Sanity checks when restarting aCT. Check for nonexistent jobs... TODO
        """
        
        pjobs=self.panda.queryJobInfo()

        # panda error if [] possible
        if len(pjobs) == 0:
            self.log.info('No panda jobs found')
            return
        
        pjids=[]
        for j in pjobs:
            if j['jobStatus'] == 'sent' or j['jobStatus'] == 'running' or j['jobStatus'] == 'transferring' or j['jobStatus'] == 'starting' :
                pjids.append(j['PandaID'])
        self.log.info("%d" % len(pjids))

        # try to recover lost jobs:

        jobs=self.db.getJobs("pstatus like '%'")

        for j in jobs:
            self.log.info("%d" % j['pandaid'])
            if j['pandaid'] in pjids:
                pass
            else:
                self.log.info("not in panda -> remove")
                jd={}
                jd['trfstatus']='toremove'
                self.db.updateJob(j['pandaid'],jd)

        # return 
        # check db (something is wrong with activated)
        count=0
        for j in pjobs:
            self.log.debug("checking job %d" % j['PandaID'])
            job=self.db.getJob(j['PandaID'])
            if job is None and ( j['jobStatus'] == 'running' or j['jobStatus'] == 'transferring' or j['jobStatus'] == 'starting') :
                self.log.info("Missing: %d" % j['PandaID'])
                count+=1
                self.panda.updateStatus(j['PandaID'],'failed')
        self.log.info("missing jobs: %d removed" % count)
            
    def checkQueues(self):
        """
        get the queue status from panda and store in db
        """

        if time.time()-self.queuestamp < int(self.conf.get(["panda","schedinterval"])) :
            return
        else:
            self.queuestamp=time.time()

        for q in self.conf.getList(["panda","queues","item"]):
            res = re.match("ARC-(.+)",q)
            cluster = res.group(1)
            status = self.panda.getQueueStatus(cluster)
            r=self.db.getSchedconfig(cluster)
            if r is None:
                self.db.insertSchedconfig(cluster,status)
            else:
                self.db.updateSchedconfig(cluster,status)

    def run(self):
        """
        Main loop
        """

        
        try:
            self.log.info("Running")

            while 1:
                self.conf.parse()
                self.setSites()

                # request new jobs
                num=self.getJobs(int(self.conf.get(['panda','getjobs'])))
                if num:
                    self.log.info("GetJobs: %s" % str(num))

                self.updatePandaHeartbeat('sent')
                self.updatePandaHeartbeat('starting')

                aCTUtils.sleep(100000)

        except aCTSignal.ExceptInterrupt,x:
            print x
            return

    def runold(self):
        """
        Main loop
        """
        try:
            self.log.info("Running")
            ###AF self.checkJobs()

            while 1:
                # try to reparse config file if it was modified. 
                self.conf.parse()
                # request new jobs
                num=self.getJobs(int(self.conf.get(['panda','getjobs'])))
                if num:

                    self.log.info("GetJobs: %s" % str(num))
                # panda updates
                self.updatePandaInitStatus()
                self.updatePandaSubmitted()
                self.updatePandaHeartbeat('sent')
                self.updatePandaHeartbeat('starting')
                self.updatePandaHeartbeat('running')
                self.updatePandaHeartbeat('transferring')
                self.updatePandaFinishedPilot()
                ### self.checkQueues()
                # 
                aCTUtils.sleep(10)

        except aCTSignal.ExceptInterrupt,x:
            print x
            return
            

    def finish(self):
        """
        clean finish handled by signals
        """
        self.log.info("Cleanup")


        
if __name__ == '__main__':
    am=aCTAutopilot()
    am.run()
    #am.finish()
