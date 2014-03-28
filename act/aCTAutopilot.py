from threading import Thread
import pickle
import re
import time
import arc
import aCTPanda
import aCTProxy
import aCTUtils
from aCTATLASProcess import aCTATLASProcess

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


        
class aCTAutopilot(aCTATLASProcess):

    """
    Main class for Panda interaction. Three major functions: init, run, finish
    """

    def __init__(self):
        aCTATLASProcess.__init__(self)
        
        # Get DN from configured proxy file
        uc = arc.UserConfig()
        uc.ProxyPath(str(self.arcconf.get(['voms', 'proxypath'])))
        cred = arc.Credential(uc)
        dn = cred.GetIdentityName()
        self.log.info("Running under DN %s" % dn)

        # Keep a panda object per proxy. The site "type" maps to a specific
        # proxy role
        self.pandas = {}
        # Map the site type to a proxy id in proxies table
        # In future for analysis the id will change once the job is picked up
        self.proxymap = {}
        
        actp = aCTProxy.aCTProxy(self.log)
        for role in self.arcconf.getList(['voms', 'roles', 'item']):
            attr = '/atlas/Role='+role
            proxyid = actp.getProxyId(dn, attr)
            if not proxyid:
                raise Exception("Proxy with DN "+dn+" and attribute "+attr+" was not found in proxies table")

            proxyfile = actp.path(dn, attribute=attr)
            # pilot role is mapped to analysis type
            if role == 'pilot':
                role = 'analysis'
            self.pandas[role] = aCTPanda.aCTPanda(self.log, proxyfile)
            self.proxymap[role] = proxyid
            
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
            self.sites[sitename]['type'] = self.conf.getListCond(["sites","site"],"name=" + sitename ,["type"])[0]
            try:
                self.sites[sitename]['maxjobs'] = int(self.conf.getListCond(["sites","site"],"name=" + sitename ,["maxjobs"])[0])
            except:
                self.sites[sitename]['maxjobs'] = 1000000
        print self.sites


    def getPanda(self, sitename):
        return self.pandas[self.sites[sitename]['type']]


    def updatePandaHeartbeat(self,pstatus):
        """
        Heartbeat status updates.
        """
        nthreads=int(self.conf.get(["panda","threads"]))
        columns = ['pandaid', 'siteName', 'startTime', 'endTime', 'computingElement', 'node']
        jobs=self.dbpanda.getJobs("pandastatus='"+pstatus+"' and "+self.dbpanda.timeStampLessThan("theartbeat", self.conf.get(['panda','heartbeattime'])), columns)
        if not jobs:
            return
        
        self.log.info("Update heartbeat for %d jobs in state %s" % (len(jobs), pstatus))

        if pstatus == 'sent':
            pstatus = 'starting'
        tlist=[]
        for j in jobs:
            jd = {}
            jd['startTime'] = j['startTime']
            jd['endTime'] = j['endTime']
            jd['computingElement'] = j['computingElement']
            jd['node'] = j['node']
            t=PandaThr(self.getPanda(j['siteName']).updateStatus,j['pandaid'],pstatus,jd)
            tlist.append(t)
        aCTUtils.RunThreadsSplit(tlist,nthreads)
        
        for t in tlist:
            if t.status == None:
                continue
            self.log.debug(t.status)
            if t.status['command'][0] != "NULL":
                self.log.info("response: %s %s" % (t.id,t.status) )
            jd={}
            jd['pandastatus']=pstatus
            jd['theartbeat']=self.dbpanda.getTimeStamp()
            # If panda tells us to kill the job, set actpandastatus to tobekilled
            # and remove from heartbeats
            if (t.status['command'][0] == "tobekilled") or (t.status['command'][0] == "badattemptnr"):
                jd['actpandastatus']="tobekilled"
                jd['pandastatus']=None
            self.dbpanda.updateJob(t.id,jd)

        self.log.info("Threads finished")


    def updatePandaFinishedPilot(self):
        """
        Final status update for completed jobs (finished or failed in athena)
        """
        nthreads=int(self.conf.get(["panda","threads"]))
        jobs=self.dbpanda.getJobs("actpandastatus='finished' or actpandastatus='failed'");

        if not jobs:
            return
        
        self.log.info("Updating panda for %d finished jobs" % len(jobs))
        
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
                self.log.error('%s: %s' % (j['pandaid'], x))
                # TODO push back to download (tofinished)
                continue

            t=PandaThr(self.getPanda(j['siteName']).updateStatus,j['pandaid'],j['pandastatus'],jd)
            tlist.append(t)
        aCTUtils.RunThreadsSplit(tlist,nthreads)

        for t in tlist:
            if t.status == None:
                continue
            jd={}
            jd['pandastatus']=None
            jd['actpandastatus']='done'
            jd['theartbeat']=self.dbpanda.getTimeStamp()
            self.dbpanda.updateJob(t.id,jd)

        self.log.info("Threads finished")


    def getJobs(self,num):

        """
        Get at most num panda jobs from panda server. Store fetched jobs in database.
        """
       
        if num == 0:
            return 0

        count=0

        for site, attrs in self.sites.iteritems():        

            # Get number of jobs injected into ARC but not yet submitted
            nsubmitting = self.dbpanda.getNJobs("actpandastatus='sent' and siteName='%s'" %  site )
            # Get total number of active jobs
            nall = self.dbpanda.getNJobs("siteName='%s' and actpandastatus!='done' and actpandastatus!='cancelled'" % site)
            self.log.info("Site %s: %i jobs in sent, %i total" % (site, nsubmitting, nall))

            # Limit number of jobs waiting submission to avoid getting too many
            # jobs from Panda 
            if nsubmitting > int(self.conf.get(["panda","minjobs"])) :
                self.log.info("Site %s: at limit of sent jobs" % site)
                continue
            
            if self.sites[site]['maxjobs'] == 0:
                self.log.info("Site %s: accepting new jobs disabled" % site)
                continue
            
            if nall >= self.sites[site]['maxjobs']:
                self.log.info("Site %s: at or above max job limit of %d" % (site, self.sites[site]['maxjobs']))
                continue

            nthreads=int(self.conf.get(['panda','threads']))

            # if no jobs available
            stopflag=False
       
            for nc in range(0,max(int(num/nthreads),1)):
                if stopflag:
                    continue

                tlist=[]

                for i in range(0,nthreads):
                    t=PandaGetThr(self.getPanda(site).getJob,site)
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
                    n['actpandastatus'] = 'sent'
                    n['siteName']=site
                    n['proxyid']=self.proxymap[attrs['type']]
                    self.dbpanda.insertJob(pandaid,pandajob,n)
                    count+=1
        return count


    def checkJobs(self):

        """
        Sanity checks when restarting aCT. Check for nonexistent jobs... TODO
        """
        
        # Does it matter which proxy is used? Assume no
        pjobs=self.pandas.values()[0].queryJobInfo()

        # panda error if [] possible
        if len(pjobs) == 0:
            self.log.info('No panda jobs found')
            return
        
        pjids=[]
        for j in pjobs:
            if j['jobStatus'] == 'sent' or j['jobStatus'] == 'running' or j['jobStatus'] == 'transferring' or j['jobStatus'] == 'starting' :
                pjids.append(j['PandaID'])
        self.log.info("%d" % len(pjids))

        # try to recover lost jobs (jobs in aCT but not in Panda)

        jobs=self.dbpanda.getJobs("pandastatus like '%'")

        for j in jobs:
            self.log.info("%d" % j['pandaid'])
            if j['pandaid'] in pjids:
                pass
            else:
                self.log.info("%d not in panda, cancel and remove from aCT", j['pandaid'])
                jd={}
                jd['pandastatus'] = None
                jd['actpandastatus']='tobekilled'
                self.dbpanda.updateJob(j['pandaid'],jd)

        # check db for jobs in Panda but not in aCT
        count=0
        for j in pjobs:
            self.log.debug("checking job %d" % j['PandaID'])
            job=self.dbpanda.getJob(j['PandaID'])
            if job is None and ( j['pandastatus'] == 'running' or j['pandastatus'] == 'transferring' or j['pandastatus'] == 'starting') :
                self.log.info("Missing: %d" % j['PandaID'])
                count+=1
                self.getPanda().updateStatus(j['PandaID'],'failed')
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
            r=self.dbpanda.getSchedconfig(cluster)
            if r is None:
                self.dbpanda.insertSchedconfig(cluster,status)
            else:
                self.dbpanda.updateSchedconfig(cluster,status)


    def process(self):
        """
        Method called from loop
        """
        self.setSites()

        # request new jobs
        num=self.getJobs(int(self.conf.get(['panda','getjobs'])))
        if num:
            self.log.info("Got %i jobs" % num)
        
        # Update all jobs currently in the system
        self.updatePandaHeartbeat('sent')
        self.updatePandaHeartbeat('starting')
        self.updatePandaHeartbeat('running')
        self.updatePandaHeartbeat('transferring')
        
        # Update jobs which finished
        self.updatePandaFinishedPilot()

        
if __name__ == '__main__':
    am=aCTAutopilot()
    am.run()
    #am.finish()
