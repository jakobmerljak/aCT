from threading import Thread
import datetime
import os
import pickle
import re
import time
import random
import arc
import aCTPanda
from act.common import aCTProxy
from act.common import aCTUtils
from aCTATLASProcess import aCTATLASProcess
from aCTAGISParser import aCTAGISParser
from aCTPandaJob import aCTPandaJob

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
        self.result = None
    def run(self):
        self.result=self.func(self.id,self.status,self.args)

class PandaGetThr(Thread):
    """
    Similar to previous but for aCTPanda.getJob
    """
    def __init__ (self,func,siteName,prodSourceLabel=None):
        Thread.__init__(self)
        self.func=func
        self.siteName=siteName
        self.prodSourceLabel=prodSourceLabel
        self.result = (None,None)
    def run(self):
        self.result=self.func(self.siteName,self.prodSourceLabel)


        
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
        self.agisparser = aCTAGISParser(self.log)
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
        self.sites = self.agisparser.getSites()


    def getPanda(self, sitename):
        return self.pandas[self.sites[sitename]['type']]


    def updatePandaHeartbeat(self,pstatus):
        """
        Heartbeat status updates.
        """
        nthreads=int(self.conf.get(["panda","threads"]))
        # Check if we should send heartbeats
        hb = ''
        if pstatus == 'running' or pstatus == 'transferring':
            hb = ' and sendhb=1'
        columns = ['pandaid', 'siteName', 'startTime', 'endTime', 'computingElement', 'node']
        jobs=self.dbpanda.getJobs("pandastatus='"+pstatus+"'"+hb+" and ("+self.dbpanda.timeStampLessThan("theartbeat", self.conf.get(['panda','heartbeattime']))+" or modified > theartbeat) limit 1000", columns)
        if not jobs:
            return
        
        self.log.info("Update heartbeat for %d jobs in state %s (%s)" % (len(jobs), pstatus, ','.join([str(j['pandaid']) for j in jobs]))) 

        changed_pstatus = False
        if pstatus == 'sent':
            pstatus = 'starting'
            changed_pstatus = True
        tlist=[]
        for j in jobs:
            jd = {}
            jd['startTime'] = j['startTime']
            jd['endTime'] = j['endTime']
            jd['computingElement'] = j['computingElement']
            jd['node'] = j['node']
            jd['siteName'] = j['siteName']
            t=PandaThr(self.getPanda(j['siteName']).updateStatus,j['pandaid'],pstatus,jd)
            tlist.append(t)
        aCTUtils.RunThreadsSplit(tlist,nthreads)
        
        for t in tlist:
            if t.result == None:
                continue
            if t.result['StatusCode'] and t.result['StatusCode'][0] == '60':
                self.log.error('Failed to contact Panda, proxy may have expired')
                continue
            self.log.debug('%s: %s' % (t.id, t.result))
            if t.result.has_key('command')  and t.result['command'][0] != "NULL":
                self.log.info("%s: response: %s" % (t.id,t.result) )
            jd={}
            if changed_pstatus:
                jd['pandastatus']=pstatus
            # Make sure heartbeat is ahead of modified time so it is not picked up again
            if self.sites[t.args['siteName']]['truepilot'] and pstatus == 'starting':
                # Set theartbeat 1h in the future to allow job to start
                # running and avoid race conditions with heartbeats
                jd['theartbeat'] = self.dbpanda.getTimeStamp(time.time()+3600)
            else:
                jd['theartbeat'] = self.dbpanda.getTimeStamp(time.time()+1)
            # If panda tells us to kill the job, set actpandastatus to tobekilled
            # and remove from heartbeats
            if t.result.has_key('command') and ( (t.result['command'][0] == "tobekilled") or (t.result['command'][0] == "badattemptnr") ):
                self.log.info('%s: cancelled by panda' % t.id)
                jd['actpandastatus']="tobekilled"
                jd['pandastatus']=None
            self.dbpanda.updateJob(t.id,jd)

        self.log.info("Threads finished")


    def updatePandaFinishedPilot(self):
        """
        Final status update for completed jobs (finished or failed in athena)
        and cancelled jobs
        """
        nthreads=int(self.conf.get(["panda","threads"]))
        jobs=self.dbpanda.getJobs("actpandastatus='finished' or actpandastatus='failed' or actpandastatus='cancelled' limit 1000");

        if not jobs:
            return
        
        self.log.info("Updating panda for %d finished jobs (%s)" % (len(jobs), ','.join([str(j['pandaid']) for j in jobs]))) 
        
        tlist=[]
        for j in jobs:

            # If true pilot skip heartbeat and just update DB
            if not j['sendhb']:
                jd={}
                jd['pandastatus']=None
                jd['actpandastatus']='done'
                if j['actpandastatus'] == 'failed':
                    jd['actpandastatus']='donefailed'
                if j['actpandastatus'] == 'cancelled':
                    jd['actpandastatus']='donecancelled'
                if not j['startTime']:
                    jd['startTime'] = datetime.datetime.utcnow()
                if not j['endTime']:
                    jd['endTime'] = datetime.datetime.utcnow()
                self.dbpanda.updateJob(j['pandaid'], jd)
                continue
            
            # Cancelled jobs have no pickle info
            if j['actpandastatus'] == 'cancelled':
                jobinfo = aCTPandaJob(jobinfo = {'jobId': j['pandaid'], 'state': 'failed'})
                jobinfo.pilotErrorCode = 1144
                jobinfo.pilotErrorDiag = "This job was killed by panda server"
                if j['startTime']:
                    jobinfo.startTime = j['startTime']
                else:
                    jobinfo.startTime = datetime.datetime.utcnow()
                jobinfo.endTime = datetime.datetime.utcnow()
            else:
                try:
                    # Load pickled information from pilot
                    fname = self.conf.get(['tmp','dir'])+"/pickle/"+str(j['pandaid'])+".pickle"
                    jobinfo = aCTPandaJob(filename=fname)
                except Exception,x:
                    self.log.error('%s: %s' % (j['pandaid'], x))
                    # Send some basic info back to panda
                    info = {'jobId': j['pandaid'], 'state': j['pandastatus']} 
                    jobinfo = aCTPandaJob(jobinfo=info)
                    jobinfo.pilotErrorCode = 1008
                    jobinfo.pilotErrorDiag = 'Job failed for unknown reason'
                else:
                    os.remove(fname)

            t=PandaThr(self.getPanda(j['siteName']).updateStatus,j['pandaid'],j['pandastatus'],jobinfo.dictionary())
            tlist.append(t)
        
        aCTUtils.RunThreadsSplit(tlist,nthreads)

        for t in tlist:
            if t.result == None:
                continue
            if t.result['StatusCode'] and t.result['StatusCode'][0] != '0':
                self.log.error('Error updating panda')
                continue
            jd={}
            jd['pandastatus']=None
            jd['actpandastatus']='done'
            if t.status == 'failed':
                jd['actpandastatus']='donefailed'
            if 'pilotErrorCode' in t.args and t.args['pilotErrorCode'] == 1144:
                jd['actpandastatus']='donecancelled'
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
            if not attrs['enabled']:
                continue        

            if attrs['status'] == 'offline':
                self.log.info("Site %s is offline, will not fetch new jobs" % site)
                continue

            # Get number of jobs injected into ARC but not yet submitted
            nsubmitting = self.dbpanda.getNJobs("actpandastatus='sent' and siteName='%s'" %  site )
            # Get total number of active jobs
            nall = self.dbpanda.getNJobs("siteName='%s' and actpandastatus!='done' \
                                          and actpandastatus!='donefailed' and actpandastatus!='cancelled'" % site)
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

            nthreads = min(int(self.conf.get(['panda','threads'])), self.sites[site]['maxjobs'] - nall) 

            # if no jobs available
            stopflag=False
       
            for nc in range(0,max(int(num/nthreads),1)):
                if stopflag:
                    continue

                tlist=[]

                for i in range(0,nthreads):
                    if attrs['type'] == "analysis":
                        r=random.Random()
                        if r.randint(0,100) <= 10:
                          t=PandaGetThr(self.getPanda(site).getJob,site,'rc_test')
                        else:
                          t=PandaGetThr(self.getPanda(site).getJob,site,'user')
                    else:
                        r=random.Random()
                        if r.randint(0,100) <= 10:
                          t=PandaGetThr(self.getPanda(site).getJob,site,'rc_test')
                        else:
                          t=PandaGetThr(self.getPanda(site).getJob,site)
                    tlist.append(t)
                    t.start()
                    nall += 1
                    if nall >= self.sites[site]['maxjobs']:
                        self.log.info("Site %s: reached max job limit of %d" % (site, self.sites[site]['maxjobs']))
                        stopflag = True
                        break
                    
                for t in tlist:
                    t.join()
                    (pandaid,pandajob)=t.result
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


    def updateArchive(self):
        """
        Move old jobs older than 1 day to archive table
        """
        
        # modified column is reported in local time so may not be exactly one day
        select = self.dbpanda.timeStampLessThan('modified', 60*60*24)
        select += ' and (actpandastatus="done" or actpandastatus="donefailed" or actpandastatus="donecancelled")'
        columns = ['pandaid', 'sitename', 'actpandastatus', 'starttime', 'endtime', 'modified']
        jobs = self.dbpanda.getJobs(select, columns)
        if not jobs:
            return
        
        self.log.info('Archiving %d jobs' % len(jobs))
        for job in jobs:
            self.log.debug('Archiving panda job %d' % job['pandaid'])
            # Fill out empty start/end time
            if job['starttime']:
                if not job['endtime']:
                    job['endtime'] = job['modified']
            elif job['endtime']:
                job['starttime'] = job['endtime']
            else:
                job['starttime'] = self.dbpanda.getTimeStamp()
                job['endtime'] = self.dbpanda.getTimeStamp()
                
            self.dbpanda.insertJobArchiveLazy(job)
            self.dbpanda.deleteJob(job['pandaid']) # commit is called here
        

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
        
        # Move old jobs to archive - every hour
        if time.time()-self.starttime > 3600:
            self.log.info("Checking for jobs to archive")
            self.updateArchive()
            self.starttime = time.time()

        
if __name__ == '__main__':
    am=aCTAutopilot()
    am.run()
    #am.finish()
