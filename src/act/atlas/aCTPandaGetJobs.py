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


        
class aCTPandaGetJobs(aCTATLASProcess):

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

            # Limit number of jobs waiting submission to avoid getting too many
            # jobs from Panda 
            if nsubmitting > int(self.conf.get(["panda","minjobs"])) :
                self.log.info("Site %s: at limit of sent jobs" % site)
                continue

            # Get total number of active jobs
            nall = self.dbpanda.getNJobs("siteName='%s' and actpandastatus!='done' \
                                          and actpandastatus!='donefailed' and actpandastatus!='donecancelled'" % site)
            self.log.info("Site %s: %i jobs in sent, %i total" % (site, nsubmitting, nall))

            # Limit number of jobs waiting submission to avoid getting too many
            # jobs from Panda 
            #if nsubmitting > int(self.conf.get(["panda","minjobs"])) :
            #    self.log.info("Site %s: at limit of sent jobs" % site)
            #    continue
            
            if self.sites[site]['maxjobs'] == 0:
                self.log.info("Site %s: accepting new jobs disabled" % site)
                continue
            
            if nall >= self.sites[site]['maxjobs']:
                self.log.info("Site %s: at or above max job limit of %d" % (site, self.sites[site]['maxjobs']))
                continue

            nthreads = min(int(self.conf.get(['panda','threads'])), self.sites[site]['maxjobs'] - nall) 
            #nthreads = min(4, self.sites[site]['maxjobs'] - nall) 

            # if no jobs available
            stopflag=False
       
            for nc in range(0,max(int(num/nthreads),1)):
                if stopflag:
                    continue

                tlist=[]

                for i in range(0,nthreads):
                    if attrs['type'] == "analysis":
                        r=random.Random()
                        if r.randint(0,100) <= 2:
                          t=PandaGetThr(self.getPanda(site).getJob,site,'rc_test')
                        else:
                          t=PandaGetThr(self.getPanda(site).getJob,site,'user')
                    else:
                        r=random.Random()
                        if r.randint(0,100) <= 2:
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



    def process(self):
        """
        Method called from loop
        """
        self.setSites()

        # request new jobs
        num=self.getJobs(int(self.conf.get(['panda','getjobs'])))
        if num:
            self.log.info("Got %i jobs" % num)
        
if __name__ == '__main__':
    am=aCTPandaGetJobs()
    am.run()
    #am.finish()
