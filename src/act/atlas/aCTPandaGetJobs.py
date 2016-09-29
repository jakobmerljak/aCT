from threading import Thread
import re
import time
import random
import arc
import aCTPanda
from act.common import aCTProxy
from aCTATLASProcess import aCTATLASProcess
from aCTAGISParser import aCTAGISParser


class PandaGetThr(Thread):
    """
    Helper function for getting panda jobs
    """
    def __init__ (self, func, siteName, prodSourceLabel=None):
        Thread.__init__(self)
        self.func = func
        self.siteName = siteName
        self.prodSourceLabel = prodSourceLabel
        self.result = (None, None)
    def run(self):
        self.result = self.func(self.siteName, self.prodSourceLabel)


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

        # AGIS queue info
        self.sites={}
        # Panda info on activated jobs: {queue: {'rc_test': 2, 'rest': 40}}
        self.activated = {}


    def getEndTime(self):
        return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())


    def setSites(self):
        self.sites = self.agisparser.getSites()

    def getActivated(self):
        """
        Get the number of activated jobs for each served queue
        """
        # Assume any proxy is ok to query panda
        queueinfo = self.pandas.values()[0].getQueueStatus()
        if queueinfo:
            for site in [k for k,v in self.sites.items() if v['enabled']]:
                if site not in queueinfo:
                    self.log.debug("%s: no jobs" % site)
                    self.activated[site] = {'rc_test': 0, 'rest': 0}
                    continue
                n_rc_test = 0
                n_rest = 0
                for label, jobs in queueinfo[site].iteritems():
                    if 'activated' in jobs:
                        if label == 'rc_test':
                            n_rc_test += jobs['activated']
                        else:
                            n_rest += jobs['activated']
                            
                self.activated[site] = {'rc_test': n_rc_test, 'rest': n_rest}
                self.log.debug('%s: activated rc_test %d, rest %d' % (site, n_rc_test, n_rest))

    def getPanda(self, sitename):
        return self.pandas[self.sites[sitename]['type']]


    def getJobs(self, num):
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
            nsubmitting = self.dbpanda.getNJobs("actpandastatus='sent' and siteName='%s'" % site )

            # Get total number of active jobs
            nall = self.dbpanda.getNJobs("siteName='%s' and actpandastatus!='done' \
                                          and actpandastatus!='donefailed' and actpandastatus!='donecancelled'" % site)
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
       
            for nc in range(0, max(int(num/nthreads), 1)):
                if stopflag:
                    continue

                tlist = []

                for i in range(0, nthreads):
                    r = random.Random()
                    if r.randint(0,100) <= 2:
                        if site in self.activated and self.activated[site]['rc_test'] == 0:
                            self.log.debug('%s: No rc_test activated jobs' % site)
                            continue
                        else:
                            t = PandaGetThr(self.getPanda(site).getJob, site, 'rc_test')
                    else:
                        if site in self.activated and self.activated[site]['rest'] == 0:
                            self.log.debug('%s: No activated jobs' % site)
                            continue
                        elif attrs['type'] == "analysis":
                            t = PandaGetThr(self.getPanda(site).getJob, site, 'user')
                        else:
                            t = PandaGetThr(self.getPanda(site).getJob, site)
                    tlist.append(t)
                    t.start()
                    nall += 1
                    if nall >= self.sites[site]['maxjobs']:
                        self.log.info("Site %s: reached max job limit of %d" % (site, self.sites[site]['maxjobs']))
                        stopflag = True
                        break
                    
                for t in tlist:
                    t.join()
                    (pandaid, pandajob, eventranges) = t.result
                    if pandaid == -1: # No jobs available
                        self.activated[site]['rc_test' if t.prodSourceLabel == 'rc_test' else 'rest'] = 0
                        continue
                    if pandaid == None:
                        stopflag = True
                        continue
                    
                    n = {}
                    # Check eventranges is defined for ES jobs
                    if re.search('eventService=True', pandajob) and (eventranges is None or eventranges == '[]'):
                        self.log.warning('%s: No event ranges given by panda' % pandaid)
                        n['pandastatus'] = 'finished'
                        n['actpandastatus'] = 'finished'
                        n['arcjobid'] = -1 # dummy id so job is not submitted
                    else:
                        n['pandastatus'] = 'sent'
                        n['actpandastatus'] = 'sent'
                    n['siteName'] = site
                    n['proxyid'] = self.proxymap[attrs['type']]
                    n['eventranges'] = eventranges
                    self.dbpanda.insertJob(pandaid, pandajob, n)
                    count += 1

        return count


    def process(self):
        """
        Method called from loop
        """
        self.setSites()
        if not self.activated or time.time() - self.starttime > 300:
            self.getActivated()
            self.starttime = time.time()

        # request new jobs
        num = self.getJobs(int(self.conf.get(['panda','getjobs'])))
        if num:
            self.log.info("Got %i jobs" % num)
        
if __name__ == '__main__':
    am=aCTPandaGetJobs()
    am.run()
    #am.finish()
