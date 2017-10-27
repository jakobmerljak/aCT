import httplib
import json

from act.atlas.aCTATLASProcess import aCTATLASProcess
from act.atlas.aCTPanda2ClassAd import aCTPanda2ClassAd
from act.atlas.aCTAGISParser import aCTAGISParser


class aCTPanda2Condor(aCTATLASProcess):
    '''
    Take new jobs in Panda table and insert then into the condorjobs table.
    '''

    def __init__(self):
        aCTATLASProcess.__init__(self)
        self.agisparser = aCTAGISParser(self.log)
        self.sites = {}
        self.setSites()

    def setSites(self):
        self.sites = self.agisparser.getSites(flavour='HTCONDOR-CE')                        
        self.osmap = self.agisparser.getOSMap()                        

    def createCondorJobs(self):

        jobs = self.dbpanda.getJobs("arcjobid is NULL and siteName in ('%s') limit 10000" % "','".join(self.sites.keys()))
        proxies_map = {}

        for job in jobs:

            if job['proxyid'] not in proxies_map:
                proxies_map[job['proxyid']] = self.dbarc.getProxyPath(job['proxyid'])

            parser = aCTPanda2ClassAd(job['pandajob'], job['siteName'], self.sites[job['siteName']], self.osmap,
                                   self.arcconf.get(["tmp", "dir"]), job['eventranges'], self.log)

            self.log.info("site %s maxwalltime %s", job['siteName'],self.sites[job['siteName']]['maxwalltime'] )

            parser.parse()

            try:
                classad = parser.getClassAd()
            except:
                pass
            if classad is not None:
                endpoints = self.sites[job['siteName']]['endpoints']
                cl = []
                for e in endpoints:
                    cl.append('condor %s %s' % (e.split(':')[0], e))
                cls = ",".join(cl)
                self.log.info("Inserting job %i with clusterlist %s" % (job['pandaid'], cls))
                maxattempts = 0 # Never resubmit condor jobs

                aid = self.dbcondor.insertCondorJobDescription(classad, maxattempts=maxattempts, clusterlist=cls,
                                                               proxyid=job['proxyid'], appjobid=str(job['pandaid']),
                                                               fairshare=job['siteName'])
                if not aid:
                    self.log.error("%s: Failed to insert condor job description: %s" % (job['pandaid'], classad))
                    continue

                jd = {}
                jd['arcjobid'] = aid['LAST_INSERT_ID()']
                jd['pandastatus'] = 'starting'
                # make sure actpandastatus is really 'sent', in case of resubmitting
                jd['actpandastatus'] = 'sent'
                self.dbpanda.updateJob(job['pandaid'], jd)

    def process(self):
        self.setSites()
        self.createCondorJobs()

if __name__ == '__main__':

    am = aCTPanda2Condor()
    am.run()
    am.finish()
