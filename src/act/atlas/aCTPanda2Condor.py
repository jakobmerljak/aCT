from act.atlas.aCTATLASProcess import aCTATLASProcess
from act.atlas.aCTPanda2ClassAd import aCTPanda2ClassAd


class aCTPanda2Condor(aCTATLASProcess):
    '''
    Take new jobs in Panda table and insert then into the condorjobs table.
    '''

    def __init__(self):
        aCTATLASProcess.__init__(self, ceflavour=['HTCONDOR-CE', 'CREAM-CE'])

    def createCondorJobs(self):

        jobs = self.dbpanda.getJobs("arcjobid is NULL and siteName in %s limit 10000" % self.sitesselect)
        proxies_map = {}

        for job in jobs:

            if job['proxyid'] not in proxies_map:
                proxies_map[job['proxyid']] = self.dbarc.getProxyPath(job['proxyid'])

            parser = aCTPanda2ClassAd(job['pandajob'], job['siteName'], self.sites[job['siteName']], proxies_map[job['proxyid']],
                                   self.arcconf.get(["tmp", "dir"]), self.conf, self.log)

            self.log.info("site %s maxwalltime %s", job['siteName'],self.sites[job['siteName']]['maxwalltime'] )

            parser.parse()

            try:
                classad = parser.getClassAd()
            except:
                pass
            if classad is not None:
                endpoints = ','.join(self.sites[job['siteName']]['endpoints'])
                self.log.info("Inserting job %d with clusterlist %s" % (job['pandaid'], endpoints))
                self.log.debug("%d: classad: %s" % (job['pandaid'], classad))
                maxattempts = 0 # Never resubmit condor jobs

                aid = self.dbcondor.insertCondorJobDescription(classad, maxattempts=maxattempts, clusterlist=endpoints,
                                                               proxyid=job['proxyid'], appjobid=str(job['pandaid']),
                                                               fairshare=job['siteName'])
                if not aid:
                    self.log.error("%d: Failed to insert condor job description: %s" % (job['pandaid'], classad))
                    continue

                jd = {}
                jd['arcjobid'] = aid['LAST_INSERT_ID()']
                jd['pandastatus'] = 'starting'
                # make sure actpandastatus is really 'sent', in case of resubmitting
                jd['actpandastatus'] = 'sent'
                jd['corecount'] = int(classad['+xcount'])
                self.dbpanda.updateJob(job['pandaid'], jd)

    def process(self):
        self.setSites()
        self.createCondorJobs()

if __name__ == '__main__':

    am = aCTPanda2Condor()
    am.run()
    am.finish()
