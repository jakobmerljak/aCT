from urlparse import urlparse
from aCTATLASProcess import aCTATLASProcess
from aCTPanda2Xrsl import aCTPanda2Xrsl
from aCTAGISParser import aCTAGISParser

class aCTPanda2Arc(aCTATLASProcess):
    '''
    Take new jobs in Panda table and insert then into the arcjobs table.
    '''

    def __init__(self):
        aCTATLASProcess.__init__(self)
        self.agisparser = aCTAGISParser()
        
        self.sites = {}
        self.setSites()
        print self.sites

    def setSites(self):
        self.sites = self.agisparser.getSites()                        

    def createArcJobs(self):

        jobs = self.dbpanda.getJobs("arcjobid is NULL limit 10000")

        for job in jobs:
            
            parser = aCTPanda2Xrsl(job['pandajob'], job['siteName'], self.sites[job['siteName']]['schedconfig'],
                                   self.sites[job['siteName']]['catalog'], self.sites[job['siteName']]['corecount'])
            parser.parse()
            try:
                xrsl = parser.getXrsl()
            except:
                pass
            if xrsl is not None:
                #print xrsl
                endpoints = self.sites[job['siteName']]['endpoints']
                cl = []
                for e in endpoints:
                    cl.append(urlparse(e).hostname + urlparse(e).path)
                cls = ",".join(cl)
                self.log.info("Inserting job %i with clusterlist %s" % (job['pandaid'], cls))
                maxattempts = 5
                if job['siteName'] == 'BOINC':
                    maxattempts = 30
                aid = self.dbarc.insertArcJobDescription(xrsl, maxattempts=maxattempts, clusterlist=cls,
                                                         proxyid=job['proxyid'], appjobid=str(job['pandaid']))
                jd = {}
                jd['arcjobid'] = aid['LAST_INSERT_ID()']
                jd['pandastatus'] = 'starting'
                # make sure actpandastatus is really 'sent', in case of resubmitting 
                jd['actpandastatus'] = 'sent'
                self.dbpanda.updateJob(job['pandaid'], jd)
                

    def process(self):
        self.setSites()
        self.createArcJobs()


if __name__ == '__main__':

    am=aCTPanda2Arc()
    am.run()
    am.finish()
