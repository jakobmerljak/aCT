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
        self.agisparser = aCTAGISParser(self.log)
        
        self.sites = {}
        self.setSites()

    def setSites(self):
        self.sites = self.agisparser.getSites()                        

    def createArcJobs(self):

        jobs = self.dbpanda.getJobs("arcjobid is NULL limit 10000")

        for job in jobs:

            inputdir = self.conf.get(["tmp", "dir"]) + "/inputfiles/" + str(job['pandaid'])
            
            parser = aCTPanda2Xrsl(job['pandajob'], job['siteName'], self.sites[job['siteName']]['schedconfig'],
                                   self.sites[job['siteName']]['catalog'], self.sites[job['siteName']]['corecount'],
                                   self.sites[job['siteName']]['truepilot'], self.sites[job['siteName']]['maxwalltime'], inputdir)
            parser.parse()
            try:
                xrsl = parser.getXrsl()
            except:
                pass
            if xrsl is not None:
                endpoints = self.sites[job['siteName']]['endpoints']
                cl = []
                for e in endpoints:
                    if not e.startswith('gsiftp://'):
                        # gsiftp is not used here, it's just to make a url
                        e = 'gsiftp://' + e
                    cl.append(urlparse(e).hostname + urlparse(e).path)
                cls = ",".join(cl)
                self.log.info("Inserting job %i with clusterlist %s" % (job['pandaid'], cls))
                maxattempts = 5
                if job['siteName'] == 'BOINC':
                    maxattempts = 30
                if self.sites[job['siteName']]['truepilot']:
                    # truepilot jobs should never be resubmitted
                    maxattempts = 0

                # Set the list of files to download at the end of the job
                downloadfiles = 'gmlog/errors'
                if not self.sites[job['siteName']]['truepilot']:
                    downloadfiles += ';jobSmallFiles.tgz'
                try:
                    downloadfiles += ';%s' % parser.jobdesc['logFile'][0].replace('.tgz', '')
                except:
                    pass

                aid = self.dbarc.insertArcJobDescription(xrsl, maxattempts=maxattempts, clusterlist=cls,
                                                         proxyid=job['proxyid'], appjobid=str(job['pandaid']),
                                                         downloadfiles=downloadfiles)
                if not aid:
                    self.log.error("%s: Failed to insert arc job description: %s" % (job['pandaid'], xrsl))
                    continue
                
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
