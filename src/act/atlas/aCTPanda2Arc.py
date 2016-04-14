import httplib
import json

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
        self.osmap = self.agisparser.getOSMap()                        

    def createArcJobs(self):

        jobs = self.dbpanda.getJobs("arcjobid is NULL limit 10000")
        proxies_map = {}

        for job in jobs:

            if job['proxyid'] not in proxies_map:
                proxies_map[job['proxyid']] = self.dbarc.getProxyPath(job['proxyid'])

            parser = aCTPanda2Xrsl(job['pandajob'], job['siteName'], self.sites[job['siteName']]['schedconfig'],
                                   self.sites[job['siteName']]['catalog'], self.osmap, self.sites[job['siteName']]['corecount'],
                                   self.sites[job['siteName']]['truepilot'], self.sites[job['siteName']]['maxwalltime'],
                                   self.conf.get(["tmp", "dir"]), job['eventranges'])

            self.log.info("site %s maxwalltime %s", job['siteName'],self.sites[job['siteName']]['maxwalltime'] )

            parser.parse()
            traces = []
            # Getting DDM endpoint. 'ddm' should be filled by _parseAgisJson
            if 'ddm' in self.sites[job['siteName']]:
                for trace in parser.traces:
                    trace['localSite'] = self.sites[job['siteName']]['ddm']
                    trace['remoteSite'] = self.sites[job['siteName']]['ddm']
                    traces.append(trace)
            else:
                traces = parser.traces
            self.sendTraces(traces, proxies_map[job['proxyid']])
            try:
                xrsl = parser.getXrsl()
            except:
                pass
            if xrsl is not None:
                endpoints = self.sites[job['siteName']]['endpoints']
                cl = []
                for e in endpoints:
                    if e.find('://') == -1:
                        # gsiftp is default if not specified
                        e = 'gsiftp://' + e
                    cl.append(e)
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

    def sendTraces(self, traces, proxypath):
        for trace in traces:
            try:
                conn = httplib.HTTPSConnection('rucio-lb-prod.cern.ch:443', key_file=proxypath, cert_file=proxypath)
                rdata = json.dumps(trace)
                headers = {"Content-type": "application/json"}
                conn.request("POST", "/traces/", rdata, headers)
                resp = conn.getresponse()
                status = resp.status
                if status != 201:
                    self.log.error("Error sending trace: %s : %s" % (resp.status, resp.reason))
                conn.close()
            except Exception, error:
                self.log.error("Error sending trace: %s" % error)


if __name__ == '__main__':

    am = aCTPanda2Arc()
    am.run()
    am.finish()
