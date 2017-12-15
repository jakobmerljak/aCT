import json
import requests
from act.common.aCTLogger import aCTLogger

class aCTAPFMon:

    def __init__(self, conf):
        
        self.logger = aCTLogger(self.__class__.__name__, arclog=False)
        self.log = self.logger()

        self.apfmonurl = conf.get(["monitor", "apfmon"])
        self.acturl = conf.get(["joblog", "urlprefix"])
        self.factory = conf.get(["panda", "schedulerid"])

    def registerFactory(self):
        '''Register this instance of aCT'''

        if not self.apfmonurl:
            return

        factoryregister = '%s/factories/%s' % (self.apfmonurl, self.factory)
        factory = {"url":     self.acturl,
                   "email":   "act-dev@cern.ch",
                   "version": "1.0.0"}
        payload = json.dumps(factory)

        self.log.debug("Registering %s as new factory at %s" % (self.factory, factoryregister))
        r = requests.put(factoryregister, data=payload)
        self.log.debug("APFmon returned %d: %s" % (r.status_code, r.text))

    def registerLabels(self, sites):
        '''Register labels (panda queues)'''

        if not self.apfmonurl or not sites:
            return

        labelregister = '%s/labels' % self.apfmonurl
        labels = [{"name": "%s" % site, "factory": "%s" % self.factory} for site in sites]
        payload = json.dumps(labels)

        self.log.debug("Registering new labels: %s" % ','.join([s for s in sites]))
        r = requests.put(labelregister, data=payload)
        self.log.debug("APFmon returned %d: %s" % (r.status_code, r.text))

    def registerJobs(self, pandaids, site):
        '''Register new jobs'''

        if not self.apfmonurl:
            return

        jobregister = '%s/jobs' % self.apfmonurl
        jobs = [{'cid'        : '%s' % pid,
                 'factory'    : self.factory,
                 'label'      : site} for pid in pandaids]
        payload = json.dumps(jobs)

        self.log.debug("sending to %s: %s" % (jobregister, jobs))
        r = requests.put(jobregister, data=payload)
        self.log.debug("APFmon returned %d: %s" % (r.status_code, r.text))

    def updateJob(self, pandaid, status, exitcode=None):
        '''Update job status. Exit code is required for "exiting" state'''
        
        if not self.apfmonurl:
            return
 
        jobupdate = '%s/jobs/%s:%s' % (self.apfmonurl, self.factory, pandaid)
        payload = {"state": "%s" % status}
        if exitcode is not None:
            payload['rc'] = exitcode

        self.log.debug("sending to %s: %s" % (jobupdate, payload))
        r = requests.post(jobupdate, data=payload)
        self.log.debug("APFmon returned %d: %s" % (r.status_code, r.text))

def test():
    from act.common.aCTConfig import aCTConfigATLAS
    conf = aCTConfigATLAS()
    apf = aCTAPFMon(conf)
    apf.registerFactory()
    apf.registerLabels(['ARC-TEST', 'BOINC_MCORE'])
    apf.registerJobs(['123456', '234567'], 'ARC-TEST')
    apf.updateJob('123456', 'running')

if __name__ == '__main__':
    test()
    
