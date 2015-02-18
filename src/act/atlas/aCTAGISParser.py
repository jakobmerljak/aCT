# load agis.json
# if file changed since last load, reload
# getSites()
## returns dict with site info
## always check config first
## schedconfig=sitename?
## maxjobs defaults to 1M
## endpoints should be pulled out of "queues" (ce_endpoints)
## corecount defaults to 1
## catalog defaults to panda config value
import logging
import time
import os, sys
import json
from act.common import aCTConfig

class aCTAGISParser:
    
    def __init__(self, logger):
        self.log=logger
        self.conf=aCTConfig.aCTConfigATLAS()
        self.tparse = 0
        self.getSites()
                
    def _parseConfigSites(self):
        sites = {}
        for sitename in self.conf.getList(["sites","site","name"]):
            sites[sitename] = {}
            configendpoints = self.conf.getListCond(["sites","site"],"name=" + sitename ,["endpoints","item"])
            if configendpoints:
                sites[sitename]['endpoints'] = configendpoints
            try:
                sites[sitename]['schedconfig'] = self.conf.getListCond(["sites","site"],"name=" + sitename ,["schedconfig"])[0]
            except:
                sites[sitename]['schedconfig'] = sitename
            try:
                sites[sitename]['type'] = self.conf.getListCond(["sites","site"],"name=" + sitename ,["type"])[0]
            except:
                # ignore missing type and hope agis has the info
                pass
            try:
                sites[sitename]['corecount'] = int(self.conf.getListCond(["sites","site"],"name=" + sitename ,["corecount"])[0])
            except:
                sites[sitename]['corecount'] = 1
            try:
                sites[sitename]['catalog'] = self.conf.getListCond(["sites","site"],"name=" + sitename ,["catalog"])[0]
            except:
                sites[sitename]['catalog'] = self.conf.get(["panda", "catalog"])
            try:
                sites[sitename]['maxjobs'] = int(self.conf.getListCond(["sites","site"],"name=" + sitename ,["maxjobs"])[0])
            except:
                sites[sitename]['maxjobs'] = 1000000
        self.log.debug("Parsed sites from config: %s"%str(sites.keys()))
        return sites 

  
    def _parseAgisJson(self, agisfilename, pilotmgr):
        # todo: first read from config, then read from agis and update if not already in sites list
        with open(agisfilename) as f:
            agisjson=json.load(f)
        sites=dict([(entry,agisjson[entry]) for entry in agisjson if agisjson[entry]['pilot_manager']==pilotmgr])
        for sitename in sites:
            if not sites[sitename].has_key('catalog'):
                sites[sitename]['catalog'] = self.conf.get(["panda", "catalog"])
            if not sites[sitename].has_key('schedconfig'):
                sites[sitename]['schedconfig'] = sitename
            if not sites[sitename].has_key('maxjobs'):
                sites[sitename]['maxjobs'] = self.conf.get(["agis", "maxjobs"])
            if (not sites[sitename].has_key('corecount')) or (not sites[sitename]['corecount']):
                sites[sitename]['corecount'] = 1
            # pull out endpoints
            if not sites[sitename].has_key('endpoints'):
                sites[sitename]['endpoints'] = ['%s/%s' % (queue['ce_endpoint'], queue['ce_queue_name']) for queue in sites[sitename]['queues']]
        self.log.debug("Parsed sites from AGIS: %s"%str(sites.keys()))
        return sites

    def _mergeSiteDicts(self, dict1, dict2):
        for d in dict2.keys():
            if dict1.has_key(d):
                dict1[d].update(dict2[d])
            else:
                dict1[d]=dict2[d]

    def getSites(self):
        agisfile = self.conf.get(['agis','jsonfilename'])
        if not agisfile:
            # No AGIS, only manually configured sites
            return self._parseConfigSites()
        
        # check if json file or config file changed before parsing
        if (self.tparse<os.stat(agisfile).st_mtime) or (self.tparse<os.stat(self.conf.configfile).st_mtime):
            self.log.info("AGIS file and/or config modified, reparsing site info")
            pilotmgr = self.conf.get(['agis','pilotmanager'])
            start_parsing = time.time()
            self.sites = self._parseAgisJson(agisfile, pilotmgr)
            self._mergeSiteDicts(self.sites, self._parseConfigSites())
            self.tparse = time.time()
            self.log.debug("Time to parse site info: %g s"%(self.tparse-start_parsing))
        return self.sites
    
if __name__ == '__main__':

    import pprint
    log = logging.getLogger()
    log.setLevel("DEBUG")
    out = logging.StreamHandler(sys.stdout)
    log.addHandler(out)
    agisparser=aCTAGISParser(log)
    while 1:
        sites = agisparser.getSites()
        pprint.pprint(sites)
        print len(sites)
        exit(1)
        time.sleep(10)
