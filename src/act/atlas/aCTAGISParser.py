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
                pass
            try:
                sites[sitename]['type'] = self.conf.getListCond(["sites","site"],"name=" + sitename ,["type"])[0]
            except:
                # ignore missing type and hope agis has the info
                pass
            try:
                sites[sitename]['corecount'] = int(self.conf.getListCond(["sites","site"],"name=" + sitename ,["corecount"])[0])
            except:
                pass
            try:
                sites[sitename]['catalog'] = self.conf.getListCond(["sites","site"],"name=" + sitename ,["catalog"])[0]
            except:
                sites[sitename]['catalog'] = self.conf.get(["panda", "catalog"])
            try:
                sites[sitename]['maxjobs'] = int(self.conf.getListCond(["sites","site"],"name=" + sitename ,["maxjobs"])[0])
            except:
                pass
            try:
                sites[sitename]['truepilot'] = int(self.conf.getListCond(["sites","site"],"name=" + sitename ,["truepilot"])[0])
            except:
                pass
            sites[sitename]['status'] = 'online'
            sites[sitename]['enabled'] = True
        self.log.info("Parsed sites from config: %s"%str(sites.keys()))
        return sites 

  
    def _parseAgisJson(self, agisfilename, pilotmgr):
        # todo: first read from config, then read from agis and update if not already in sites list
        with open(agisfilename) as f:
            agisjson=json.load(f)
        sites=dict([(agisjson[entry]['panda_resource'],dict(agisjson[entry].items()+[('schedconfig',entry)])) for entry in agisjson if agisjson[entry].has_key('panda_resource')])
        for sitename in sites:
            if not sites[sitename].has_key('catalog'):
                sites[sitename]['catalog'] = self.conf.get(["panda", "catalog"])
            if not sites[sitename].has_key('schedconfig'):
                sites[sitename]['schedconfig'] = sitename
            if sites[sitename]['pilot_manager'] == pilotmgr:
                sites[sitename]['enabled'] = True
                sites[sitename]['maxjobs'] = int(self.conf.get(["agis", "maxjobs"]))
            else:
                sites[sitename]['enabled'] = False
                sites[sitename]['maxjobs'] = 0
            if (not sites[sitename].has_key('corecount')) or (not sites[sitename]['corecount']):
                sites[sitename]['corecount'] = 1
            # pull out endpoints
            if not sites[sitename].has_key('endpoints'):
                sites[sitename]['endpoints'] = ['%s/%s' % (queue['ce_endpoint'], queue['ce_queue_name']) for queue in sites[sitename]['queues']]
            if not sites[sitename].has_key('maxwalltime'):
                try:
                    maxwalltime = max([int(queue['ce_queue_maxwctime']) for queue in sites[sitename]['queues']])
                except:
                    maxwalltime = 0
                # if maxwalltime is not set or is larger than a week, then set to 1 week
                if maxwalltime <= 0:
                    maxwalltime = 60*24*7
                else:
                    maxwalltime = min(maxwalltime, 60*24*7) 
                sites[sitename]['maxwalltime'] = maxwalltime
            else:
                sites[sitename]['maxwalltime'] = min(int(sites[sitename]['maxwalltime']), 60*24*7)
            if not sites[sitename].has_key('maxcputime'):
                try:
                    maxcputime = max([int(queue['ce_queue_maxcputime']) for queue in sites[sitename]['queues']])
                except:
                    maxcputime = 0
                # if maxcputime is not set or is larger than a week, then set to 1 week
                if maxcputime <= 0:
                    maxcputime = 60*24*7
                else:
                    maxcputime = min(maxcputime, 60*24*7) 
                sites[sitename]['maxcputime'] = maxcputime
            else:
                sites[sitename]['maxcputime'] = min(int(sites[sitename]['maxcputime']), 60*24*7)
            # true pilot or not
            sites[sitename]['truepilot'] = (sites[sitename]['copytool'] != 'mv')
        self.log.info("Parsed sites from AGIS: %s"%str(sites.keys()))
        return sites

    def _mergeSiteDicts(self, dict1, dict2):
        for d in dict2.keys():
            if dict1.has_key(d):
                dict1[d].update(dict2[d])
            else:
                dict1[d]=dict2[d]

    def getSites(self):
        self.conf.parse()
        agisfile = self.conf.get(['agis','jsonfilename'])
        if not agisfile:
            # No AGIS, only manually configured sites
            return self._parseConfigSites()
        
        # wait for AGIS json to be produced
        i = 0
        while (True):
            try:
                agismtime = os.stat(agisfile).st_mtime
                break
            except:
                i += 1 
                if i > 2:
                    self.log.critical("Couldn't get AGIS json")
                    return {}
                time.sleep(10)
        
        # check if json file or config file changed before parsing
        if (self.tparse<agismtime) or (self.tparse<os.stat(self.conf.configfile).st_mtime):
            self.log.info("AGIS file and/or config modified, reparsing site info")
            pilotmgr = self.conf.get(['agis','pilotmanager'])
            start_parsing = time.time()
            self.sites = self._parseAgisJson(agisfile, pilotmgr)
            self._mergeSiteDicts(self.sites, self._parseConfigSites())
            self.tparse = time.time()
            self.log.debug("Time to parse site info: %g s"%(self.tparse-start_parsing))
            
            self.log.info("Queues served:")
            for site, info in dict(self.sites).items():
                if not info['enabled']:
                    continue
                if not info['endpoints']:
                    self.log.warning("%s: No CE endpoints defined, this site cannot be used" % site)
                    del self.sites[site]
                else:
                    self.log.info("%s: %s, maxjobs %d" % (site, 'True pilot' if info['truepilot'] else 'ARC pilot', info['maxjobs']))

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
        sites = dict([(s,i) for s,i in sites.items() if i['enabled']])
        pprint.pprint(sites)
        print len(sites)
        exit(1)
        time.sleep(10)
