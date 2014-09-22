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
import time
import os
import json
from act.common import aCTConfig

class aCTAGISParser:
    
    def __init__(self):
        self.tparse = time.time()
        self.conf=aCTConfig.aCTConfigATLAS()
        agisfile = self.conf.get(['agis','jsonfilename'])
        self.sites = self._parseAgisJson(agisfile)
        
    def _parseAgisJson(self, agisfilename):
        agisfile=open(agisfilename)
        agisjson=json.load(agisfile)
        sites=dict([(entry,agisjson[entry]) for entry in agisjson if agisjson[entry]['pilot_manager']=='aCT'])
        for sitename in sites:
            if not sites[sitename].has_key('catalog'):
                sites[sitename]['catalog'] = self.conf.get(["panda", "catalog"])
            for entry in sites[sitename]:
                try:
                    # check if there is a corresponding entry in config.
                    sites[sitename][entry] = str(self.conf.getListCond(["sites","site"], "name="+ sitename , [entry])[0])
                    try:
                        # try to convert to int. will fail if entry wasn't meant to be int
                        sites[sitename][entry] = int(sites[sitename][entry])
                    except:
                        pass
                except:
                    pass
            if not sites[sitename].has_key('schedconfig'):
                sites[sitename]['schedconfig'] = sitename
            if not sites[sitename].has_key('maxjobs'):
                sites[sitename]['maxjobs'] = 1000000
            if not sites[sitename]['corecount']:
                sites[sitename]['corecount'] = 1
            # pull out endpoints
            sites[sitename]['endpoints'] = [queue['ce_endpoint'] for queue in sites[sitename]['queues']]
        return sites

    def getSites(self):
        agisfile = self.conf.get(['agis','jsonfilename'])
        # check if json file changed before parsing
        if self.tparse<os.stat(agisfile).st_mtime:
            self.sites = self._parseAgisJson(agisfile)
        return self.sites
    
if __name__ == '__main__':

    import pprint
    agisparser=aCTAGISParser()
    while 1:
        sites = agisparser.getSites()
        pprint.pprint(sites)
        exit(1)
        time.sleep(1)
