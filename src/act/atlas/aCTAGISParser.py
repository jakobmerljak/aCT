import logging
import time
import os, re, sys
import json
from act.common import aCTConfig

class aCTAGISParser:
    '''
    Load agis jsons. If file changes since last load, reload. Then load site
    info from config and overwrite agis values.
    '''

    def __init__(self, logger):
        self.log = logger
        self.conf = aCTConfig.aCTConfigATLAS()
        self.arcconf = aCTConfig.aCTConfigARC()
        self.tparse = 0
        self.getSites()

    def _parseConfigSites(self):
        sites = {}
        for sitename in self.conf.getList(["sites","site","name"]):
            siteinfo = {}
            configendpoints = self.conf.getListCond(["sites","site"],"name=" + sitename, ["endpoints","item"])
            if configendpoints:
                siteinfo['endpoints'] = configendpoints
            try:
                siteinfo['flavour'] = self.conf.getListCond(["sites","site"],"name=" + sitename, ["flavour"])[0]
            except:
                pass
            try:
                siteinfo['schedconfig'] = self.conf.getListCond(["sites","site"],"name=" + sitename, ["schedconfig"])[0]
            except:
                pass
            try:
                siteinfo['type'] = self.conf.getListCond(["sites","site"],"name=" + sitename, ["type"])[0]
            except:
                # ignore missing type and hope agis has the info
                pass
            try:
                siteinfo['corecount'] = int(self.conf.getListCond(["sites","site"],"name=" + sitename, ["corecount"])[0])
            except:
                pass
            try:
                siteinfo['maxjobs'] = int(self.conf.getListCond(["sites","site"],"name=" + sitename, ["maxjobs"])[0])
            except:
                pass
            try:
                siteinfo['truepilot'] = int(self.conf.getListCond(["sites","site"],"name=" + sitename, ["truepilot"])[0])
            except:
                pass
            try:
                siteinfo['push'] = int(self.conf.getListCond(["sites","site"],"name=" + sitename, ["push"])[0])
            except:
                pass
            try:
                siteinfo['agisjsons'] = int(self.conf.getListCond(["sites","site"],"name=" + sitename, ["agisjsons"])[0])
            except:
                siteinfo['agisjsons'] = 0
            # If status is already defined in AGIS then only override if explicity specified here
            try:
                siteinfo['status'] = self.conf.getListCond(["sites","site"],"name=" + sitename, ["status"])[0]
            except:
                if not self.sites.get(sitename, {}).get('status'):
                    siteinfo['status'] = 'online'
            siteinfo['enabled'] = True
            sites[sitename] = siteinfo
        self.log.info("Parsed sites from config: %s" % str(list(sites.keys())))
        return sites

    def _parseAgisJson(self, agisfilename, pilotmgr, pilotver):
        with open(agisfilename) as f:
            agisjson = json.load(f)
        sites = dict([(agisjson[entry]['panda_resource'], dict(list(agisjson[entry].items())+[('schedconfig', entry)])) for entry in agisjson if 'panda_resource' in agisjson[entry]])
        for sitename, siteinfo in list(sites.items()):
            siteinfo['push'] = True # TODO configure in AGIS
            if 'schedconfig' not in siteinfo:
                siteinfo['schedconfig'] = sitename
            if (pilotmgr == 'all' or siteinfo['pilot_manager'] == pilotmgr) and \
               (pilotver is None or siteinfo['pilot_version'] == str(pilotver)) and \
               siteinfo['state'] == 'ACTIVE':
                siteinfo['enabled'] = True
                siteinfo['maxjobs'] = int(self.conf.get(["agis", "maxjobs"]))
            else:
                siteinfo['enabled'] = False
                siteinfo['maxjobs'] = 0
            if ('corecount' not in siteinfo) or (not siteinfo['corecount']):
                siteinfo['corecount'] = 1
            if siteinfo.get('queues'):
                siteinfo['flavour'] = siteinfo['queues'][0]['ce_flavour']
            else:
                siteinfo['flavour'] = 'UNKNOWN'
            # pull out endpoints
            if 'endpoints' not in siteinfo:
                endpoints = []
                for queue in siteinfo['queues']:
                    if queue.get('ce_state') != 'ACTIVE':
                        if siteinfo['enabled']:
                            self.log.info('Skipping inactive CE %s' % queue.get('ce_endpoint'))
                        continue
                    if queue['ce_flavour'] == 'CREAM-CE':
                        endpoints.append('cream %s/ce-cream/services/CREAM2 %s %s' % (queue['ce_endpoint'], queue['ce_jobmanager'], queue['ce_queue_name']))
                    elif queue['ce_flavour'] == 'HTCONDOR-CE':
                        endpoints.append('condor %s %s %s' % (queue['ce_endpoint'].split(':')[0], queue['ce_endpoint'], queue['ce_queue_name']))
                    elif queue['ce_flavour'] == 'ARC-CE':
                        endpoints.append('%s/%s' % (queue['ce_endpoint'], queue['ce_queue_name']))
                    else:
                        if siteinfo['enabled']:
                            self.log.warning('Cannot use CE flavour %s for queue %s' % (queue['ce_flavour'], sitename))
                # Ignore endpoints with "default" queue unless that is the only queue
                nondefaultendpoints = [e for e in endpoints if not e.endswith(' default')]
                if not nondefaultendpoints:
                    siteinfo['endpoints'] = endpoints
                else:
                    siteinfo['endpoints'] = nondefaultendpoints
            if 'maxtime' not in siteinfo or siteinfo['maxtime'] == 0:
                try:
                    maxwalltime = max([int(queue['ce_queue_maxwctime']) for queue in siteinfo['queues']])
                except:
                    maxwalltime = 0
                # if maxwalltime is not set or is larger than a week, then set to 1 week
                if maxwalltime <= 0:
                    maxwalltime = 60*24*7
                else:
                    maxwalltime = min(maxwalltime, 60*24*7)
                siteinfo['maxwalltime'] = maxwalltime
            else:
                siteinfo['maxwalltime'] = min(int(siteinfo['maxtime'])/60, 60*24*7)
            if 'maxcputime' not in siteinfo:
                try:
                    maxcputime = max([int(queue['ce_queue_maxcputime']) for queue in siteinfo['queues']])
                except:
                    maxcputime = 0
                # if maxcputime is not set or is larger than a week, then set to 1 week
                if maxcputime <= 0:
                    maxcputime = 60*24*7
                else:
                    maxcputime = min(maxcputime, 60*24*7)
                siteinfo['maxcputime'] = maxcputime
            else:
                siteinfo['maxcputime'] = min(int(siteinfo['maxcputime']), 60*24*7)

            if siteinfo['type'] == 'special':
                siteinfo['type'] = 'production'
            # true pilot or not, based on whether mv copytool is used
            truepilot = True
            if 'mv' in siteinfo['copytools']:
                # Check in acopytools if there is more than one copytool
                if len(siteinfo['copytools']) == 1 or 'mv' in siteinfo['acopytools'].get('pr', []):
                    truepilot = False
            siteinfo['truepilot'] = truepilot
            # set OS bucket IDs
            try:
                objstore = [self.bucketmap[e]['bucket_id'] for e in siteinfo['astorages']['es_events'] if e in self.bucketmap and self.bucketmap[e]['type'] == 'OS_ES'][0]
                siteinfo['ddmoses'] = objstore
            except:
                if siteinfo['enabled'] and siteinfo['jobseed'] in ('es', 'all'):
                    self.log.debug('No ES object store for %s but jobseed is %s' % (sitename, siteinfo['jobseed']))

        if len(sites) < 100:
            self.log.info("Parsed sites from AGIS: %s" % str(list(sites.keys())))
        else:
            self.log.info("Parsed %d sites from AGIS" % len(sites))
        return sites

    def _parseDDMEndpoints(self, filename):
        self.osmap = {}
        self.bucketmap = {}
        with open(filename) as f:
            self.ddmjson = json.load(f)
        # make map of bucket_id: endpoint
        for ep in self.ddmjson:
            if ep['state'] != 'ACTIVE':
                continue
            try:
                bucket_id = ep['id']
            except:
                self.log.info('No bucket_id info for %s', ep['name'])
                continue
            try:
                protocol = [p for p in list(ep['protocols'].keys()) if p.startswith('s3://')][0]
            except:
                self.log.info('No s3 endpoint for %s' % ep['name'])
                continue
            endpoint = '%s%s' % (protocol, ep['protocols'][protocol][0][2])
            endpoint = re.sub('s3:/', 's3://', re.sub('//', '/', endpoint))
            self.osmap[bucket_id] = endpoint
            self.bucketmap[ep['name']] = {'bucket_id': bucket_id, 'type': ep['type']}

    def _mergeSiteDicts(self, dict1, dict2):
        for d in list(dict2.keys()):
            if d in dict1:
                dict1[d].update(dict2[d])
            else:
                dict1[d]=dict2[d]

    def getSites(self, flavour=None):
        '''Get site info, filtered by CE flavour(s) if given'''

        self.conf.parse()
        agisfile = self.conf.get(['agis','jsonfilename'])
        if not agisfile:
            # No AGIS, only manually configured sites
            return self._parseConfigSites()

        # wait for AGIS json to be produced
        i = 0
        while True:
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
        if (self.tparse < agismtime) or (self.tparse < os.stat(self.conf.configfile).st_mtime):
            self.log.info("AGIS file and/or config modified, reparsing site info")
            pilotmgr = self.conf.get(['agis','pilotmanager'])
            pilotver = self.conf.get(['agis','pilotversion'])
            start_parsing = time.time()
            self._parseDDMEndpoints(self.conf.get(['agis', 'osfilename']))
            self.sites = self._parseAgisJson(agisfile, pilotmgr, pilotver)
            self._mergeSiteDicts(self.sites, self._parseConfigSites())
            self.tparse = time.time()
            self.log.debug("Time to parse site info: %g s"%(self.tparse-start_parsing))

            self.log.info("Queues served:")
            for site, info in sorted(self.sites.items()):
                if not info['enabled']:
                    continue
                if 'endpoints' not in info or not info['endpoints']:
                    self.log.warning("%s: No CE endpoints defined, this site cannot be used" % site)
                    del self.sites[site]
                else:
                    self.log.info("%s (%s): %s (%s), maxjobs %d" % (site, info['status'], 'True pilot' if info['truepilot'] else 'ARC pilot', info['flavour'], info['maxjobs']))

        if flavour:
            return dict((k,v) for (k,v) in self.sites.items() if v.get('flavour') in flavour)
        return self.sites

    def getOSMap(self):
        ''' Return dictionary of OS ID to OS endpoint'''

        return self.osmap

if __name__ == '__main__':

    import pprint
    log = logging.getLogger()
    log.setLevel("DEBUG")
    out = logging.StreamHandler(sys.stdout)
    log.addHandler(out)
    agisparser=aCTAGISParser(log)
    while 1:
        sites = agisparser.getSites()
        sites = dict([(s,i) for s,i in list(sites.items()) if i['enabled']])
        pprint.pprint(sites)
        for s,i in list(sites.items()):
            try:
                print(s, i['ddmoses'])
            except:
                pass
        print(len(sites))
        oses = agisparser.getOSMap()
        sites = dict([(s,i) for s,i in list(oses.items())])
        pprint.pprint(sites)
        exit(1)
        time.sleep(10)
