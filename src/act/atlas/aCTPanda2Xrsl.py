import cgi
import json
import os
import re
import time
import uuid


class aCTPanda2Xrsl:

    def __init__(self, pandadbjob, siteinfo, osmap, tmpdir, atlasconf, log):
        self.log = log
        self.pandajob = pandadbjob['pandajob']
        self.jobdesc = cgi.parse_qs(self.pandajob)
        self.pandajobid = pandadbjob['id']
        self.pandaid = self.jobdesc['PandaID'][0]
        self.xrsl = {}
        self.siteinfo = siteinfo
        self.ncores = siteinfo['corecount']
        self.prodSourceLabel = self.jobdesc['prodSourceLabel'][0]

        self.defaults = {}
        self.defaults['memory'] = 2000
        self.defaults['cputime'] = 2*1440*60
        self.sitename = pandadbjob['siteName']
        self.schedconfig = siteinfo['schedconfig']
        self.truepilot = siteinfo['truepilot']
        self.osmap = osmap
        self.maxwalltime = siteinfo['maxwalltime']
        if self.maxwalltime == 0:
            self.maxwalltime = 7*24*60

        self.created = pandadbjob['created']
        self.wrapper = atlasconf.get(["executable", "wrapperurl"])
        self.tmpdir = tmpdir
        self.inputfiledir = os.path.join(self.tmpdir, 'inputfiles')
        self.inputjobdir = os.path.join(self.inputfiledir, self.jobdesc['PandaID'][0])
        self.atlasconf = atlasconf
        self.eventranges = pandadbjob['eventranges']
        self.traces = []
            
        try:
            self.schedulerid = json.loads(pandadbjob['metadata'])['schedulerid']
        except:
            self.schedulerid = atlasconf.get(["panda", "schedulerid"])

        self.rtesites = ["BEIJING-CS-TH-1A_MCORE","BEIJING-ERAII_MCORE","BEIJING-TIANJIN-TH-1A_MCORE","LRZ-LMU_MUC1_MCORE"]#,"LRZ-LMU_MUC_MCORE1"]#"MPPMU-DRACO_MCORE","MPPMU-HYDRA_MCORE"]
        self.atlasrelease = None
        self.monitorurl = atlasconf.get(["monitor", "apfmon"])
        # ES merge jobs need unique guids because pilot uses them as dict keys
        if not self.truepilot and self.jobdesc.has_key('eventServiceMerge') and self.jobdesc['eventServiceMerge'][0] == 'True':
            if self.pandajob.startswith('GUID'):
                esjobdesc = self.pandajob[self.pandajob.find('&'):]
            else:
                esjobdesc = self.pandajob[:self.pandajob.find('&GUID')] + self.pandajob[self.pandajob.find('&', self.pandajob.find('&GUID')+5):]
            esjobdesc += '&GUID=%s' % '%2C'.join(['DUMMYGUID%i' % i for i in range(len(self.jobdesc['GUID'][0].split(',')))])
            self.pandajob = esjobdesc

        #print self.jobdesc.keys()

    def getNCores(self):

        # Unified panda queues: always use coreCount from job description
        try:
            self.ncores = int(self.jobdesc.get('coreCount', [1])[0])
        except: # corecount is NULL
            self.ncores = 1

        self.xrsl['count'] = '(count=%d)' % self.ncores

        # force single-node jobs for now
        if self.ncores > 1:
            self.xrsl['countpernode'] = '(countpernode=%d)' % self.ncores
	    # try if it works without mcore
            #if self.sitename.find('RAL-LCG2') < 0 and self.sitename.find('TOKYO') < 0 and self.sitename.find('FZK') < 0 and self.sitename.find('TRIUMF') < 0:
            #    self.xrsl['countpernode'] = '(runtimeenvironment = APPS/HEP/ATLAS-MULTICORE-1.0)'

        return self.ncores

    def setJobname(self):

        if 'jobName' in self.jobdesc:
            jobname = self.jobdesc['jobName'][0]
        else:
            jobname = "pandajob"
        self.xrsl['jobname'] = '(jobname = "%s")' % jobname

    def setDisk(self):

        if self.sitename not in ['UIO_MCORE', 'ANALY_UIO']:
            return
        # Space for data created by the job
        if 'maxDiskCount' in self.jobdesc:
            disk = int(self.jobdesc['maxDiskCount'][0])
        else:
            disk = 500
        # Add input file sizes
        if 'fsize' in self.jobdesc:
            disk += sum([int(f) for f in self.jobdesc['fsize'][0].split(',')]) / 1000000
        # Add safety factor
        disk += 2000
        self.log.debug('%s: disk space %d' % (self.pandaid, disk))
        self.xrsl['disk'] = "(disk = %d)" % disk

    def setTime(self):

        if 'maxCpuCount' in self.jobdesc:
            cpucount = int(self.jobdesc['maxCpuCount'][0])

            # hack for group production!!!
            if cpucount == 600:
                cpucount = 24*3600

            self.log.info('%s: job maxCpuCount %d' % (self.pandaid, cpucount))
        else:
            cpucount = self.defaults['cputime']
            self.log.info('%s: Using default maxCpuCount %d' % (self.pandaid, cpucount))

        if cpucount <= 0:
            cpucount = self.defaults['cputime']

        walltime = int(cpucount / 60)

        # Jedi underestimates walltime increase by 50% for now
        walltime = walltime * 1.5

        # JEDI analysis hack
        walltime = max(120, walltime)
        walltime = min(self.maxwalltime, walltime)

        cputime = self.getNCores() * walltime
        if self.sitename.startswith('BOINC'):
            walltime = min(240, walltime)
            cputime = walltime

        self.log.info('%s: walltime: %d, cputime: %d, maxtime: %d' % (self.pandaid, walltime, cputime, self.maxwalltime))

        self.xrsl['time'] = '(walltime=%d)(cputime=%d)' % (walltime, cputime)

    def setMemory(self):

        if 'minRamCount' in self.jobdesc:
            memory = int(self.jobdesc['minRamCount'][0])
        elif not self.sitename.startswith('ANALY'):
            memory = 4000
        else:
            memory = 2000

        if memory <= 0:
            memory = self.defaults['memory']

        # fix until maxrrs in pandajob is better known
        if memory <= 1000:
            memory = 1000

        if self.sitename == 'BOINC_MCORE':
            memory = 2400

        # hack mcore pile, use new convention for memory
        # fix memory to 500MB units (AF fix before divide)
        memory = int(memory-1)/500*500 + 500

        if self.getNCores() > 1:
            # hack for 0 ramcount, defaulting to 4000, see above, fix to 2000/core
            if memory == 4000:
                memory = 2000
            else:
                memory = memory / self.getNCores()

        #AF # fix memory to 500MB units
        #AF memory = int(memory-1)/500*500 + 500
        # temp hack
        if self.sitename == 'CSCS-LCG2-HPC_MCORE_TEST':
            memory = 850
        if self.sitename == 'MPPMU_MCORE' and memory < 2000:
            memory = 2000

        self.xrsl['memory'] = '(memory = %d)' % (memory)

    def setRTE(self):

        # Non-RTE setup only requires ATLAS-SITE and possibly ENV/PROXY
        if self.truepilot:
            #self.xrsl['rtes'] = "(runtimeenvironment = ENV/PROXY)(runtimeenvironment = APPS/HEP/ATLAS-SITE-LCG)"
            self.xrsl['rtes'] = "(runtimeenvironment = ENV/PROXY)"
            return
        if self.siteinfo['type'] == 'analysis' and 'BOINC' not in self.sitename:
            self.xrsl['rtes'] = "(runtimeenvironment = ENV/PROXY)(runtimeenvironment = APPS/HEP/ATLAS-SITE)"
            return
        if self.sitename not in self.rtesites:
            self.xrsl['rtes'] = "(runtimeenvironment = APPS/HEP/ATLAS-SITE)"
            return
        
        # Old-style RTE setup
        atlasrtes = []
        for (package, cache) in zip(self.jobdesc['swRelease'][0].split('\n'), self.jobdesc['homepackage'][0].split('\n')):
            if cache.find('Production') > 1 and cache.find('AnalysisTransforms') < 0:
                rte = package.split('-')[0].upper()  + '-' + cache.split('/')[1]
            elif cache.find('AnalysisTransforms') != -1:
                rte = package.upper()
                res = re.match('AnalysisTransforms-(.+)_(.+)', cache)
                if res is not None:
                    if res.group(1).find('AtlasProduction') != -1:
                        rte = "ATLAS-" + res.group(2)
                    else:
                        rte = "ATLAS-" + res.group(1).upper() + "-" + res.group(2)
            else:
                rte = cache.replace('Atlas', 'Atlas-').replace('/', '-').upper()
            rte = str(rte)
            rte = rte.replace('ATLAS-', '')
            rte += "-"+self.jobdesc['cmtConfig'][0].upper()

            if cache.find('AnalysisTransforms') < 0:
                rte = rte.replace('PHYSICS-', 'ATLASPHYSICS-')
                rte = rte.replace('PROD2-', 'ATLASPROD2-')
                rte = rte.replace('PROD1-', 'ATLASPROD1-')
                rte = rte.replace('DERIVATION-', 'ATLASDERIVATION-')
                rte = rte.replace('P1HLT-', 'ATLASP1HLT-')
                rte = rte.replace('TESTHLT-', 'ATLASTESTHLT-')
                rte = rte.replace('CAFHLT-', 'ATLASCAFHLT-')
                rte = rte.replace('21.0.13.1','ATLASPRODUCTION-21.0.13.1')
                rte = rte.replace('21.0.20.1','ATLASPRODUCTION-21.0.20.1')
            if cache.find('AnalysisTransforms') != -1:
                res=re.match(r'(21\..+)',rte)
                if res is not None:
                    rte = rte.replace('21','OFFLINE-21')

            if rte.find('NULL') != -1:
                rte = 'PYTHON-CVMFS-X86_64-SLC6-GCC47-OPT'

            atlasrtes.append(rte)

        self.xrsl['rtes'] = ""
        for rte in atlasrtes[-1:]:
            self.xrsl['rtes'] += "(runtimeenvironment = APPS/HEP/ATLAS-" + rte + ")"

        if self.siteinfo['type'] == 'analysis':
            self.xrsl['rtes'] += "(runtimeenvironment = ENV/PROXY)"

        self.atlasrelease = ",".join(atlasrtes)


    def setExecutable(self):

        self.xrsl['executable'] = "(executable = runpilot3-wrapper.sh)"

    def setArguments(self):

        if self.prodSourceLabel == 'ptest': # pilot2
            pargs = '"-q" "%s" "-r" "%s" "-s" "%s" "-d" "-j" "%s" "--pilot-user" "ATLAS" "-w" "generic"' % (self.schedconfig, self.sitename, self.sitename, self.prodSourceLabel)
            if self.truepilot:
                pargs += ' "--url" "https://pandaserver.cern.ch" "-p" "25443"'
            else:
                pargs += ' "-z" "-t"'
        else:
            pargs = '"-h" "%s" "-s" "%s" "-f" "false" "-u" "%s"' % (self.schedconfig, self.sitename, self.prodSourceLabel)
            if self.truepilot:
                pargs += ' "-p" "25443" "-w" "https://pandaserver.cern.ch"'
            else:
                pargs += ' "-F" "Nordugrid-ATLAS" "-d" "{HOME}" "-j" "false" "-z" "true" "-b" "2" "-t" "false"'

        self.xrsl['arguments'] = '(arguments = %s)' % pargs

        # Panda job hacks for specific sites
        # Commented on request of Rod
        #if self.sitename in ['LRZ-LMU_MUC_MCORE1', 'LRZ-LMU_MUC1_MCORE']:
        if self.sitename in ['IN2P3-CC_HPC_IDRIS_MCORE']:
            self.pandajob = re.sub(r'--DBRelease%3D%22all%3Acurrent%22', '--DBRelease%3D%22100.0.2%22', self.pandajob)

    def setInputsES(self, inf):
        
        for f, s, i in zip (self.jobdesc['inFiles'][0].split(","), self.jobdesc['scopeIn'][0].split(","), self.jobdesc['prodDBlockToken'][0].split(",")):
            if i == 'None':
                # Rucio file
                lfn = '/'.join(["rucio://rucio-lb-prod.cern.ch;rucioaccount=pilot;cache=invariant/replicas", s, f])
            else:
                i = int(i.split("/")[0])
                if i in self.osmap:
                    lfn = '/'.join([self.osmap[i], f])
                else:
                    # TODO this exception is ignored by panda2arc
                    raise Exception("No OS defined in AGIS for bucket id %d" % i)
            inf[f] = lfn

    def setInputs(self):

        x = ""
        # create input file with job desc
        pandaid = self.jobdesc['PandaID'][0]
        try:
            os.makedirs(self.inputjobdir)
        except:
            pass
        tmpfile = self.inputjobdir+"/pandaJobData.out"
        with open(tmpfile, "w") as f:
            f.write(self.pandajob)
        x += '(pandaJobData.out "%s/pandaJobData.out")' % self.inputjobdir

        if self.truepilot:
            if self.prodSourceLabel == 'ptest':
                x += '(runpilot3-wrapper.sh "%s")' % '/data/user/atlact1/act-prod/runpilot2-wrapper.sh'
            else:
                x += '(runpilot3-wrapper.sh "%s")' % self.wrapper
            self.xrsl['inputfiles'] = "(inputfiles =  %s )" % x
            return
        
        if self.eventranges:
            x += '(runpilot3-wrapper.sh "http://aipanda404.cern.ch;cache=check/data/releases/runpilot3-wrapper-es.sh")'      
        else:
            x += '(runpilot3-wrapper.sh "%s")' % self.wrapper

        if self.prodSourceLabel == 'rc_test':
            x += '(pilotcode.tar.gz "http://pandaserver.cern.ch:25080;cache=check/cache/pilot/pilotcode-rc.tar.gz")'
        elif self.prodSourceLabel == 'ptest':
            x += '(pilotcode.tar.gz "http://project-atlas-gmsb.web.cern.ch;cache=check/project-atlas-gmsb/pilotcode-dev.tar.gz")'
        elif self.sitename in ['LRZ-LMU_MUC1_MCORE', 'LRZ-LMU_MUC_MCORE1', 'IN2P3-CC_HPC_IDRIS_MCORE']:
            #x += '(pilotcode.tar.gz "http://wguan-wisc.web.cern.ch;cache=check/wguan-wisc/wguan-pilot-dev-HPC_arc.tar.gz")'
            x += '(pilotcode.tar.gz "http://pandaserver.cern.ch:25080;cache=check/cache/pilot/pilotcode-PICARD.tar.gz")'
        elif self.sitename in self.rtesites:
            x += '(pilotcode.tar.gz "http://aipanda404.cern.ch;cache=check/data/releases/pilotcode-PICARD-RTE.tar.gz")'
        #elif 'BOINC' in self.sitename or (self.sitename.startswith('ANALY') and not self.truepilot) or ('MPPMU' in self.sitename):
            # Use official pilot for BOINC and ND analy queues
            #x += '(pilotcode.tar.gz "http://pandaserver.cern.ch:25080;cache=check/cache/pilot/pilotcode-PICARD.tar.gz")'
            #x += '(pilotcode.tar.gz "http://aipanda404.cern.ch;cache=check/data/releases/pilotcode-PICARD-NOKILL.tar.gz")'
        else:
            # Use no kill pilot to avoid pilot killing too much at end of job
            # Paul's version using PILOT_NOKILL
            #x += '(pilotcode.tar.gz "http://project-atlas-gmsb.web.cern.ch;cache=check/project-atlas-gmsb/pilotcode-PICARD-72.12-NORDUGRID.tar.gz")'
            x += '(pilotcode.tar.gz "http://aipanda404.cern.ch;cache=check/data/releases/pilotcode-PICARD-NOKILL.tar.gz")'
            #x += '(pilotcode.tar.gz "http://pandaserver.cern.ch:25080;cache=check/cache/pilot/pilotcode-PICARD.tar.gz")'
            #x += '(pilotcode.tar.gz "http://aipanda404.cern.ch;cache=check/data/releases/pilotcode-PICARD-AF.tar.gz")'
            #x += '(pilotcode.tar.gz "http://dcameron.web.cern.ch;cache=check/dcameron/dev/pilotcode-DC.tar.gz")'

        # Special HPCs which cannot get agis files from cvmfs or over network
        if re.match('BEIJING-.*_MCORE', self.sitename):
            x += '(agis_schedconf.agis.%s.json "http://atlas-agis-api.cern.ch/request/pandaqueue/query/list/?json&preset=schedconf.all&panda_queue=%s")' % (self.sitename,self.sitename)
            x += '(agis_ddmendpoints.json "/cvmfs/atlas.cern.ch/repo/sw/local/etc/agis_ddmendpoints.json")'
            x += '(agis_ddmblacklisting.json "/cvmfs/atlas.cern.ch/repo/sw/local/etc/agis_ddmblacklisting.json")'
            x += '(agis_schedconf.cvmfs.json "/cvmfs/atlas.cern.ch/repo/sw/local/etc/agis_schedconf.json")'

        if self.eventranges:
            x += '(ARCpilot-test.tar.gz "http://aipanda404.cern.ch;cache=check/data/releases/ARCpilot-es.tar.gz")'
        x += '(queuedata.json "http://pandaserver.cern.ch:25085;cache=check/cache/schedconfig/%s.all.json")' % self.schedconfig

        if 'inFiles' in self.jobdesc:
            inf = {}
            if self.jobdesc.has_key('eventServiceMerge') and self.jobdesc['eventServiceMerge'][0] == 'True':
                self.setInputsES(inf)

            for filename, scope, dsn, guid, token, ddmin in zip(self.jobdesc['inFiles'][0].split(","),
                                                                self.jobdesc['scopeIn'][0].split(","),
                                                                self.jobdesc['realDatasetsIn'][0].split(","),
                                                                self.jobdesc['GUID'][0].split(","),
                                                                self.jobdesc['prodDBlockToken'][0].split(","),
                                                                self.jobdesc['ddmEndPointIn'][0].split(",")):

                # Skip files which use direct I/O: site has it enabled, token is
                # not 'local', file is root file and --useLocalIO is not used

                # don't use direct I/O - pending new mover switch
                #if token != 'local' and self.siteinfo.get('direct_access_lan', False) and \
                #  not ('.tar.gz' in filename or '.lib.tgz' in filename or '.raw.' in filename) and \
                #  '--useLocalIO' not in self.jobdesc['jobPars'][0]:
                #    continue
                # Hard-coded pilot rucio account - should change based on proxy
                # Rucio does not expose mtime, set cache=invariant so not to download too much
                if self.sitename in []:
                    lfn = '/'.join(["rucio://rucio-lb-prod.cern.ch;rucioaccount=pilot;transferprotocol=https;httpgetpartial=no;cache=invariant/replicas", scope, filename]) 
                else:
                    lfn = '/'.join(["rucio://rucio-lb-prod.cern.ch;rucioaccount=pilot;cache=invariant/replicas", scope, filename])
                inf[filename] = lfn
                dn = self.jobdesc.get('prodUserID', [])
                eventType = 'get_sm'
                if re.match('user', self.prodSourceLabel):
                    eventType = 'get_sm_a'
                self.traces.append({'uuid': str(uuid.uuid4()),
                                    'scope': scope,
                                    'filename': filename,
                                    'dataset': dsn,
                                    'guid': guid,
                                    'eventVersion': 'aCT',
                                    'timeStart': time.time(),
                                    'usrdn': dn[0],
                                    'localSite': ddmin,
                                    'remoteSite': ddmin,
                                    'eventType': eventType})

            # some files are double:
            for k, v in inf.items():
                x += "(" + k + " " + '"' + v + '"' + ")"
        
            if self.jobdesc.has_key('eventService') and self.jobdesc['eventService'] and self.eventranges:
                # Create tmp json file to upload with job
                pandaid = self.jobdesc['PandaID'][0]
                tmpjsonfile = os.path.join(self.tmpdir, 'eventranges', str('%s.json' % pandaid))
                jsondata = json.loads(self.eventranges)
                with open(tmpjsonfile, 'w') as f:
                    json.dump(jsondata, f)
                x += '("eventranges.json" "%s")' %  tmpjsonfile
            
        self.xrsl['inputfiles'] = "(inputfiles =  %s )" % x

    def setLog(self):

        if 'logFile' in self.jobdesc:
            logfile = self.jobdesc['logFile'][0]
        else:
            logfile = "LOGFILE"

        self.xrsl['log'] = '(stdout = "' + logfile.replace('.tgz', '') + '")(join = yes)'

    def setGMLog(self):

        self.xrsl['gmlog'] = '("gmlog" = "gmlog")'
        self.xrsl['rerun'] = '("rerun" = "2")'

    def setOutputs(self):

        # dynamic outputs

        output = '("jobSmallFiles.tgz" "")'
        output += '("@output.list" "")'
        # needed for SCEAPI
        # generated output file list"
        output += '("output.list" "")'
        self.xrsl['outputs'] = "(outputfiles = %s )" % output

        if self.truepilot:
            self.xrsl['outputs'] = ""

    def setPriority(self):

        if 'currentPriority' in self.jobdesc:
            #self.xrsl['priority'] = '("priority" = ' + str(int(self.jobdesc['currentPriority'][0])/100) + ')'
            # need a better number
            prio = 50
            try:
                prio = int(self.jobdesc['currentPriority'][0])
                if prio < 1:
                    prio = 1
                if prio > 0 and prio < 1001:
                    prio = prio * 90 / 1000.
                    prio = int(prio)
                if prio > 1000 and prio < 10001:
                    prio = 90 + (prio - 1000) / 900.
                    prio = int(prio)
                if prio > 10000:
                    prio = 100
            except:
                pass
            #self.xrsl['priority'] = '("priority" = 60 )'
            self.xrsl['priority'] = '("priority" = %d )' % prio
            if self.sitename == 'wuppertalprod_MCORE':
                self.xrsl['priority'] = ""
            if self.sitename == 'wuppertalprod':
                self.xrsl['priority'] = ""
            if self.sitename == 'wuppertalprod_HI':
                self.xrsl['priority'] = ""
            if self.sitename == 'ANALY_wuppertalprod':
                self.xrsl['priority'] = ""

    def setEnvironment(self):

        # Set schedulerID and job log URL
        environment = {}
        environment['PANDA_JSID'] = self.schedulerid
        schedurl = self.atlasconf.get(["joblog", "urlprefix"])
        environment['GTAG'] = '%s/%s/%s/%s.out' % (schedurl, self.created.strftime('%Y-%m-%d'), self.sitename, self.pandaid)

        # ATLAS_RELEASE for RTE sites
        if self.atlasrelease:
            environment['ATLAS_RELEASE'] = self.atlasrelease

        # Vars for APFMon (truepilot only)
        if self.truepilot and self.monitorurl:
            environment['APFCID'] = self.pandajobid
            # harvester prepends "harvester-" to the schedulerid but APFMon uses the original one
            environment['APFFID'] = self.schedulerid.replace("harvester-","")
            environment['APFMON'] = self.monitorurl
            environment['FACTORYQUEUE'] = self.sitename

        environment['PILOT_NOKILL'] = 'YES'
        self.xrsl['environment'] = '(environment = %s)' % ''.join(['("%s" "%s")' % (k,v) for (k,v) in environment.items()])

    def parse(self):
        self.setTime()
        self.setJobname()
        self.setDisk()
        self.setMemory()
        self.setRTE()
        self.setExecutable()
        self.setArguments()
        self.setInputs()
        self.setLog()
        self.setGMLog()
        self.setOutputs()
        self.setPriority()
        self.setEnvironment()

    def getXrsl(self):
        x = "&"
        for k in self.xrsl.keys():
            x += self.xrsl[k] + "\n"
        return x


if __name__ == '__main__':
    from act.common.aCTLogger import aCTLogger
    from act.common.aCTConfig import aCTConfigATLAS
    from datetime import datetime
    logger=aCTLogger('test')
    log=logger()
    pandajob = "jobsetID=799&logGUID=5ba37307-e4d7-4224-82f9-ff0503622677&cmtConfig=x86_64-slc6-gcc48-opt&prodDBlocks=user.rwatari%3Auser.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_RDO2RDOFTK_v01_all1E5ev_EXT2.99328897%2Cpanda.1110091801.467362.lib._9845189&dispatchDBlockTokenForOut=NULL%2CNULL%2CNULL&destinationDBlockToken=NULL%2CNULL%2CNULL&destinationSE=NULL&realDatasets=user.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev_EXT0%2F%2Cuser.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev_EXT1%2F%2Cuser.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev.log%2F&prodUserID=%2FDC%3Dch%2FDC%3Dcern%2FOU%3DOrganic+Units%2FOU%3DUsers%2FCN%3Drwatari%2FCN%3D764796%2FCN%3DRyutaro+Watari%2FCN%3Dproxy&GUID=51997D0A-850A-9044-A264-83A8986FE1C6%2C1de48e07-f37c-43e6-a343-3947342858b1&realDatasetsIn=user.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_RDO2RDOFTK_v01_all1E5ev_EXT2%2Cpanda.1110091801.467362.lib._9845189&nSent=0&cloud=ND&StatusCode=0&homepackage=AnalysisTransforms-AtlasProduction_20.7.3.7&inFiles=user.rwatari.9557718.EXT2._000016.RDO_FTK.pool.root%2Cpanda.1110091801.467362.lib._9845189.7456421499.lib.tgz&processingType=panda-client-0.5.69-jedi-athena-trf&currentPriority=814&fsize=1140292964%2C727003478&fileDestinationSE=ANALY_SiGNET_DIRECT%2CANALY_SiGNET_DIRECT%2CANALY_SiGNET_DIRECT&scopeOut=user.rwatari%2Cuser.rwatari&minRamCount=4772&jobDefinitionID=836&scopeLog=user.rwatari&transformation=http%3A%2F%2Fpandaserver.cern.ch%3A25085%2Ftrf%2Fuser%2FrunAthena-00-00-12&maxDiskCount=3167&coreCount=1&prodDBlockToken=NULL%2CNULL&transferType=NULL&destinationDblock=user.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev_EXT0.104826316_sub0341667607%2Cuser.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev_EXT1.104826317_sub0341667608%2Cuser.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev.log.104826315_sub0341667610&dispatchDBlockToken=NULL%2CNULL&jobPars=-l+panda.1110091801.467362.lib._9845189.7456421499.lib.tgz+--sourceURL+https%3A%2F%2Faipanda078.cern.ch%3A25443+-r+WorkArea%2Frun%2Ffast%2F+--trf+--useLocalIO++-i+%22%5B%27user.rwatari.9557718.EXT2._000016.RDO_FTK.pool.root%27%5D%22+-o+%22%7B%27IROOT%27%3A+%5B%28%27InDetDxAOD.pool.root%27%2C+%27user.rwatari.9845189.EXT0._002324.InDetDxAOD.pool.root%27%29%2C+%28%27esd.pool.root%27%2C+%27user.rwatari.9845189.EXT1._002324.esd.pool.root%27%29%5D%7D%22++-j+%22Reco_tf.py%2520--inputRDOFile%253Duser.rwatari.9557718.EXT2._000016.RDO_FTK.pool.root%2520--outputESDFile%253Desd.pool.root%2520%2520--doAllNoise%2520False%2520--autoConfiguration%253Deverything%2520--numberOfCavernBkg%253D0%2520--postInclude%253DFTKFastSim%2FInDetDxAOD.py%2520--preExec%2520%2527rec.UserAlgs%253D%255B%2522FTKFastSim%2FFTKFastSimulation_jobOptions.py%2522%255D%253Brec.doCalo.set_Value_and_Lock%2528False%2529%253Brec.doMuon.set_Value_and_Lock%2528False%2529%253Brec.doJetMissingETTag.set_Value_and_Lock%2528False%2529%253Brec.doEgamma.set_Value_and_Lock%2528False%2529%253Brec.doMuonCombined.set_Value_and_Lock%2528False%2529%253Brec.doTau.set_Value_and_Lock%2528False%2529%253Brec.doTrigger.set_Value_and_Lock%2528False%2529%253Brec.doFTK.set_Value_and_Lock%2528True%2529%253Bfrom%2520AthenaCommon.DetFlags%2520import%2520DetFlags%253BDetFlags.all_setOn%2528%2529%253BDetFlags.FTK_setOn%2528%2529%2527%2520--maxEvents%253D-1%2520--postExec%2520r2e%253A%2520%2527ServiceMgr%252B%253DService%2528%2522BeamCondSvc%2522%2529%253BbeamCondSvc%253DServiceMgr.BeamCondSvc%253BbeamCondSvc.useDB%253DFalse%253BbeamCondSvc.posX%253D-0.0497705%253BbeamCondSvc.posY%253D1.06299%253BbeamCondSvc.posZ%253D0.0%253BbeamCondSvc.sigmaX%253D0.0251281%253BbeamCondSvc.sigmaY%253D0.0231978%253BbeamCondSvc.sigmaZ%253D0.1%253BbeamCondSvc.sigmaXY%253D-2.7745e-06%253BbeamCondSvc.tiltX%253D-1.51489e-05%253BbeamCondSvc.tiltY%253D-4.83891e-05%253B%2527%22&attemptNr=2&swRelease=Atlas-20.7.3&nucleus=NULL&maxCpuCount=0&outFiles=user.rwatari.9845189.EXT0._002324.InDetDxAOD.pool.root%2Cuser.rwatari.9845189.EXT1._002324.esd.pool.root%2Cuser.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev.log.9845189.002324.log.tgz&ddmEndPointOut=NDGF-T1_SCRATCHDISK%2CNDGF-T1_SCRATCHDISK%2CNDGF-T1_SCRATCHDISK&scopeIn=user.rwatari%2Cpanda&PandaID=3072596651&sourceSite=NULL&dispatchDblock=NULL%2Cpanda.1110091801.467362.lib._9845189&prodSourceLabel=user&checksum=ad%3Afd1c3aac%2Cad%3A516b31b3&jobName=user.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev%2F.3071213044&ddmEndPointIn=NDGF-T1_SCRATCHDISK%2CNDGF-T1_SCRATCHDISK&taskID=9845189&logFile=user.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev.log.9845189.002324.log.tgz"
    siteinfo = {'schedconfig': 'ANALY_SiGNET_DIRECT', 'corecount': 1, 'truepilot': False, 'maxwalltime': 10800, 'direct_access_lan': True, 'type': 'analysis'}
    conf = aCTConfigATLAS()
    pandadbjob = {'pandajob': pandajob, 'siteName': 'ANALY_SiGNET_DIRECT', 'eventranges': None, 'metadata': {}, 'created': datetime.utcnow()}
    a = aCTPanda2Xrsl(pandadbjob, siteinfo, {}, '/tmp', conf, log)
    a.parse()
    print a.getXrsl()
