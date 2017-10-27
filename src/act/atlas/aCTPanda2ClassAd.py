import cgi
import json
import os
import re
import time
import uuid
from classad import ClassAd


class aCTPanda2ClassAd:

    def __init__(self, pandajob, sitename, siteinfo, osmap, tmpdir, eventranges, log):
        self.classad = ClassAd()
        self.log = log
        self.pandajob = pandajob
        self.jobdesc = cgi.parse_qs(pandajob)
        self.pandaid = self.jobdesc['PandaID'][0]
        self.siteinfo = siteinfo
        self.ncores = siteinfo['corecount']

        self.defaults = {}
        self.defaults['memory'] = 2000
        self.defaults['cputime'] = 2*24*3600
        self.sitename = sitename
        self.schedconfig = siteinfo['schedconfig']
        self.maxwalltime = siteinfo['maxwalltime'] * 60
        if self.maxwalltime == 0:
            self.maxwalltime = 7*24*3600

        self.tmpdir = tmpdir
        self.inputfiledir = os.path.join(self.tmpdir, 'inputfiles')
        self.inputjobdir = os.path.join(self.inputfiledir, self.jobdesc['PandaID'][0])
        self.eventranges = eventranges
        self.longjob = False
        self.traces = []
        if len(self.pandajob) > 50000:
            self.longjob = True

    def getNCores(self):

        # Unified panda queues: always use coreCount from job description
        try:
            self.ncores = int(self.jobdesc.get('coreCount', [1])[0])
        except: # corecount is NULL
            self.ncores = 1

        self.classad['RequestCpus'] = self.ncores
        return self.ncores

    def setDisk(self):

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
        self.classad['RequestDisk'] = disk * 1024 # number in KiB

    def setTime(self):

        if 'maxCpuCount' in self.jobdesc:
            cpucount = int(self.jobdesc['maxCpuCount'][0])
            cpucount = int(2 * cpucount)
            self.log.info('%s: job maxCpuCount %s' % (self.pandaid, cpucount))
        else:
            cpucount = self.defaults['cputime']
            self.log.info('%s: Using default maxCpuCount %s' % (self.pandaid, cpucount))

        if cpucount == 0:
            cpucount = self.defaults['cputime']

        walltime = cpucount

        # Give jobs at least 1 hour
        walltime = max(3600, walltime)
        # .. and at most 1 week
        walltime = min(self.maxwalltime, walltime)

        cputime = self.getNCores() * walltime
        self.log.info('%s: walltime: %ds, cputime: %ds' % (self.pandaid, walltime, cputime))

        self.classad['MaxRuntime'] = walltime

    def setMemory(self):

        if 'minRamCount' in self.jobdesc:
            memory = int(self.jobdesc['minRamCount'][0])
        elif not self.sitename.startswith('ANALY'):
            memory = 4000
        else:
            memory = 2000

        if memory <= 0:
            memory = self.defaults['memory']

        # fix until maxrss in pandajob is better known
        if memory <= 500:
            memory = 500

        if self.getNCores() > 1:
            # hack for 0 ramcount, defaulting to 4000, see above, fix to 2000/core
            if memory == 4000:
                memory = 2000
            else:
                memory = memory / self.getNCores()

        # fix memory to 500MB units
        memory = int(memory-1)/500*500 + 500

        self.classad['RequestMemory'] = memory

    def setExecutable(self):

        self.classad['Cmd'] = "runpilot3-wrapper.sh"

    def setArguments(self):

        pargs = 'pilot.py -h %s -s %s -f false -p 25443 -d {HOME} -w https://pandaserver.cern.ch' % (self.schedconfig, self.sitename)

        pandajobarg = self.pandajob
        if self.longjob:
            pandajobarg = "FILE"
        self.classad['Arguments'] = '%s %s' (pandajobarg, pargs)

    def setInputs(self):

        self.classad['']
        x = ""
        if self.truepilot:
            x += '(ARCpilot "http://voatlas404.cern.ch;cache=check/data/data/ARCpilot-true")'
        elif self.eventranges:
            x += '(ARCpilot "http://voatlas404.cern.ch;cache=check/data/data/ARCpilot-es")'      
        else:
            x += '(ARCpilot "http://voatlas404.cern.ch;cache=check/data/data/ARCpilot")'

        if self.jobdesc['prodSourceLabel'][0] == 'rc_test':
            x += '(pilotcode.tar.gz "http://pandaserver.cern.ch:25080;cache=check/cache/pilot/pilotcode-rc.tar.gz")'
        #elif self.eventranges: # ES job
        #    x += '(pilotcode.tar.gz "http://wguan-wisc.web.cern.ch;cache=check/wguan-wisc/wguan-pilot-dev-HPC_arc.tar.gz")'
        else:
            x += '(pilotcode.tar.gz "http://pandaserver.cern.ch:25080;cache=check/cache/pilot/pilotcode-PICARD.tar.gz")'

        if self.eventranges:
            x += '(ARCpilot-test.tar.gz "http://voatlas404.cern.ch;cache=check/data/data/ARCpilot-es.tar.gz")'
        else:
            x += '(ARCpilot-test.tar.gz "http://voatlas404.cern.ch;cache=check/data/data/ARCpilot.tar.gz")'

        if self.longjob:
            # TODO create input file
            pandaid = self.jobdesc['PandaID'][0]
            try:
                os.makedirs(self.inputjobdir)
            except:
                pass
            tmpfile = self.inputjobdir+"/pandaJobData.out"
            f = open(tmpfile, "w")
            f.write(self.pandajob)
            f.close()
            x += '(pandaJobData.out "%s/pandaJobData.out")' % self.inputjobdir

        if not self.truepilot:
            x += '(queuedata.pilot.json "http://pandaserver.cern.ch:25085;cache=check/cache/schedconfig/%s.all.json")' % self.schedconfig

        if 'inFiles' in self.jobdesc and not self.truepilot:
            inf = {}
            if self.jobdesc.has_key('eventServiceMerge') and self.jobdesc['eventServiceMerge'][0] == 'True':
                self.setInputsES(inf)

            for filename, scope, dsn, guid, token in zip(self.jobdesc['inFiles'][0].split(","),
                                                         self.jobdesc['scopeIn'][0].split(","),
                                                         self.jobdesc['realDatasetsIn'][0].split(","),
                                                         self.jobdesc['GUID'][0].split(","),
                                                         self.jobdesc['prodDBlockToken'][0].split(",")):

                # Skip files which use direct I/O: site has it enabled, token is
                # not 'local', file is root file and --useLocalIO is not used

                # don't use direct I/O - pending new mover switch
                #if token != 'local' and self.siteinfo.get('direct_access_lan', False) and \
                #  not ('.tar.gz' in filename or '.lib.tgz' in filename or '.raw.' in filename) and \
                #  '--useLocalIO' not in self.jobdesc['jobPars'][0]:
                #    continue
                # Hard-coded pilot rucio account - should change based on proxy
                # Rucio does not expose mtime, set cache=invariant so not to download too much
                lfn = '/'.join(["rucio://rucio-lb-prod.cern.ch;rucioaccount=pilot;transferprotocol=gsiftp;cache=invariant/replicas", scope, filename])
                # lfn='/'.join(["rucio://rucio-lb-prod.cern.ch;rucioaccount=pilot;transferprotocol=gsiftp,https;cache=invariant/replicas", scope, filename])
                # lfn='/'.join(["rucio://rucio-lb-prod.cern.ch;rucioaccount=pilot;transferprotocol=https,gsiftp;cache=invariant/replicas", scope, filename])
                inf[filename] = lfn
                dn = self.jobdesc.get('prodUserID', [])
                prodSourceLabel = self.jobdesc.get('prodSourceLabel', [''])[0]
                eventType = 'get_sm'
                if re.match('user', prodSourceLabel):
                    eventType = 'get_sm_a'
                self.traces.append({'uuid': str(uuid.uuid4()), 'scope': scope, 'filename': filename, 'dataset': dsn, 'guid': guid, 'eventVersion': 'aCT', 'timeStart': time.time(), 'usrdn': dn[0], 'eventType': eventType})

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
        schedid = self.atlasconf.get(["panda", "schedulerid"])
        schedurl = self.atlasconf.get(["joblog", "urlprefix"])
        
        environment = '"PANDA_JSID=%s' % schedid
        environment += ' GTAG=%s/$(Cluster).$(Process).out' % schedurl
        
        # Vars for APFMon
        environment += ' APFCID=$(Cluster).$(Process)'
        environment += ' APFFID=%s' % schedid
        if self.monitorurl:
            environment += ' APFMON=%s' % self.monitorurl
        environment += ' FACTORYQUEUE=%s' % self.sitename
        if self.factoryuser:
            environment += ' FACTORYUSER=%s' % self.factoryuser
        environment += '"'

        self.classad['Environment'] = environment

    def parse(self):
        self.setTime()
        #self.setDisk()
        self.setMemory()
        self.setExecutable()
        self.setArguments()
        self.setInputs()
        self.setLog()
        self.setOutputs()
        self.setPriority()
        self.setEnvironment()

    def getClassAd(self):
        return self.classad


if __name__ == '__main__':
    from act.common.aCTLogger import aCTLogger
    logger=aCTLogger('test')
    log=logger()
    pandajob = "jobsetID=799&logGUID=5ba37307-e4d7-4224-82f9-ff0503622677&cmtConfig=x86_64-slc6-gcc48-opt&prodDBlocks=user.rwatari%3Auser.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_RDO2RDOFTK_v01_all1E5ev_EXT2.99328897%2Cpanda.1110091801.467362.lib._9845189&dispatchDBlockTokenForOut=NULL%2CNULL%2CNULL&destinationDBlockToken=NULL%2CNULL%2CNULL&destinationSE=NULL&realDatasets=user.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev_EXT0%2F%2Cuser.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev_EXT1%2F%2Cuser.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev.log%2F&prodUserID=%2FDC%3Dch%2FDC%3Dcern%2FOU%3DOrganic+Units%2FOU%3DUsers%2FCN%3Drwatari%2FCN%3D764796%2FCN%3DRyutaro+Watari%2FCN%3Dproxy&GUID=51997D0A-850A-9044-A264-83A8986FE1C6%2C1de48e07-f37c-43e6-a343-3947342858b1&realDatasetsIn=user.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_RDO2RDOFTK_v01_all1E5ev_EXT2%2Cpanda.1110091801.467362.lib._9845189&nSent=0&cloud=ND&StatusCode=0&homepackage=AnalysisTransforms-AtlasProduction_20.7.3.7&inFiles=user.rwatari.9557718.EXT2._000016.RDO_FTK.pool.root%2Cpanda.1110091801.467362.lib._9845189.7456421499.lib.tgz&processingType=panda-client-0.5.69-jedi-athena-trf&currentPriority=814&fsize=1140292964%2C727003478&fileDestinationSE=ANALY_SiGNET_DIRECT%2CANALY_SiGNET_DIRECT%2CANALY_SiGNET_DIRECT&scopeOut=user.rwatari%2Cuser.rwatari&minRamCount=4772&jobDefinitionID=836&scopeLog=user.rwatari&transformation=http%3A%2F%2Fpandaserver.cern.ch%3A25085%2Ftrf%2Fuser%2FrunAthena-00-00-12&maxDiskCount=3167&coreCount=1&prodDBlockToken=NULL%2CNULL&transferType=NULL&destinationDblock=user.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev_EXT0.104826316_sub0341667607%2Cuser.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev_EXT1.104826317_sub0341667608%2Cuser.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev.log.104826315_sub0341667610&dispatchDBlockToken=NULL%2CNULL&jobPars=-l+panda.1110091801.467362.lib._9845189.7456421499.lib.tgz+--sourceURL+https%3A%2F%2Faipanda078.cern.ch%3A25443+-r+WorkArea%2Frun%2Ffast%2F+--trf+--useLocalIO++-i+%22%5B%27user.rwatari.9557718.EXT2._000016.RDO_FTK.pool.root%27%5D%22+-o+%22%7B%27IROOT%27%3A+%5B%28%27InDetDxAOD.pool.root%27%2C+%27user.rwatari.9845189.EXT0._002324.InDetDxAOD.pool.root%27%29%2C+%28%27esd.pool.root%27%2C+%27user.rwatari.9845189.EXT1._002324.esd.pool.root%27%29%5D%7D%22++-j+%22Reco_tf.py%2520--inputRDOFile%253Duser.rwatari.9557718.EXT2._000016.RDO_FTK.pool.root%2520--outputESDFile%253Desd.pool.root%2520%2520--doAllNoise%2520False%2520--autoConfiguration%253Deverything%2520--numberOfCavernBkg%253D0%2520--postInclude%253DFTKFastSim%2FInDetDxAOD.py%2520--preExec%2520%2527rec.UserAlgs%253D%255B%2522FTKFastSim%2FFTKFastSimulation_jobOptions.py%2522%255D%253Brec.doCalo.set_Value_and_Lock%2528False%2529%253Brec.doMuon.set_Value_and_Lock%2528False%2529%253Brec.doJetMissingETTag.set_Value_and_Lock%2528False%2529%253Brec.doEgamma.set_Value_and_Lock%2528False%2529%253Brec.doMuonCombined.set_Value_and_Lock%2528False%2529%253Brec.doTau.set_Value_and_Lock%2528False%2529%253Brec.doTrigger.set_Value_and_Lock%2528False%2529%253Brec.doFTK.set_Value_and_Lock%2528True%2529%253Bfrom%2520AthenaCommon.DetFlags%2520import%2520DetFlags%253BDetFlags.all_setOn%2528%2529%253BDetFlags.FTK_setOn%2528%2529%2527%2520--maxEvents%253D-1%2520--postExec%2520r2e%253A%2520%2527ServiceMgr%252B%253DService%2528%2522BeamCondSvc%2522%2529%253BbeamCondSvc%253DServiceMgr.BeamCondSvc%253BbeamCondSvc.useDB%253DFalse%253BbeamCondSvc.posX%253D-0.0497705%253BbeamCondSvc.posY%253D1.06299%253BbeamCondSvc.posZ%253D0.0%253BbeamCondSvc.sigmaX%253D0.0251281%253BbeamCondSvc.sigmaY%253D0.0231978%253BbeamCondSvc.sigmaZ%253D0.1%253BbeamCondSvc.sigmaXY%253D-2.7745e-06%253BbeamCondSvc.tiltX%253D-1.51489e-05%253BbeamCondSvc.tiltY%253D-4.83891e-05%253B%2527%22&attemptNr=2&swRelease=Atlas-20.7.3&nucleus=NULL&maxCpuCount=0&outFiles=user.rwatari.9845189.EXT0._002324.InDetDxAOD.pool.root%2Cuser.rwatari.9845189.EXT1._002324.esd.pool.root%2Cuser.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev.log.9845189.002324.log.tgz&ddmEndPointOut=NDGF-T1_SCRATCHDISK%2CNDGF-T1_SCRATCHDISK%2CNDGF-T1_SCRATCHDISK&scopeIn=user.rwatari%2Cpanda&PandaID=3072596651&sourceSite=NULL&dispatchDblock=NULL%2Cpanda.1110091801.467362.lib._9845189&prodSourceLabel=user&checksum=ad%3Afd1c3aac%2Cad%3A516b31b3&jobName=user.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev%2F.3071213044&ddmEndPointIn=NDGF-T1_SCRATCHDISK%2CNDGF-T1_SCRATCHDISK&taskID=9845189&logFile=user.rwatari.1k_10mu.xm005_yp106.RDO.20161003_2_EXT0_PseduoTracking_v14_all1E5ev.log.9845189.002324.log.tgz"
    siteinfo = {'schedconfig': 'ANALY_SiGNET_DIRECT', 'corecount': 1, 'truepilot': True, 'maxwalltime': 10800, 'direct_access_lan': True, 'type': 'analysis'}
    a = aCTPanda2ClassAd(pandajob, 'ANALY_SiGNET_DIRECT', siteinfo, {}, '/tmp', None, log)
    a.parse()
    print a.getClassAd()
