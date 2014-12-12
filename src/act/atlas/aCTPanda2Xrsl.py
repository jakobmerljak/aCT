import cgi
import re

class aCTPanda2Xrsl:

    def __init__(self,pandajob,sitename,schedconfig,catalog,corecount=1,truepilot=0):
        self.pandajob=pandajob
        self.jobdesc = cgi.parse_qs(pandajob)
        self.xrsl={}
        # self.ncores = 0
        # use schedconfig/xml value for now
        self.ncores = corecount

        self.defaults={}
        self.defaults['memory'] = 2000
        self.defaults['cputime'] = 2*1440*60
        self.sitename=sitename
        self.schedconfig=schedconfig
        self.catalog = catalog
        self.truepilot = truepilot

        #print self.jobdesc.keys()

    def getNCores(self):

        if self.ncores == 0:
            if self.jobdesc.has_key('CoreCount'):
                self.ncores = int(self.jobdesc['CoreCount'][0])
            else:
                self.ncores=1        

        self.xrsl['count']='(count=%d)' % self.ncores

        # force single-node jobs for now
        if self.ncores > 1:
            #self.xrsl['countpernode'] = '(countpernode=%d)' % self.ncores
            self.xrsl['countpernode'] = '(runtimeenvironment = APPS/HEP/ATLAS-MULTICORE-1.0)'

        return self.ncores


    def setJobname(self):

        if self.jobdesc.has_key('jobName'):
            jobname = self.jobdesc['jobName'][0]
        else:
            jobname = "pandajob"
        self.xrsl['jobname'] = '(jobname = "%s")' % jobname

    def setDisk(self):

        if self.jobdesc.has_key('maxDiskCount'):
            disk=int(self.jobdesc['maxDiskCount'][0])
        else:
            disk=500
        self.xrsl['disk']= "(disk = %d)" % disk


    def setTime(self):

        if self.jobdesc.has_key('maxCpuCount'):
            cpucount = int(self.jobdesc['maxCpuCount'][0])
        else:
            cpucount = 2*24*3600

        # JEDI issues
        if cpucount > 345600:
            cpucount = 345600

        # shorten installation jobs
        try:
            if self.jobdesc['prodSourceLabel'][0] == 'install':
                cpucount = 6*3600
        except:
            pass

        if int(cpucount) <= 0 :
            cpucount = self.defaults['cputime']

        walltime = int( cpucount  / 60)

        if self.getNCores() > 1:
            walltime = int (walltime / self.getNCores() ) + 60

        #if self.sitename.find("MPPMU-HYDRA_MCORE") != -1:
        #    walltime=90

        # JEDI analysis hack
        walltime = max(60,walltime)
        cputime = self.getNCores() * walltime
        

        self.xrsl['time']='(walltime=%d)(cputime=%d)' % (walltime,cputime)

    def setMemory(self):

        
        if self.jobdesc.has_key('minRamCount'):
            memory = int(self.jobdesc['minRamCount'][0])
        else:
            memory = 2000

        if memory <= 0:
            memory = self.defaults['memory']

        if self.sitename == 'BOINC':
            memory=1536

        # hack mcore pile
        if self.getNCores() > 1 and memory > 2500:
            memory=2500

        self.xrsl['memory']='(memory = %d)' % (memory)


    def setRTE(self):

        atlasrtes = []
        for (package,cache) in zip(self.jobdesc['swRelease'][0].split('\n'),self.jobdesc['homepackage'][0].split('\n')):
            if cache.find('Production') > 1 and cache.find('AnalysisTransforms') < 0 :
                rte = package.split('-')[0].upper()  + '-' \
                      + cache.split('/')[1]
            elif cache.find('AnalysisTransforms') != -1:
                rte=package.upper()
    	        res=re.match('AnalysisTransforms-(.+)_(.+)',cache)
                if res is not None:
    	            if res.group(1).find('AtlasProduction') != -1:
                        rte="ATLAS-"+res.group(2)
                    else:
                        rte="ATLAS-"+res.group(1).upper()+"-"+res.group(2)
            else:
                rte=cache.replace('Atlas','Atlas-').replace('/','-').upper()
            rte=str(rte)
            rte=rte.replace('ATLAS-','')
            rte += "-"+self.jobdesc['cmtConfig'][0].upper()

            if cache.find('AnalysisTransforms') < 0 :
                rte=rte.replace('PHYSICS-','ATLASPHYSICS-')
                rte=rte.replace('PROD2-','ATLASPROD2-')
                rte=rte.replace('PROD1-','ATLASPROD1-')
                rte=rte.replace('DERIVATION-','ATLASDERIVATION-')

            if rte.find('NULL') != -1:
                rte='PYTHON-CVMFS-X86_64-SLC6-GCC47-OPT'

            atlasrtes.append(rte)


        self.xrsl['rtes']=""
        for rte in atlasrtes[-1:]:
            self.xrsl['rtes'] += "(runtimeenvironment = APPS/HEP/ATLAS-" + rte + ")"


        self.artes = ",".join(atlasrtes)
        
        # Set proxy environment for truepilot jobs
        if self.truepilot:
            self.artes = ""
            self.xrsl['rtes'] = "(runtimeenvironment = ENV/PROXY)(runtimeenvironment = APPS/HEP/ATLAS-SITE-LCG)"

    def setExecutable(self):

        self.xrsl['executable'] = "(executable = ARCpilot-test)"

    def setArguments(self):
        
        if self.artes is None:
                self.setRTE()

        # Set options for NG/true pilot
        #if truepilot:

        #pargs='"pilot3/pilot.py" "-h" "NDGF-condor" "-s" "Nordugrid" "-F" "Nordugrid-ATLAS" "-d" "{HOME}" "-j" "false" "-f" "false" "-z" "true" "-b" "2" "-t" "false"'
        pargs='"pilot3/pilot.py" "-h" "%s" "-s" "%s" "-F" "Nordugrid-ATLAS" "-d" "{HOME}" "-j" "false" "-f" "false" "-z" "true" "-b" "2" "-t" "false"' % (self.sitename,self.sitename)
        self.xrsl['arguments']  = '(arguments = "'+self.artes+'" "' + self.pandajob  + '" '+pargs+ ')'

        if self.truepilot:
            pargs='"pilot3/pilot.py" "-h" "%s" "-s" "%s" "-f" "false" "-p" "25443" "-w" "https://pandaserver.cern.ch"' % (self.sitename,self.sitename)
            self.xrsl['arguments']  = '(arguments = "'+self.artes+'" "' + self.pandajob  + '" '+pargs+ ')'


    def setInputs(self):

        x = ""        
        if self.truepilot:
            x += '(ARCpilot-test "http://voatlas404.cern.ch;cache=check/data/data/ARCpilot-true")'
        else:
            x += '(ARCpilot-test "http://voatlas404.cern.ch;cache=check/data/data/ARCpilot-test")'
        if self.sitename.find("LRZ-LMU_MUC_MCORE") != -1:
            x += '(pilotcode.tar.gz "http://pandaserver.cern.ch:25080;cache=check/cache/pilot/pilotcode-rc.tar.gz")'
            #x += '(pilotcode.tar.gz "http://project-atlas-gmsb.web.cern.ch;cache=check/project-atlas-gmsb/pilotcode-dev.tar.gz")'
        elif self.jobdesc['prodSourceLabel'][0] == 'rc_test':
            x += '(pilotcode.tar.gz "http://pandaserver.cern.ch:25080;cache=check/cache/pilot/pilotcode-rc.tar.gz")'
        else:
            #x += '(pilotcode.tar.gz "http://project-atlas-gmsb.web.cern.ch;cache=check/project-atlas-gmsb/pilotcode-dev.tar.gz")'
            x += '(pilotcode.tar.gz "http://pandaserver.cern.ch:25080;cache=check/cache/pilot/pilotcode.tar.gz")'
        #x += '(pilotcode.tar.gz "http://www-f9.ijs.si;cache=check/grid/pilotcode-58fp1.tar.gz")'
        #x += '(pilotcode.tar.gz "http://www-f9.ijs.si;cache=check/grid/pilotcode-58j1.tar.gz")'
        if self.sitename.find("ARC-TEST") != -1:
            x += '(ARCpilot-test.tar.gz "http://voatlas404.cern.ch;cache=check/data/data/ARCpilot-test.tar.gz")'
        else:
            x += '(ARCpilot-test.tar.gz "http://voatlas404.cern.ch;cache=check/data/data/ARCpilot-test.tar.gz")'
        if not self.truepilot:
            x += '(queuedata.pilot.json "http://pandaserver.cern.ch:25085;cache=check/cache/schedconfig/%s.all.json")' % self.schedconfig

        if(self.jobdesc.has_key('inFiles') and not self.truepilot):
            inf={}
            if self.catalog.find('lfc://') == 0:
                for f,g in zip (self.jobdesc['inFiles'][0].split(","),self.jobdesc['GUID'][0].split(",")):
                    lfn="lfc://prod-lfc-atlas.cern.ch/:guid="+g
                    inf[f]=lfn
            elif self.catalog.find('rucio://') == 0:
                for f,s in zip (self.jobdesc['inFiles'][0].split(","),self.jobdesc['scopeIn'][0].split(",")):
                    # Hard-coded pilot rucio account - should change based on proxy
                    # Rucio does not expose mtime, set cache=invariant so not to download too much
                    lfn='/'.join(["rucio://voatlasrucio-server-prod.cern.ch;rucioaccount=pilot;transferprotocol=gsiftp,https;cache=invariant/replicas", s, f])
                    inf[f]=lfn
            else:
                raise Exception("Unknown catalog implementation: " + self.catalog)
            # some files are double:
            for k,v in inf.items():
                x += "(" + k + " " + '"' + v + '"' + ")"

        self.xrsl['inputfiles'] = "(inputfiles =  %s )" % x


    def setLog(self):

        if (self.jobdesc.has_key('logFile')):
            logfile = self.jobdesc['logFile'][0]
        else:
            logfile = "LOGFILE"

        self.xrsl['log'] = '(stdout = "' + logfile.replace('.tgz','') + '")(join = yes)'


    def setGMLog(self):

        self.xrsl['gmlog'] = '("gmlog" = "gmlog")'
        self.xrsl['rerun'] = '("rerun" = "2")'

    def setOutputs(self):

        # dynamic outputs

        x  = '("jobSmallFiles.tgz" "")'
        x += '("@output.list" "")'
        # tmp hack until arc 4.1 is everywhere, to avoid "Error reading user
        # generated output file list"
        x += '("output.list" "")' 
        self.xrsl['outputs'] = "(outputfiles = %s )" % x

        if self.truepilot:
            self.xrsl['outputs'] = ""


    def setPriority(self):
        
        if self.jobdesc.has_key('currentPriority'):
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
                    prio = 90 + ( prio - 1000 ) / 900.
                    prio = int(prio)
                if prio > 10000:
                    prio = 100
            except:
                pass
            #self.xrsl['priority'] = '("priority" = 60 )'
            self.xrsl['priority'] = '("priority" = %d )' % prio
            pass
            

    def parse(self):
        self.setTime()
        self.setJobname()
        #self.setDisk()
        self.setMemory()
        self.setRTE()
        self.setExecutable()
        self.setArguments()
        self.setInputs()
        self.setLog()
        self.setGMLog()
        self.setOutputs()
        self.setPriority()

    def getXrsl(self):
        x="&"
        for k in self.xrsl.keys():
            x+=self.xrsl[k]+"\n"
        return x


if __name__ == '__main__':


    pandajob='logGUID=d786ee44-a065-4566-b0ad-4361dab1b06f&cmtConfig=i686-slc5-gcc43-opt&dispatchDBlockTokenForOut=NULL%2CNULL%2CNULL%2CNULL&destinationDBlockToken=ATLASDATADISK%2CATLASDATADISK%2CATLASDATADISK%2CATLASDATADISK&destinationSE=CERN-PROD&realDatasets=valid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.recon.ESD.e1900_s1638_r4899_tid01341574_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.recon.AOD.e1900_s1638_r4899_tid01341574_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.recon.TAG.e1900_s1638_r4899_tid01341574_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.recon.log.e1900_s1638_r4899_tid01341574_00&prodUserID=jose.enrique.garcia%40cern.ch&GUID=72CD95A0-93E6-1840-B89C-0E00240E27FD%2C35369775-2BA2-E145-A142-BAA78880C6E1%2CEE1CC970-A1D1-3E4D-9A03-998467FB843E%2C02A0460A-5F63-8C46-AEAC-CA4EE5FB3086%2C49BBFB21-9F2A-1D48-90D9-0C82C0A71D06%2C5D152B2E-6C0D-0643-978F-314F449CF2D2%2C29BED061-C906-4D47-B24D-9BEADF04289C%2C76009FCC-3B48-BE4C-ADA4-28A154EBF572%2C04AD849D-F5AD-3D4A-BA1B-26850BFDB2A4%2C4569FF77-3842-2149-8BF8-82B799096CCD%2C2F96B80E-C313-2541-BE94-385F2D5D23E7%2CD24C033E-2E72-1643-B621-0DE4079D93D3%2CF54DAC3C-6C4D-DA4B-A0DD-96A5F12B49B9%2C7CA3CC33-CC49-EE44-8719-A8B5C7E64192%2CEF32DBED-7CB4-3A42-8887-B0379399F8C9%2C719b77d7-54c7-4112-95f5-8dec51cfcd4e%2CCF5D74CA-9EE0-C048-BFF8-F93894383B65%2C768FD917-6B5D-E348-B450-203723C0FA2D%2C340A28C3-8C42-0644-B30A-B0C8FB42C26A%2C8DC99D5B-B595-CB49-A09D-D388CFAB3B74%2CD48B5105-D0F8-E54B-BF23-E934264F549F%2C7A59B8AC-C76F-4645-97D2-001885A664E5%2CF3F7C224-580E-B147-9FD4-B52E2C2BE012%2CE896C839-5CB1-D148-9B0F-44530046E432%2C17FD899C-3585-3041-8D72-DD0F82DA9A06%2CE90E87DC-2B16-CD4B-90A9-ABC5979BB1DC%2CC52DDC02-14E1-444E-BC12-95448DF43B47%2CABF60825-4ED9-5343-BBB9-B940CA92B22D%2C55D5E1D2-BA76-1D46-B03E-4ED329396FD6%2C2EDE36F0-D774-0C4F-AC34-D1006FA95BE7%2CAC5716C4-A1A3-CF47-90E2-0734D157F4F8&realDatasetsIn=valid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cddo.000001.Atlas.Ideal.DBRelease.v230101%2Cmc12_14TeV.119995.Pythia8_A2MSTW2008LO_minbias_inelastic_low.merge.HITS.e1133_s1638_s1659%2F%2Cmc12_14TeV.119996.Pythia8_A2MSTW2008LO_minbias_inelastic_high.merge.HITS.e1133_s1638_s1659_tid01222104_00%2Cmc12_14TeV.119996.Pythia8_A2MSTW2008LO_minbias_inelastic_high.merge.HITS.e1133_s1638_s1659_tid01222104_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.simul.HITS.e1900_s1638_tid01246207_00%2Cmc12_14TeV.119995.Pythia8_A2MSTW2008LO_minbias_inelastic_low.merge.HITS.e1133_s1638_s1659%2F%2Cmc12_14TeV.119995.Pythia8_A2MSTW2008LO_minbias_inelastic_low.merge.HITS.e1133_s1638_s1659%2F%2Cmc12_14TeV.119995.Pythia8_A2MSTW2008LO_minbias_inelastic_low.merge.HITS.e1133_s1638_s1659%2F%2Cmc12_14TeV.119995.Pythia8_A2MSTW2008LO_minbias_inelastic_low.merge.HITS.e1133_s1638_s1659%2F%2Cmc12_14TeV.119996.Pythia8_A2MSTW2008LO_minbias_inelastic_high.merge.HITS.e1133_s1638_s1659_tid01222104_00%2Cmc12_14TeV.119996.Pythia8_A2MSTW2008LO_minbias_inelastic_high.merge.HITS.e1133_s1638_s1659_tid01222104_00%2Cmc12_14TeV.119996.Pythia8_A2MSTW2008LO_minbias_inelastic_high.merge.HITS.e1133_s1638_s1659_tid01222104_00&nSent=4&cloud=CERN&StatusCode=0&homepackage=IBLProd%2F17.3.10.1.9&inFiles=HITS.01246207._000291.pool.root.1%2CHITS.01246207._000307.pool.root.1%2CHITS.01246207._000308.pool.root.1%2CHITS.01246207._000310.pool.root.1%2CHITS.01246207._000311.pool.root.1%2CHITS.01246207._000318.pool.root.1%2CHITS.01246207._000320.pool.root.1%2CHITS.01246207._000323.pool.root.1%2CHITS.01246207._000324.pool.root.1%2CHITS.01246207._000325.pool.root.1%2CHITS.01246207._000327.pool.root.1%2CHITS.01246207._000317.pool.root.1%2CHITS.01246207._000330.pool.root.1%2CHITS.01246207._000455.pool.root.1%2CHITS.01246207._000449.pool.root.1%2CDBRelease-23.1.1.tar.gz%2CHITS.01222100._000106.pool.root.1%2CHITS.01222104._000109.pool.root.1%2CHITS.01222104._000110.pool.root.1%2CHITS.01246207._000014.pool.root.1%2CHITS.01246207._000024.pool.root.1%2CHITS.01246207._000030.pool.root.1%2CHITS.01246207._000075.pool.root.1%2CHITS.01246207._000222.pool.root.1%2CHITS.01222100._000107.pool.root.1%2CHITS.01222100._000108.pool.root.1%2CHITS.01222100._000109.pool.root.1%2CHITS.01222100._000110.pool.root.1%2CHITS.01222104._000106.pool.root.1%2CHITS.01222104._000107.pool.root.1%2CHITS.01222104._000108.pool.root.1&processingType=validation&fsize=151174736%2C147668340%2C142550293%2C154935036%2C152996472%2C162045722%2C147247345%2C151492131%2C150528904%2C157304985%2C142994620%2C156109886%2C148761687%2C136075690%2C141567353%2C713566706%2C908043152%2C299988220%2C304423706%2C162260812%2C158996143%2C155373606%2C145577873%2C157495287%2C908094177%2C901244403%2C905077013%2C921653187%2C298933095%2C294433725%2C297102623&fileDestinationSE=CERN-PROD%2CCERN-PROD%2CCERN-PROD%2CCERN-PROD&scopeOut=valid1%2Cvalid1%2Cvalid1&minRamCount=3800&jobDefinitionID=359119736&scopeLog=valid1&transformation=DigiMReco_trf.py&maxDiskCount=8000&prodDBlockToken=NULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL&transferType=NULL&destinationDblock=valid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.recon.ESD.e1900_s1638_r4899_tid01341574_00_sub0130314221%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.recon.AOD.e1900_s1638_r4899_tid01341574_00_sub0130314225%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.recon.TAG.e1900_s1638_r4899_tid01341574_00_sub0130314230%2Cvalid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.recon.log.e1900_s1638_r4899_tid01341574_00_sub0130314234&dispatchDBlockToken=NULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL%2CNULL&jobPars=inputHitsFile%3DHITS.01246207._%5B000014%2C000024%2C000030%2C000075%2C000222%2C000291%2C000307%2C000308%2C000310%2C000311%2C000317%2C000318%2C000320%2C000323%2C000324%2C000325%2C000327%2C000330%2C000449%2C000455%5D.pool.root+outputESDFile%3DESD.01341574._000022.pool.root.1+outputAODFile%3DAOD.01341574._000022.pool.root.1+outputTAGFile%3DTAG.01341574._000022.pool.root.1+maxEvents%3D2000+triggerConfigByRun%3D%22%7B195847%3A%5C%22MCRECO%3ADB%3ATRIGGERDBMC%3A337%2C146%2C276%5C%22%7D%22+preInclude_h2r%3DDigitization%2FForceUseOfPileUpTools.py%2CSimulationJobOptions%2FpreInclude.PileUpBunchTrains2011Config8_DigitConfig.py%2CRunDependentSimData%2FconfigLumi_mc12_v1.py+postInclude_h2r%3DInDetIBL_Example%2FpostOptions.IBL_Digitization.py+preExec_h2r%3D%22from+Digitization.DigitizationFlags+import+digitizationFlags%3B+digitizationFlags.overrideMetadata%2B%3D%5B%5C%22SimLayout%5C%22%2C%5C%22PhysicsList%5C%22%5D%22+postExec_h2r%3D%22job.PileUpToolsAlg.PileUpTools%5B%5C%22MergeMcEventCollTool%5C%22%5D.OnlySaveSignalTruth%3DTrue%22+preInclude_r2e%3DInDetIBL_Example%2FpreInclude.IBL_Rec.py%2CInDetIBL_Example%2FNewLayerMaterial_03.py+postInclude_r2e%3DInDetIBL_Example%2FpostInclude.IBL_Rec.py+preExec_r2e%3D%22rec.Commissioning.set_Value_and_Lock%28True%29%3B+from+CaloRec.CaloCellFlags+import+jobproperties%3B+jobproperties.CaloCellFlags.doLArCellEmMisCalib%3DFalse%3Bfrom+InDetRecExample.InDetJobProperties+import+InDetFlags%3BInDetFlags.doBackTracking.set_Value_and_Lock%28True%29%3BInDetFlags.doTRTStandalone.set_Value_and_Lock%28True%29%3BInDetFlags.doPixelClusterSplitting.set_Value_and_Lock%28False%29%3BInDetFlags.writeRDOs.set_Value_and_Lock%28True%29%3BTriggerFlags.doLVL2.set_Value_and_Lock%28True%29%3BTriggerFlags.doEF.set_Value_and_Lock%28True%29%22+postInclude_e2a%3DInDetIBL_Example%2FpostInclude.IBL_NoAlign.py+preExec_e2a%3D%22InDetFlags.doPixelClusterSplitting.set_Value_and_Lock%28False%29%3BTriggerFlags.AODEDMSet%3D%5C%22AODSLIM%5C%22%3Brec.Commissioning.set_Value_and_Lock%28True%29%22+autoConfiguration%3Deverything+DBRelease%3DDBRelease-23.1.1.tar.gz+conditionsTag%3DOFLCOND-MC12-IBL-20-30-50+geometryVersion%3DATLAS-IBL-03-00-00+--athenaopts%3D--keep-configuration+numberOfLowPtMinBias%3D39.8608+numberOfHighPtMinBias%3D0.138128+LowPtMinbiasHitsFile%3DHITS.01222100._%5B000106%2C000107%2C000108%2C000109%2C000110%5D.pool.root+DataRunNumber%3D-1+jobNumber%3D22+digiSeedOffset1%3D22+digiSeedOffset2%3D22+HighPtMinbiasHitsFile%3DHITS.01222104._%5B000106%2C000107%2C000108%2C000109%2C000110%5D.pool.root+preExec%3D%22from+LArROD.LArRODFlags+import+larRODFlags%3B+larRODFlags.doOFCPileupOptimization.set_Value_and_Lock%28True%29%3B+larRODFlags.NumberOfCollisions.set_Value_and_Lock%2820%29%3B+larRODFlags.UseDelta.set_Value_and_Lock%283%29%3B+jobproperties.Beam.energy.set_Value_and_Lock%287000%2AUnits.GeV%29%3B+jobproperties.Beam.numberOfCollisions.set_Value_and_Lock%2820.0%29%3B+jobproperties.Beam.bunchSpacing.set_Value_and_Lock%2850%29%22+postExec%3D%22CfgMgr.MessageSvc%28%29.setError%2B%3D%5B%5C%22HepMcParticleLink%5C%22%5D%22+&attemptNr=1&swRelease=Atlas-17.3.10&maxCpuCount=100000&outFiles=ESD.01341574._000022.pool.root.1%2CAOD.01341574._000022.pool.root.1%2CTAG.01341574._000022.pool.root.1%2Clog.01341574._000022.job.log.tgz.1&currentPriority=900&scopeIn=valid1%2Cvalid1%2Cvalid1%2Cvalid1%2Cvalid1%2Cvalid1%2Cvalid1%2Cvalid1%2Cvalid1%2Cvalid1%2Cvalid1%2Cvalid1%2Cvalid1%2Cvalid1%2Cvalid1%2Cddo%2Cmc12_14TeV%2Cmc12_14TeV%2Cmc12_14TeV%2Cvalid1%2Cvalid1%2Cvalid1%2Cvalid1%2Cvalid1%2Cmc12_14TeV%2Cmc12_14TeV%2Cmc12_14TeV%2Cmc12_14TeV%2Cmc12_14TeV%2Cmc12_14TeV%2Cmc12_14TeV&PandaID=1928208201&dispatchDblock=panda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.GEN.1cc2007f-458b-436f-9c40-e312a8926432_dis001928208162%2Cpanda.1341574.09.11.GEN.b292c3fb-7742-4ec4-8377-0e14b4db1e62_dis001928208162%2Cpanda.1341574.09.11.GEN.b292c3fb-7742-4ec4-8377-0e14b4db1e62_dis001928208162%2Cpanda.1341574.09.11.GEN.b292c3fb-7742-4ec4-8377-0e14b4db1e62_dis001928208162%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.HITS.7b58fd8b-b51f-44b6-a94d-ada128e77408_dis1928208201%2Cpanda.1341574.09.11.GEN.b292c3fb-7742-4ec4-8377-0e14b4db1e62_dis001928208162%2Cpanda.1341574.09.11.GEN.b292c3fb-7742-4ec4-8377-0e14b4db1e62_dis001928208162%2Cpanda.1341574.09.11.GEN.b292c3fb-7742-4ec4-8377-0e14b4db1e62_dis001928208162%2Cpanda.1341574.09.11.GEN.b292c3fb-7742-4ec4-8377-0e14b4db1e62_dis001928208162%2Cpanda.1341574.09.11.GEN.b292c3fb-7742-4ec4-8377-0e14b4db1e62_dis001928208162%2Cpanda.1341574.09.11.GEN.b292c3fb-7742-4ec4-8377-0e14b4db1e62_dis001928208162%2Cpanda.1341574.09.11.GEN.b292c3fb-7742-4ec4-8377-0e14b4db1e62_dis001928208162&prodSourceLabel=managed&checksum=ad%3A88716ba5%2Cad%3A3317b9cb%2Cad%3Acced6da5%2Cad%3Af85fd73f%2Cad%3A37c7f13f%2Cad%3Ab52565aa%2Cad%3Ac3b5df33%2Cad%3A31137849%2Cad%3Aa1b30416%2Cad%3Ab12b9841%2Cad%3Af23cac2e%2Cad%3A3ee3cef7%2Cad%3Aa35f7501%2Cad%3Ab48bcbfc%2Cad%3A1cb4abf6%2Cad%3A9f56b207%2Cad%3A603209bd%2Cad%3A0a551f20%2Cad%3Ace924d00%2Cad%3A6e020562%2Cad%3A0553226f%2Cad%3Aaf342214%2Cad%3Aa098c5ce%2Cad%3A138854f7%2Cad%3A95ead297%2Cad%3A47158820%2Cad%3Aa43f1cab%2Cad%3A18627a6d%2Cad%3Ad530f334%2Cad%3A4af0fe91%2Cad%3A7b2ca679&jobName=valid1.105200.McAtNloJimmy_CT10_ttbar_LeptonFilter.recon.e1900_s1638_r4899_tid01341574._000022.job&taskID=1341574&logFile=log.01341574._000022.job.log.tgz.1'


    pandajob="logGUID=26e620d0-e252-41ce-94ea-fa2471fca4a7&cmtConfig=x86_64-slc5-gcc43-opt&dispatchDBlockTokenForOut=NULL%2CNULL&destinationDBlockToken=ATLASDATADISK%2CATLASDATADISK&destinationSE=INFN-T1&realDatasets=mc11_7TeV.126130.J3_pythia_jetjet_Perugia2011.simul.HITS.e877_s1349_tid01361280_00%2Cmc11_7TeV.126130.J3_pythia_jetjet_Perugia2011.simul.log.e877_s1349_tid01361280_00&prodUserID=jose.enrique.garcia%40cern.ch&GUID=D6211C66-6E37-E311-83E9-0025901AA3BE%2C1b2daa89-e8ab-47ca-a5af-173c4b1d0725&realDatasetsIn=mc11_7TeV.126130.J3_pythia_jetjet_Perugia2011.evgen.EVNT.e877_tid01358974_00%2Cddo.000001.Atlas.Ideal.DBRelease.v160801&nSent=100&cloud=IT&StatusCode=0&homepackage=AtlasProduction%2F16.6.7.14&inFiles=EVNT.01358974._001138.pool.root.1%2CDBRelease-16.8.1.tar.gz&processingType=simul&fsize=71384635%2C534823901&fileDestinationSE=INFN-T1%2CINFN-T1&scopeOut=mc11_7TeV&minRamCount=2000&jobDefinitionID=373528873&scopeLog=mc11_7TeV&transformation=AtlasG4_trf.py&maxDiskCount=8000&prodDBlockToken=NULL%2CNULL&transferType=NULL&destinationDblock=mc11_7TeV.126130.J3_pythia_jetjet_Perugia2011.simul.HITS.e877_s1349_tid01361280_00_sub0138498014%2Cmc11_7TeV.126130.J3_pythia_jetjet_Perugia2011.simul.log.e877_s1349_tid01361280_00_sub0138498018&dispatchDBlockToken=NULL%2CNULL&jobPars=inputEvgenFile%3DEVNT.01358974._001138.pool.root.1+outputHitsFile%3DHITS.01361280._085006.pool.root.1+maxEvents%3D1+skipEvents%3D250+DBRelease%3DDBRelease-16.8.1.tar.gz+preInclude%3DSimulationJobOptions%2FpreInclude.CalHits.py%2CSimulationJobOptions%2FpreInclude.BeamPipeKill.py%2CSimulationJobOptions%2FpreInclude.ParticleID.py+geometryVersion%3DATLAS-GEO-18-01-03_VALIDATION+conditionsTag%3DOFLCOND-SDR-BS7T-05-12+AMITag%3Ds1349+randomSeed%3D85006+physicsList%3DQGSP_BERT+firstEvent%3D251+RunNumber%3D126130+&attemptNr=1&swRelease=Atlas-16.6.7&maxCpuCount=100000&outFiles=TEST-HITS.01361280._085006.pool.root.1%2Clog.01361280._085006.job.log.tgz.1&currentPriority=110&scopeIn=mc11_7TeV%2Cddo&PandaID=1977745545&dispatchDblock=panda.1361280.11.07.GEN.ad783244-f71e-4e13-944f-59c164ff5cf5_dis001977745458%2Cpanda.1361280.11.07.GEN.6b2356f6-d865-4a90-b7c3-7d4a0ab5cdee_dis001977745458&prodSourceLabel=managed&checksum=ad%3Ac9e2ff0a%2Cad%3Ab6ac2f66&jobName=TEST-mc11_7TeV.126130.J3_pythia_jetjet_Perugia2011.simul.e877_s1349_tid01361280._085006.job&taskID=1361280&logFile=TEST-log.01361280._085006.job.log.tgz.1"

    pandajob="ogGUID=df19539e-d442-4432-9a42-1aa8d0736a09&cmtConfig=i686-slc5-gcc43-opt&dispatchDBlockTokenForOut=NULL%2CNULL&destinationDBlockToken=NULL%2CNULL&destinationSE=LRZ-LMU_C2PAP&realDatasets=hc_test.gangarbt.hc20028229.LRZ-LMU_C2PAP.473%2Chc_test.gangarbt.hc20028229.LRZ-LMU_C2PAP.473&prodUserID=%2FDC%3Dch%2FDC%3Dcern%2FOU%3DOrganic+Units%2FOU%3DUsers%2FCN%3Dgangarbt%2FCN%3D703216%2FCN%3DRobot%3A+Ganga+Robot%2FCN%3Dproxy&GUID=e188c93b-d07c-41fc-841f-e9945cd85d15%2C46349036-6095-E011-952E-002655213860&realDatasetsIn=ddo.000001.Atlas.Ideal.DBRelease.v240601%2Cmc11_7TeV.105000.pythia_minbias_inelastic.evgen.EVNT.e816_tid420738_00&nSent=0&cloud=DE&StatusCode=0&homepackage=AtlasProduction%2F16.6.7.34&inFiles=DBRelease-24.6.1.tar.gz%2CEVNT.420738._000001.pool.root.1&processingType=gangarobot-pft&fsize=833392469%2C23892103&fileDestinationSE=LRZ-LMU_C2PAP%2CLRZ-LMU_C2PAP&scopeOut=hc_test&minRamCount=0&jobDefinitionID=52&scopeLog=hc_test&transformation=AtlasG4_trf.py&maxDiskCount=0&prodDBlockToken=NULL%2CNULL&transferType=NULL&destinationDblock=hc_test.gangarbt.hc20028229.LRZ-LMU_C2PAP.473_sub0139394088%2Chc_test.gangarbt.hc20028229.LRZ-LMU_C2PAP.473_sub0139394088&dispatchDBlockToken=NULL%2CNULL&jobPars=maxEvents%3D2+skipEvents%3D0+randomSeed%3D568+geometryVersion%3DATLAS-GEO-11-00-00+conditionsTag%3DOFLCOND-SDR-BS7T-02+--ignoreerrors%3DTrue+DBRelease%3DDBRelease-24.6.1.tar.gz+RunNumber%3D105000+outputHitsFile%3DHits.hc_20028229.HITS.pool.root.LRZ-LMU_C2PAP.1384367986.820f+inputEvgenFile%3DEVNT.420738._000001.pool.root.1+--overwriteQueuedata%3D%7Ballowfax%3DFalse%7D+&attemptNr=0&swRelease=Atlas-16.6.7&maxCpuCount=0&outFiles=Hits.hc_20028229.HITS.pool.root.LRZ-LMU_C2PAP.1384367986.820f%2Ce8976462-758a-46c8-82bb-522a68cfc69b.job.log.tgz&currentPriority=10000&scopeIn=ddo%2Cmc11_7TeV&PandaID=1986227157&dispatchDblock=NULL%2CNULL&prodSourceLabel=prod_test&checksum=ad%3A46c6683e%2Cad%3A2da86ea1&jobName=e8976462-758a-46c8-82bb-522a68cfc69b&taskID=NULL&logFile=e8976462-758a-46c8-82bb-522a68cfc69b.job.log.tgz"

    a=aCTPanda2Xrsl(pandajob,'LRZ-LMU_C2PAP')
    a.parse()
    print a.getXrsl()
