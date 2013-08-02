import os
import time
from aCTDBArc import aCTDBArc
import arc
import cgi
import lfcthr as lfc
import LFCTools
import aCTConfig
import aCTLogger
import aCTSignal
import aCTUtils
from threading import Thread

class SubmitThr(Thread):
    def __init__ (self,func,pandaid,jobdescs,uc,logger):
        Thread.__init__(self)
        self.func=func
        self.pandaid=pandaid
        self.jobdescs = jobdescs
        self.uc = uc
        self.log=logger
        self.job = None
    def run(self):
        self.job=self.func(self.jobdescs,self.uc,self.log)


def Submit(jobdescs,uc,log):

    global queuelist

    if len(queuelist) == 0  :
        log.error("no cluster free for submission")
        return None
    
    # Do brokering among the available queues
    jobdesc = jobdescs[0]
    broker = arc.Broker(uc, jobdesc, "Random")
    targetsorter = arc.ExecutionTargetSorter(broker)
    for target in queuelist:
        log.debug("considering target %s:%s" % (target.ComputingService.Name, target.ComputingShare.Name))

        # Adding an entity performs matchmaking and brokering
        targetsorter.addEntity(target)
    
    if len(targetsorter.getMatchingTargets()) == 0:
        log.error("no clusters satisfied job description requirements")
        return None
        
    targetsorter.reset() # required to reset iterator, otherwise we get a seg fault
    selectedtarget = targetsorter.getCurrentTarget()
    # Job object will contain the submitted job
    job = arc.Job()
    submitter = arc.Submitter(uc)
    if submitter.Submit(selectedtarget, jobdesc, job) != arc.SubmissionStatus.NONE:
        log.error("Submission failed")
        return None

    return job


## To move to Panda code ##    
def GetSRMurlOld (dataset,lfn,token=None,attempt=100):
  # to clean up!!!!!!
  # mapping of output dataset/lfn to  srm url
  # TODO overwrite should be off by default -> multiple jobs!!!!
  if attempt > 1 :
    overwrite = ';overwrite=yes'
  else:
    overwrite = ''

  srmdiskpath = 'srm://srm.ndgf.org' + overwrite + '/atlas/disk/atlasmcdisk'
  srmtokenpath = 'srm://srm.ndgf.org;spacetoken=ATLASMCDISK' + overwrite + '/atlas/disk/atlasmcdisk'
  #
  # This should really be rewritten to extract the surl fragments from TiersOfAtlas instead of setting them
  # by hand!
  #
  if token == 'ATLASMCDISK':
    srmdiskpath = 'srm://srm.ndgf.org' + overwrite + '/atlas/disk/atlasmcdisk'
    srmtokenpath = 'srm://srm.ndgf.org;spacetoken=ATLASMCDISK' + overwrite + '/atlas/disk/atlasmcdisk'
  elif token == 'ATLASMCTAPE':
    srmdiskpath = 'srm://srm.ndgf.org' + overwrite + '/atlas/tape/atlasmctape'
    srmtokenpath = 'srm://srm.ndgf.org;spacetoken=ATLASMCDISK' + overwrite + '/atlas/tape/atlasmctape'
  elif token == 'ATLASDATADISK':
    srmdiskpath = 'srm://srm.ndgf.org' + overwrite + '/atlas/disk/atlasdatadisk'
    srmtokenpath = 'srm://srm.ndgf.org;spacetoken=ATLASMCDISK' + overwrite + '/atlas/disk/atlasdatadisk'
  elif token == 'ATLASDATATAPE':
    srmdiskpath = 'srm://srm.ndgf.org' + overwrite + '/atlas/tape/atlasdatatape'
    srmtokenpath = 'srm://srm.ndgf.org;spacetoken=ATLASMCDISK' + overwrite + '/atlas/tape/atlasdatatape'
  else:
    print 'WARNING, token is ',token,', using default srmpaths'

  if token == None:
    surlprefix = srmdiskpath
  else:
    surlprefix = srmtokenpath

  surl = surlprefix + '/' + LFCTools.to_native_lfn(dataset,lfn,'').replace(LFCTools.LFC_HOME,'')
  #print 'making surl',surl
  return surl




class aCTSubmitter:

    class BrokerQueued:
        """
        Simple broker to order by number of queued jobs on a cluster/queue
        """
        def __init__(self):
            pass
        def __call__(self,targets):
            tl=list(targets)
            tl.sort(self.__cmp)
            return tl
        def __cmp(self,target1,target2):
            a = target1.grid_queued
            b = target2.grid_queued
            return cmp(a,b)


    def __init__(self):
        self.logger=aCTLogger.aCTLogger("submitter")
        self.log=self.logger()

        self.conf=aCTConfig.aCTConfig()
        self.db=aCTDBArc(self.log,self.conf.get(["db","file"]))

        # ARC Configuration
        self.uc = arc.UserConfig()
        self.uc.ProxyPath("/tmp/x509up_u%s" % os.getuid())
        self.uc.CACertificatesDirectory("/etc/grid-security/certificates")
        timeout=int(self.conf.get(['atlasgiis','timeout']))
        self.uc.Timeout(timeout)
        # Set random broker (it is the default but set here anyway)
        self.uc.Broker("Random")

        self.log.info("Started")

        # start time for periodic restart
        self.starttime=time.time()


    def RunThreadsSplit(self,plist,nthreads=1):
        it=0
        while it < len(plist):
            tl=[]
            for i in range(0,nthreads):
                try:
                    t=plist[it]
                    tl.append(t)
                    t.start()
                except:
                    pass
                it+=1
            errfl=False
            for t in tl:
                t.join(60.0)
                if t.isAlive() :
                    # abort due to timeout and try again
                    self.log.error("submission timeout: exit and try again")
                    errfl=True
                # updatedb
                if t.job is None:
                    #self.log.error("no jobname")
                    self.log.error("no job defined for %d" % t.pandaid)
                    errfl=True
                    continue
                jd={}
                jd['arcstate']='submitted'
                # initial offset to 1 minute to force first status check
                jd['tarcstate']=time.time()-int(self.conf.get(['jobs','checkinterval']))+300
                self.db.updateArcJob(t.pandaid,jd,t.job)
            if errfl:
                exit(1)


    ## To move to Panda code ##
    def GetLFN(self,guid,pandaid):
        """
        retrieve lfn from the database
        """
        j=self.db.getJob(pandaid)
        l={}
        for f in j['lfns'].split(","):
            (k,v)=f.split("=",1)
            l[k]=v
        return l[guid]


    ## To move to Panda code ##
    def GetLFNs(self,guids):
        """
        Lookup for LFNs in LFC by guid. Bulk method
        """
        host=str(self.conf.get(['lfc','host'])).split("/")[2]
        os.environ['LFC_HOST'] = host
        lfcurl=self.conf.get(['lfc','host'])
        if lfc.lfc_startsess(host, '') != 0:
            return guids
        for g in guids.keys():
            try:
                links = lfc.lfc_getlinks('', str(g))
            except Exception,x:
                self.log.error(x) 
                lfc.lfc_endsess()
                return guids
            if not links or not links[1] or not links[1][0]:
                guids[g]=None
            else:
                guids[g]=lfcurl+links[1][0].path            
        lfc.lfc_endsess()
        return guids


    ## To move to Panda code ##
    def GetSRMurl (self,dataset,lfn,token=None,attempt=0):
        # mapping of output dataset/lfn to  srm url
        if attempt > 1 :
          overwrite = ';overwrite=yes'
        else:
          overwrite = ''

        spacetoken=";spacetoken="+token
        tokens={}
        for (i,j) in zip(self.conf.getList(['srm','token','name']),self.conf.getList(['srm','token','dir'])):
            tokens[i]=j
        srmtokenpath = self.conf.get(['srm','prefix']) + spacetoken + overwrite + tokens[token]
        surl = srmtokenpath + '/' + LFCTools.to_native_lfn(dataset,lfn,'').replace(LFCTools.LFC_HOME,'')
        #print 'making surl',surl
        return surl



    ## To move to Panda code ##
    def panda2xrsl(self,pjobdef,pandaid):
        """
        Translate panda job description to xrsl format
        """
        logdir = 'gmlog'
        jobdesc = cgi.parse_qs(pjobdef)
        #print jobdesc
        try:
              logfile = jobdesc['logFile'][0]
        except:
          return None
        #print "______________________________"

        xrsl = "&"

        jobname = jobdesc['jobName'][0]
        xrsl += "(jobname = " + jobname + ")"
        #
        #......ramcount, diskcount, cpucount
        #
        # DO IT PROPERLY
        if jobdesc.has_key('maxDiskCount'):
            diskcount = jobdesc['maxDiskCount'][0]
            diskcount = '500' # MB
        else:
            diskcount = '500' # MB

        if jobdesc.has_key('minRamCount'):
            ramc = int(jobdesc['minRamCount'][0])
        if ramc == 0:
            ramc = 2000
        if ramc == 20000:
            ramc = 4000
        else:
            ramc = 2000
        if jobname.find("recon") != -1:
            ramc = max(ramc,2000)
        elif jobname.find("pile") != -1:
            ramc = max(ramc,3000)
        elif jobname.find("digit") != -1:
            ramc = max(ramc,1000)
        elif jobname.find("merge") != -1:
            ramc = max(ramc,1800)
        elif jobname.find("evgen") != -1:
            ramc = max(ramc,700)
    
        # testing
            #if jobname.find("simul") != -1:
            #    ramc = 1600
        
        #if ramc > 10000:
        #    ramc = 2000
         
        if jobdesc.has_key('maxCpuCount'):
            cpucount = int(jobdesc['maxCpuCount'][0])
        if cpucount == 0:
          cpucount = 200000
        else:
            cpucount = 200000
        if cpucount < 80000:
            cpucount = 80000
            # pilot loop handling?
        # TEMP FIX !!!!!!!
        fac=1.
        if jobname.find('tid168025') != -1:
           fac=2
        if jobname.find('tid168019') != -1:
           fac=2
        if jobname.find('tid044849') != -1:
           fac=1.5
        if jobname.find('tid044848') != -1:
           fac=1.5
        if jobname.find('tid070377') != -1:
           fac=5
        if jobname.find('tid071384') != -1:
           fac=20
        if jobname.find('tid071789') != -1:
           fac=20
        if jobname.find('tid071924') != -1:
           fac=4
        if jobname.find('tid073017') != -1:
           fac=3
        if jobname.find('tid075769') != -1:
           fac=3
        if jobname.find('tid078798') != -1:
           fac=5
        if jobname.find('tid081528') != -1:
           fac=3
        if jobname.find('tid092051') != -1:
           fac=3
        if jobname.find('tid092491') != -1:
           fac=3
        if jobname.find('tid094611') != -1:
           fac=3
        if jobname.find('tid095297') != -1:
           fac=3
        if jobname.find('tid095303') != -1:
           fac=3
        if jobname.find('tid095307') != -1:
           fac=3
        if jobname.find('tid095313') != -1:
           fac=3
        if jobname.find('tid095175') != -1:
           fac=2
        if jobname.find('_s65') != -1:
           fac=1.0
        if jobname.find('tid118654') != -1:
           fac=4
        if jobname.find('data10') != -1:
           fac=2
        #if jobname.find('evgen') != -1:
        #   fac=0.3
        if jobname.find('tid153663') != -1:
           fac=3
        if jobname.find('tid153662') != -1:
           fac=3
        if jobname.find('tid212439') != -1:
           fac=3
        if jobname.find('tid278617') != -1:
           fac=3
        if jobname.find('tid277089') != -1:
           fac=3
        if jobname.find('tid432462') != -1:
           fac=3
        if jobname.find('tid432319') != -1:
           fac=3
        if jobname.find('tid431656') != -1:
           fac=3
        if jobname.find('tid489614') != -1:
           fac=3
        if jobname.find('tid489613') != -1:
           fac=3
        if jobname.find('tid368866') != -1:
           ramc=4000
        if jobname.find('r3429') != -1:
           ramc=4000
        if jobname.find('tid523662') != -1:
           fac=3
        if jobname.find('tid523659') != -1:
           fac=3
        if jobname.find('tid514688') != -1:
           fac=3
        if jobname.find('tid534677') != -1:
           fac=3
        if jobname.find('tid534679') != -1:
           fac=3
        if jobname.find('tid544951') != -1:
           fac=3
        if jobname.find('tid525407') != -1:
           fac=3
        if jobname.find('tid621648') != -1:
           fac=4
        if jobname.find('tid670352') != -1:
           fac=4
        if jobname.find('tid573532') != -1:
           fac=4
        if jobname.find('tid625293') != -1:
           fac=4
        if jobname.find('tid654152') != -1:
           fac=4
        if jobname.find('r3182') != -1:
           fac=3
        if jobname.find('tid00976698') != -1:
           fac=3
        if jobname.find('tid00929583') != -1:
           fac=3
    
            #if ramc == 2200 : 
        #  ramc = 2000
            #if ramc > 2200 : 
        #  ramc = 2000
    
        if jobname.find('tid598001') != -1:
           ramc=2500
        if jobname.find('tid598297') != -1:
           ramc=3000
        if jobname.find('tid596556') != -1:
           ramc=2500
        if ( jobname.find('recon') != -1 ) and ( ramc == 2000  ) :
           ramc=2100
        #if ( jobname.find('recon') != -1 ) and ( jobname.find('_a1') != -1  ) :
        #   ramc=2500
        #if ( jobname.find('mc11_7TeV.105016.J7_pythia_jetjet.recon.e815_s1273_s1274') != -1 ) :
        #   ramc=2500
            #   fac=2       
        if jobname.find('tid579879') != -1:
           ramc=3000
        if ramc == 3800:
           ramc=3000
        if ramc == 3500:
           ramc=3000
        if ramc == 3250:
           ramc=2500
        if jobname.find('tid797949') != -1:
           ramc=2000
           cpucount=200000
        if jobname.find('evgen') != -1:
           ramc=2000
           #### AF cpucount=350000
        if jobname.find('tid00891808') != -1:
           fac = 3
        if jobname.find('tid00868047') != -1:
           fac = 3
        if jobname.find('tid00909110') != -1:
           fac = 3
        if jobname.find('tid00920051') != -1:
           fac = 3
        if jobname.find('tid00921299') != -1:
           fac = 3
        if jobname.find('tid00922435') != -1:
           fac = 3
        if jobname.find('tid00922464') != -1:
           fac = 3
        if jobname.find('tid00900239') != -1:
           fac=2
        if jobname.find('tid00920467') != -1:
           fac=2
        if jobname.find('tid00912224') != -1:
           fac=2
        if jobname.find('tid00923424') != -1:
           fac=3
        if jobname.find('tid0097434') != -1:
           fac=2
        if jobname.find('tid00937892') != -1:
           fac=2
        if jobname.find('tid00937890') != -1:
           fac=2
        if jobname.find('e1578_s1372_s1370_r3108') != -1:
           ramc=3000
        if jobname.find('tid00937944') != -1:
           ramc=3000
        if jobname.find('tid01110497') != -1:
           ramc=3800
        if jobname.find('tid01112336') != -1:
           ramc=3800
        if jobname.find('tid01112352') != -1:
           ramc=3800
        if jobname.find('tid01112546') != -1:
           ramc=3800
        if jobname.find('tid01112567') != -1:
           ramc=3800
        if jobname.find('tid01112555') != -1:
           ramc=3800
        if jobname.find('r4065') != -1:
           fac=1.5
        
        gridtime = str(int(fac * 1.5 * cpucount * 1000 / 28.5 / 2800))
    
        #if int(gridtime) > 5760 :
        #  gridtime="5760"

        xrsl += '(memory=' + str(ramc) + ')'
        #AF   xrsl += '(disk=' + diskcount + ')'
        #xrsl += '(gridtime=' + gridtime + ')'
        xrsl += '(walltime=' + gridtime + ')'
        xrsl += '(cputime=' + gridtime + ')'
        #AF xrsl += '(jobreport=https://grid.uio.no:8001/logger)'
        #
        #......The runtime environment
        #
        atlasrtes = []
        for (package,cache) in zip(jobdesc['swRelease'][0].split('\n'),jobdesc['homepackage'][0].split('\n')):
            if cache.find('Production') > 1:
                rte = package.split('-')[0].upper()  + '-' \
                      + cache.split('/')[1]
            else:
                rte=cache.replace('Atlas','Atlas-').replace('/','-').upper()
            rte=str(rte)
            rte=rte.replace('ATLAS-','')
        # rte += "-"+jobdesc['cmtConfig'][0].upper()
            if jobdesc['cmtConfig'][0] == "x86_64-slc5-gcc43-opt" :
                rte += "-"+jobdesc['cmtConfig'][0].upper()
            if jobdesc['cmtConfig'][0] == "x86_64-slc6-gcc46-opt" :
                rte += "-"+jobdesc['cmtConfig'][0].upper()
        #jobname = jobdesc['jobName'][0]
        #if jobname.find("tid276368") != -1:
        #  rte += "-"+"x86_64-slc5-gcc43-opt".upper()

            rte=rte.replace('PHYSICS-','ATLASPHYSICS-')
            rte=rte.replace('PROD2-','ATLASPROD2-')
            rte=rte.replace('PROD1-','ATLASPROD1-')
            atlasrtes.append(rte)
        #print "RTE ",rte+"-"+jobdesc['cmtConfig'][0].upper()
        # hack for client rte problem, keep only last !!!!!!
        for rte in atlasrtes[-1:]:
            xrsl += "(runtimeenvironment = APPS/HEP/ATLAS-" + rte + ")"
    
            #temp hack, take only the first rte
            ###arte=jobdesc['homepackage'][0].split("\n")[0].split("/")[1]
            arte=",".join(atlasrtes)
                
            #
            #......The grid job wrapper
            #
        wrapper="ptarurl"
        if jobdesc.has_key('prodSourceLabel'):
            if str(jobdesc['prodSourceLabel'][0]) == "rc_test":
                  wrapper="rcptarurl"
    
            if self.conf.get(['executable','wrapper']) == "NGExecWrapper":
                xrsl += '(executable = "NGExecWrapper")'
                #infiles
                nargs=""
                if jobdesc.has_key('inFiles'):
                    nargs = '"'+str(len(jobdesc['inFiles'][0].split(",")))+'"'
                    for f,g in zip (jobdesc['inFiles'][0].split(","),jobdesc['GUID'][0].split(",")):
                        nargs += ' "'+f+'" "' + g + '"'
                
                #nargs = '"'+str(len(jobdesc['inFiles'][0].split(",")))+'"'
                #for f,g in zip (jobdesc['inFiles'][0].split(","),jobdesc['GUID'][0].split(",")):
                #    nargs += ' "'+f+'" "' + g + '"'
                #outfiles
                nargs += ' "'+str(len(jobdesc['outFiles'][0].split(",")))+'"' 
                #for f,g in zip (jobdesc['outFiles'][0].split(","),jobdesc['destinationDblock'][0].split(",")):
                for f,g in zip (jobdesc['outFiles'][0].split(","),jobdesc['realDatasets'][0].split(",")):
                    nargs += ' "'+f+'" "None" "' + g + '"'
                #lists?
                nargs += ' "1" "inputhitsfile" "'
                for f in jobdesc['outFiles'][0].split(","):
                    nargs += ' '+f
                nargs+='"'
                #trfs
                nargs +=' "'+str(len(jobdesc['homepackage'][0].split(","))+1)+'"'
                trf=0
                trfs=jobdesc['transformation'][0].split('\n')
                trfargs=jobdesc['jobPars'][0].split('\n')
                for t in jobdesc['homepackage'][0].split('\n'):
                    nargs+=' "preinstalledCache"'
                    nargs+=' "APPS/HEP/ATLAS-'+atlasrtes[trf]+'"'
                    nargs+=' "'+trfs[trf]+'"'
                    nargs+=' "'+trfargs[trf]+'"'
                    trf+=1
                xrsl += '(arguments = '+nargs+')'
            else:
                xrsl += "(executable = ARCpilot)"
                #
                #......Pass the arguments on to the pilot job
                #
                pargs='"pilot3/pilot.py" "-h" "NDGF-condor" "-s" "Nordugrid" "-F" "Nordugrid-ATLAS" "-d" "{HOME}" "-j" "false" "-f" "false" "-z" "true" "-b" "2" "-t" "false"'
            # reduce disk space in jobdef
            ###preg=re.compile('maxDiskCount=\d+')
            ###pjobdef1=preg.sub('maxDiskCount=3000',pjobdef)
            ###print pjobdef1
                xrsl += '(arguments = "'+arte+'" "' + pjobdef  + '" '+pargs+ ')'
                #xrsl += '(arguments = "'+arte+'" "' + pjobdef1  + '" '+pargs+ ')'
              
            #
            #......The input files. The first is the grid job wrapper
            #
            xrsl += "(inputfiles ="
            #xrsl += "(NGpilot " + GetRLSurl("NGpilot") + ")"
            if self.conf.get(['executable','wrapper']) == "NGExecWrapper":
                xrsl += '(NGExecWrapper "'+self.conf.get(['executable','url'])+'")'
            else:
                xrsl += '(ARCpilot '+str(self.conf.get(['executable','wrapperurl']))+')'
                #xrsl += '(pilotcode.tar.gz '+str(self.conf.get(['executable','ptarurl']))+')'
                xrsl += '(pilotcode.tar.gz '+str(self.conf.get(['executable',wrapper]))+')'
    
            #for f,g in zip (jobdesc['inFiles'][0].split(","),jobdesc['GUID'][0].split(",")):
            #    lfn=self.GetLFN(g,pandaid)
            #    if lfn is None:
            #        return None
            #    xrsl += "(" + f + " " +lfn + ")"
            if(jobdesc.has_key('inFiles')):
                inf={}
                for f,g in zip (jobdesc['inFiles'][0].split(","),jobdesc['GUID'][0].split(",")):
                    lfn=self.GetLFN(g,pandaid)
                    if lfn is None:
                        return None
                    inf[f]=lfn
                # some files are double:
                for k,v in inf.items():
                    xrsl += "(" + k + " " + '"' + v + '"' + ")"
                    #xrsl += "(" + f + " " +lfn + ")"
            xrsl += ")"
            #
            #......The logfile (the untarred one)
            #
            xrsl += "(stdout = " + logfile.replace('.tgz','') + ")(join = yes)"
            #
            #......Avoid some bad queues!
            #
            #xrsl += "(queue!=sister)"
            #??????xrsl += "(queue!=serial_4gb)"
            #
            #......Tell where the grid-manager logfiles will be stored
            #
            xrsl += '(gmlog = "gmlog")'
            #xrsl += '(outputfiles=("/" "")'
            xrsl += '(outputfiles=("jobSmallFiles.tgz" "")'
            #
            #......The output files
            #
            #for (d,f) in zip(jobdesc['destinationDblock'][0].split(","),jobdesc['outFiles'][0].split(",")):
            #for (d,f) in zip(jobdesc['realDatasets'][0].split(","),jobdesc['outFiles'][0].split(",")):
            #    xrsl +='("' + f + '" "' + GetSRMurl(d,f,'ATLASMCDISK') + '")'
            #xrsl +=")"
    
    
            for (d,f,t,ds) in zip(jobdesc['realDatasets'][0].split(","),jobdesc['outFiles'][0].split(","),jobdesc['destinationDBlockToken'][0].split(","),jobdesc['destinationDblock'][0].split(",")):
               if t == "NULL":
                  t='ATLASDATADISK'
               if t is None:
                  t='ATLASDATADISK'
           #if jobdesc['cloud'][0] != 'ND':
           #   t='ATLASPRODDISK'
           #   d=ds
               xrsl +='("' + f + '" "' + self.GetSRMurl(d,f,t) + '")'
            xrsl +=")"
    
    
            # add additional xrsl lines
            for i in self.conf.get(["xrsl","item"]):
                xrsl += i
    
            # processingType (temporary fix for SLC5 evgen problems)
            #if jobdesc['processingType'][0] == "reprocessing" or ( jobname.find("evgen") != -1) :
            #   xrsl += '(|(queue=atlas-t1-repro)(queue=allq))'
        print jobdesc['currentPriority'][0]
        if jobname.find('tid530675') != -1:
          xrsl += '(|(queue=XXXatlas-t1-repro)(queue=XXXnormal)(queue=XXXswegrid)(queue=XXXgrad)(queue=default)(queue=XXXarc)(queue=all.q)(queue=XXXtier1))'
        elif jobname.find('tid00996772') != -1:
          xrsl += '(queue=normal)'
        elif jobname.find('tid00996773') != -1:
          xrsl += '(queue=normal)'
        elif jobname.find('XXXvalid') != -1:
          xrsl += '(queue=normal)'
        elif jobdesc['processingType'][0] == "XXXreprocessing" :
          xrsl += '(|(queue=batch)(queue=normal)(queue=XXXswegrid)(queue=grad)(queue=all)(queue=arc)(queue=all.q)(queue=Xgrid)(queue=lcg)(queue=grid)(queue=atlas))'
               #xrsl += '(|(queue=atlas-t1-repro)(queue=normal)(queue=swegrid)(queue=grad)(queue=default)(queue=arc)(queue=XXXall.q)(queue=XXXtier1))'
        #elif jobname.find("valid1") != -1:
            #   xrsl += '(|(queue=atlas-t1-repro)(queue=normal)(queue=swegrid))'
        elif jobname.find("XXXvalid2") != -1:
               xrsl += '(|(queue=atlas-t1-repro)(queue=normal)(queue=xxxxswegrid))'
        #elif jobname.find("mc10_valid") != -1:
            #   xrsl += '(|(queue=gridlong)(queue=xxxatlas-t1-reprocessing)(queue=xxxxswegrid))'
        elif jobname.find("tid167298") != -1:
               xrsl += '(|(queue=batch)(queue=xxx)(queue=xxxxswegrid))'
        elif jobname.find("tid167302") != -1:
               xrsl += '(|(queue=batch)(queue=xxx)(queue=xxxxswegrid))'
        elif jobname.find("tid151790") != -1:
               xrsl += '(&(queue!=batch)(queue!=XXXatlas-t1-reprocessing)(queue!=tier1)(queue!=t1)(queue!=ball.q))'
        elif ( jobname.find("XXXevgen") != -1 ) and ( jobname.find("XXXSherpa") != -1 ) :
               xrsl += '(|(queue=batch)(queue=xxxarc))'
        elif jobname.find("XXXevgen") != -1:
               #xrsl += '(|(queue=gridlong)(queue=arc)(queue=atlas-t1-reprocessing))'
               xrsl += '(|(queue=batch)(queue=default)(queue=arc)(queue=grad)(queue=normal))'
           #xrsl += '(cluster=arc-ce.smokerings.nsc.liu.se)'
        elif jobname.find("tid078736") != -1:
               xrsl += '(|(queue=batch)(queue=beall.q))'
        elif jobname.find("tid078737") != -1:
               xrsl += '(|(queue=arc)(queue=grad)(queue=swegrid))'
        elif jobname.find("recon") != -1:
               #xrsl += '(queue!=normal)'
               #xrsl += '(&(queue!=normal)(queue!=atlas-t1-repro)(queue!=atlas-t1-reprocessing))'
               xrsl += '(&(queue!=atlas-t1-repro)(queue!=XXXatlas-t1-reprocessing)(queue!=tier1)(queue!=t1)(queue!=XXXlcg))'
        elif jobname.find("digit") != -1:
               xrsl += '(&(queue!=atlas-t1-repro)(queue!=XXXlcg)(queue!=tier1)(queue!=t1))'
        elif jobname.find("merge") != -1:
               xrsl += '(&(queue!=atlas-t1-repro)(queue!=tier1)(queue!=XXXlcg))'
               #xrsl += '(|(queue=batch)(queue=arc)(queue=batch)(queue=swegrid)(queue=normal)(queue=grad)(queue=default))'
               #xrsl += '(queue=gridlong)'
               #xrsl += '(queue=atlas-t1-reprocessing)'
               #xrsl += '(queue=arc)'
        else:
               xrsl += '(&(queue!=atlas-t1-repro)(queue!=XXXatlas-t1-reprocessing))'
    
        #if jobname.find("tid080341") != -1:
        #   xrsl += '(cluster=jeannedarc.hpc2n.umu.se)'
    
        if jobname.find("tidX751854") != -1:
           xrsl += '(cluster=gateway01.dcsc.ku.dk)'
        if jobname.find("tid077990") != -1:
           xrsl += '(cluster=pikolit.ijs.si)'
        if jobname.find("tid077991") != -1:
           xrsl += '(cluster=arc-ce.smokerings.nsc.liu.se)'
        if jobname.find("tid077992") != -1:
           xrsl += '(cluster=nordugrid.unibe.ch)'
        if jobname.find("tid077993") != -1:
           xrsl += '(cluster=grid03.unige.ch)'
        if jobname.find("tid077994") != -1:
           xrsl += '(cluster=arc01.lcg.cscs.ch)'
        if jobname.find("tid078007") != -1:
           xrsl += '(cluster=arc-ce01.pdc.kth.se)'
        if jobname.find("tid201270") != -1:
           xrsl += '(cluster=pikolit.ijs.si)'
    
        # print str(xrsl)
    
        return str(xrsl)

    ## To move to Panda code ##
    def guidsTolfns(self):
        """
        translate input guids to lfns
        """
        jobs=self.db.getJobs("pstatus='sent' and trfstatus='tolfclfns' limit 200")
        if len(jobs) == 0:
            return 0
        self.log.info("Resolving lfns %d jobs:" % len(jobs))
        guids={}
        for j in jobs:
            jobdesc = cgi.parse_qs(j['pandajob'])
            if jobdesc.has_key('GUID'):
              for i in jobdesc['GUID'][0].split(","):
                  guids[i]=None

        lguids=self.GetLFNs(guids)
        self.log.info("Resolved %d lfns:" % len(lguids))

        for j in jobs:
            lfns=[]
            jobdesc = cgi.parse_qs(j['pandajob'])
            ok=True
            if jobdesc.has_key('GUID'):
              for i in jobdesc['GUID'][0].split(","):
                  if lguids[i] is None:
                      ok=False
            else:
                      lfns.append(i+"="+lguids[i])
            if not ok:
                  continue
            dlfns=",".join(lfns)
            #print dlfns
            n={}
            n['trfstatus']='tosubmit'
            n['lfns']=dlfns
            self.db.updateJob(j['pandaid'],n)


    def submit(self):
        """
        Main function to submit jobs.
        """

        global queuelist

        # check for stopsubmission flag
        if self.conf.get(['downtime','stopsubmission']) == "true":
            return 0

        jobs=self.db.getArcJobsInfo("arcstate='tosubmit' limit 1", ["pandaid", "jobdesc"])
        if len(jobs) == 0:
            #self.log.debug("No jobs to submit")
            return 0
        self.log.info("Submitting %d jobs:" % len(jobs))

        # GIIS setup
        giislist=self.conf.getList(['atlasgiis','item'])
        atlasgiisl=[]
        for g in giislist:
            # Specify explicitly EGIIS
            atlasgiisl.append(arc.Endpoint(str(g), arc.Endpoint.REGISTRY, "org.nordugrid.ldapegiis"))

        # retriever contains a list of CE endpoints
        retriever = arc.ComputingServiceRetriever(self.uc, atlasgiisl)
        retriever.wait()
        # targets is the list of queues
        # target.ComputingService.Name is the CE hostname
        # target.ComputingShare.Name is the queue name
        targets = retriever.GetExecutionTargets()
        
        # block rejected queues and clusters
        # TODO: When Submitter runs per site filter only sites for this process
        queuelist=[]
        for target in targets:
            s = self.db.getSchedconfig(target.ComputingService.Name)
            status = 'online'
            if s is not None:
                status=s['status']
            if target.ComputingShare.Name in self.conf.getList(['queuesreject','item']):
                pass
            elif target.ComputingService.Name in self.conf.getList(['clustersreject','item']):
                pass
            elif status == "XXXoffline":
                pass
            #elif target.ComputingService.Name == "jeannedarc.hpc2n.umu.se" and target.ComputingShare.Name == "atlas-t1-repro":
            #    pass
            else:
                # tmp hack
                target.ComputingShare.LocalWaitingJobs = 0
                target.ComputingShare.PreLRMSWaitingJobs = 0
                target.ExecutionEnvironment.CPUClockSpeed = 2000
                qjobs=self.db.getArcJobsInfo("cluster='" +str(target.ComputingService.Name)+ "' and  arcstate='submitted'", ['pandaid'])
                rjobs=self.db.getArcJobsInfo("cluster='" +str(target.ComputingService.Name)+ "' and  arcstate='running'", ['pandaid'])
                #jlimit = max ( len(rjobs)*0.20, 50)
                #jlimit = len(rjobs)*0.15 + 30
                jlimit = len(rjobs)*0.15 + 20
                #jlimit = 30000
                target.ComputingShare.PreLRMSWaitingJobs=len(qjobs)
                #if ( len(qjobs) < 200 ) :
                if len(qjobs) < jlimit:
                    queuelist.append(target)
                    self.log.debug("Adding target %s:%s" % (queuelist[-1].ComputingService.Name, target.ComputingShare.Name))
                #if target.ComputingService.Name == "lcg-lrz-ce2.grid.lrz.de":
                #   queuelist.append(target)
                if target.ComputingService.Name == "vm009.gla.scotgrid.ac.uk":
                    target.ExecutionEnvironment.CPUClockSpeed = 3000

        tlist=[]

        self.log.info("start submitting")

        # mark submitting in db
        for j in jobs:
            jd={'arcstate': 'submitting', 'tarcstate': str(time.time())}
            self.db.updateArcJob(j['pandaid'],jd)

        for j in jobs:
            self.log.debug("preparing: %s" % j['pandaid'])
            jobdescstr = str(j['jobdesc'])
            jobdescs = arc.JobDescriptionList()
            if not jobdescstr or not arc.JobDescription_Parse(jobdescstr, jobdescs):
                self.log.error("Failed to prepare job description %d" % j['pandaid'])
                continue
            t=SubmitThr(Submit,j['pandaid'],jobdescs,self.uc,self.log)
            tlist.append(t)
            #t.start()

        #aCTUtils.RunThreadsSplit(tlist,10)
        self.RunThreadsSplit(tlist,1)
        self.log.info("threads finished")

        self.log.info("end submitting")


    def checkFailedSubmissions(self):

        jobs=self.db.getArcJobsInfo("arcstate='submitting'", ["pandaid"])

        # TODO query GIIS for job name specified in description to see if job
        # was really submitted or not
        for j in jobs:
            # set to toresubmit and the application should figure out what to do
            self.db.updateArcJob(j['pandaid'], {"arcstate": "toresubmit",
                                                "tarcstate": time.time()})


    def run(self):
        """
        Main loop
        """
        try:
            while 1:
                # parse config file
                self.conf.parse()
                # TODO handle submitting
                self.checkFailedSubmissions()        
                # submit jobs
                self.submit()
                aCTUtils.sleep(1)
                # restart periodicaly for gsiftp crash
                ip=int(self.conf.get(['periodicrestart','submitter']))
                if time.time()-self.starttime > ip and ip != 0 :
                    self.log.info("Submitter exited for periodic restart")
                    return
        except aCTSignal.ExceptInterrupt,x:
            print x
            return

    def finish(self):
        self.log.info("Cleanup")

# Main
if __name__ == '__main__':
    asb=aCTSubmitter()
    asb.run()
    asb.finish()
    
