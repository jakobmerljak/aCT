import os
import time
import aCTDBPanda, aCTDBArc
from arclib import *
import aCTConfig
import re
import commands
from xml.dom import minidom
from xml.sax import SAXParseException
import LFCTools
import shutil
from xml.dom.ext import PrettyPrint
import aCTLogger
import aCTSignal
import aCTUtils
import cgi
import pickle
from threading import Thread,enumerate
import subprocess


def checkSrm(surl,chksum,dbg=False):
  cmd="ngls -l "+surl
  p = subprocess.Popen(cmd, shell=True,stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
  (fi,fo,fe)=(p.stdin, p.stdout, p.stderr)
  out=fo.readlines()
  p.wait()
  fi.close()    
  fo.close()            
  fe.close()                    

  if len(out) == 0:
    return False

  res=re.match('.+adler32:([^\s]+).+',out[0])
  if dbg:
    print "dbg checkSrm: ",surl,chksum,out[0]
  if res.group(1).find(chksum) != -1:
    return True
    
  return False



class DownloaderThr(Thread):
    """
    Helper function to be used in jobs downloading threads
    """
    def __init__ (self,func,job,ajob,dataset,turls):
        Thread.__init__(self)
        self.func=func
        self.job = job
        self.ajob = ajob
        self.dataset = dataset
        self.turls = turls
        self.status={}
    def run(self):
        self.status=self.func(self.job,self.ajob,self.dataset,self.turls)


class aCTDownloader:
    """
    Class to handle job checking and job downloading/retreival.
    """

    def __init__(self):
        # logger
        self.logger=aCTLogger.aCTLogger("downloader")
        self.log=self.logger()

        # config
        self.conf=aCTConfig.aCTConfig()
        # database
        self.dbpanda=aCTDBPanda.aCTDBPanda(self.log,self.conf.get(["db","file"]))
        self.dbarc=aCTDBArc.aCTDBArc(self.log,self.conf.get(["db","file"]))
        # ARC FTPControl
        #### self.ftpcontrol=FTPControl()
        # store the last checkJobs time to avoid overloading of GIIS
        self.checktime=time.time()
        self.checktimep=time.time()
        # start time for periodic restart
        self.starttime=time.time()
        
        # temporary directories
        try:
            os.mkdir(self.conf.get(['tmp','dir']))
        except:
            pass
        try:
            os.mkdir(self.conf.get(['tmp','dir'])+"/xml")
        except:
            pass
        try:
            os.mkdir(self.conf.get(['tmp','dir'])+"/pickle")
        except:
            pass
        try:
            os.mkdir(self.conf.get(['tmp','dir'])+"/failedjobs")
        except:
            pass


    def getJobTimes(self,jobinfo):

        # rough classification of job status
        jobstatus = jobinfo.status
        if jobstatus=='FINISHED':
            status = 'finished'
        elif jobstatus=='FAILED':
            status = 'failed'
        else:
            status='UNKNOWN'

        # Sometimes the walltime is not available but the cputime is.
        walltime = int(jobinfo.used_wall_time)
        if walltime == -1:
            walltime = None
        cputime = int(jobinfo.used_cpu_time)
        if cputime == -1:
            cputime = None

        if walltime >= 0:
            usedtime = walltime
        elif cputime >= 0:
            usedtime = cputime
        else:
            usedtime = 0


        endtime = jobinfo.completion_time.GetTime()
        if endtime == -1:
            endtime = None
    
        submissiontime = jobinfo.submission_time.GetTime()
        if submissiontime == -1:
            submissiontime = None

        if endtime != None:
            starttime = endtime - usedtime
        elif submissiontime != None:
            if status=='finished':
                starttime = submissiontime
                endtime = starttime + usedtime
            elif status=='failed':
                starttime = submissiontime
                endtime = starttime + usedtime
            else:
                starttime = submissiontime
        else:
            starttime = None
            endtime = None

        #print "status ",jobinfo.status,"| ,submission,end,wall,cpu ",submissiontime,endtime,walltime,cputime,"| starttime,endtime,walltime:",starttime,endtime,walltime
        return self.dbpanda.getTimeStamp(starttime), self.dbpanda.getTimeStamp(endtime)




    def logUrl(self,arcjobid):
        res=re.match('gsiftp://(.+):2811/jobs/(\w+)',arcjobid)
        logurl=self.conf.get(["joblog","urlprefix"])+"/"+res.group(1)+"/"+res.group(2)
        return logurl

    def checkAthenaFailed(self,j):

        """
        Check the jobs failed in athena: only log tarball has to be uploaded
        to LFC/SRM. Jobs are reported as failed in upload
        j: job to be ckecked
        """
        
        self.log.info("checking: %d" % j['pandaid'])
        #c=self.ftpcontrol
        c=FTPControl()

        # find sessionid and create tmp dirs
        jid=str(j['arcjobid'])
        #reg=re.search('.+/(\w+)',jid)
        reg=re.match('gsiftp://(.+):2811/jobs/(\w+)',jid)
        sessionid=reg.group(2)
        outd=self.conf.get(['tmp','dir'])+"/"+sessionid
        try:
            os.mkdir(outd)
        except:
            pass

        # download output list provided by GM and jobSmallFiles.tgz
        try:
            c.Download(jid + "/gmlog/output",str(outd+"/output"))
            c.Download(jid + "/jobSmallFiles.tgz",str(outd+"/jobSmallFiles.tgz"))
        except FTPControlError,x:
            self.log.error(x)
            return False
        except Exception,x:
            self.log.error("Failed downloading: %d" % x)
            return False

        command="(cd "+outd+"; tar -zxf jobSmallFiles.tgz)"
        rc=commands.getstatusoutput(command)

        # get list of files in metadata. For failed jobs only one entry should be there
	try:
          outputxml = minidom.parse(outd + "/metadata.xml")
        except Exception,x:
          self.log.error("Failed downloading: %s" % x)
          return False
        files = outputxml.getElementsByTagName ("POOLFILECATALOG")[0].getElementsByTagName ("File")

        # jobs is successful if more files are to be uploaded
        # return and pass to rerun command to retry the upload for data files
        if len(files) != 1:
            return False
        logfile=files[0].getElementsByTagName("logical")[0].getElementsByTagName("lfn")[0].getAttribute("name")
        
        f = open(outd+"/output",'r')
        lines = f.readlines()
        f.close()

        # check if log tarball is not in the list (was uploaded)
	
        turls={}
        l=str(j['turls'])
        # strip ' at beginning and end
        l=l[1:][:-1]
        t=l.split("\n")
        for l in t:
            i=l.split(" ")
            turls[i[0]]=i[1]
        print "log turl ",turls[logfile]
        cmd="ngls -l "+turls[logfile]
        cmd=cmd.replace(";","\;")
        p = subprocess.Popen(cmd, shell=True,stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
        (fi,fo,fe)=(p.stdin, p.stdout, p.stderr)
        out=fo.readlines()
        p.wait()
        fi.close()    
        fo.close()            
        fe.close()   

        if len(out) == 0:
           return False

        print out[0]

        res=re.match('.+adler32:([^\s]+).+',out[0])
        if res is not None:
          return True

        uploaded=True
        for l in lines:
            if logfile in l:
                uploaded=False
        return uploaded

    def checkJobsPStatus(self):
        """
        frequent update of pstatus
        """
        if time.time()<self.checktimep+int(self.conf.get(['jobs','checkmintime'])):
            self.log.debug("mininterval not reached")
            return
        self.checktimep=time.time()


        self.log.info("Updating pstatus")
        #jobs=self.dbpanda.getJobs("( pstatus='running' or pstatus='starting' or pstatus='transferring' )")
        jobs=self.dbpanda.getJobs("( pstatus='running' or pstatus='starting' or pstatus='transferring' ) and arcjobid not like ''")

        jlist=[]
        for job in jobs:
            jlist.append(str(job['arcjobid']))

        # query GIIS on selected jobs
        count=0
        timeout=int(self.conf.get(['atlasgiis','timeout']))
        jobinfos =  GetJobInfo(jlist,MDS_FILTER_JOBINFO,True,GetEffectiveSN(),timeout)
        for (job,ji) in zip (jobs, jobinfos):
            if(ji.status == ''):
                self.log.error( "Failed to check: %d %s" % (job['pandaid'],job['arcjobid']))
                continue
            pstatus=""
            if ji.status == "INLRMS:R":
                pstatus='running'
            else:
                if job['pstatus'] == 'running' or job['pstatus'] == 'transferring':
                    pstatus='transferring'
                else:
                    pstatus='starting'
            if job['pstatus'] != pstatus or ( ji.status == 'FINISHED' and job['trfstatus'] == 'inarc' ) or ( job['arcstatus'] != ji.status and job['arcstatus'] != 'rerunned' ) :
                count+=1
                jd={}
                jd['pstatus']=pstatus
                # expire heartbeat
                jd['theartbeat']=self.dbpanda.getTimeStamp(time.time()-2*int(self.conf.get(['panda','heartbeattime'])))
                # expire check time
                jd['tarcstatus']=self.dbpanda.getTimeStamp(time.time()-2*int(self.conf.get(['jobs','checkinterval'])))
                self.dbpanda.updateJob(job['pandaid'],jd)
        #self.dbpanda.Commit()
        self.log.info("updated %d jobs" % count)
        
        
    def checkJobs(self,trfstatus='inarc'):
        """
        check the atlas GIIS for job status
        """
        # TODO database is locked for too long with lazy -> split
        
        # minimum time between checks
        if time.time()<self.checktime+int(self.conf.get(['jobs','checkmintime'])):
            self.log.debug("mininterval not reached")
            return
        self.checktime=time.time()

        # check jobs which were last checked more than checkinterval ago
        jobs=self.dbpanda.getJobs("( pstatus='running' or pstatus='starting' or pstatus='transferring' ) and arcjobid like 'gsiftp%' and trfstatus like '"+trfstatus+"' and tarcstatus<strftime('%s','now')-"+str(self.conf.get(['jobs','checkinterval'])) + " limit 100000")

        # TODO: make function for this in aCTDBPanda
        # number of total jobs
        njobs=self.dbpanda.getNJobs()
        timeout=int(self.conf.get(['atlasgiis','timeout']))

        # Do not check too little jobs at once (at least 1% of running jobs)
        if len(jobs) < njobs/1000:
            #self.log.debug("too few to check %d" % len(jobs))
	    return
        if len(jobs):
            self.log.info("%d" % len(jobs))
        else:
            return

        
        jlist=[]
        for job in jobs:
            jlist.append(str(job['arcjobid']))

        # query GIIS on selected jobs
        jobinfos =  GetJobInfo(jlist,MDS_FILTER_JOBINFO,True,GetEffectiveSN(),timeout)
        for (job,ji) in zip (jobs, jobinfos):
            if(ji.status == ''):
                self.log.error( "Failed to check: %d %s" % (job['pandaid'],job['arcjobid']))
            	#jd={}
                #jd['trfstatus']="toresubmit"
            	#self.dbpanda.updateJob(job['pandaid'],jd)
                continue
            self.log.debug( "%d %s %d %s %f %f %s %s" % (job['pandaid'],ji.status,ji.exitcode,ji.cluster,ji.used_cpu_time,ji.used_wall_time,ji.used_memory,ji.proxy_expire_time))


            
            jd={}
            # do this check always (True)
            if job['arcstatus'] != ji.status or True:
                jd['arcstatus']=ji.status
                jd['tarcstatus']=self.db.getTimeStamp()
                # finished jobs
                if ji.status == "FINISHED" and ji.exitcode <= 0:
                    jd['trfstatus']="tofinished"
                elif ji.rerunable == "FINISHING":
                    # check athena failed
                    # allow uploading only log tarball
        	    self.dbpanda.Commit()
                    if self.checkAthenaFailed(job):
                        jd['trfstatus']="tofinished"
                    else:
                        jd['trfstatus']="torerun"
                elif ji.rerunable == "PREPARING":
                    # rerun failed in download
                    jd['trfstatus']="torerun"
                elif ( ji.status == "FINISHED" and ji.exitcode > 0 ) or \
                         ji.status == "KILLED" or \
                         ji.status == "DELETED":
                    # failed jobs ??? to test
                    jd['trfstatus']="tofailed"

                elif ji.status == "FAILED" :
                    jd['trfstatus']='tofailed'
            else:
                jd['tarcstatus']=self.dbpanda.getTimeStamp()

            # update the database
            if jd['arcstatus'] == "INLRMS:R" :
                jd['pstatus'] = 'running'
            else:
                if job['pstatus'] == 'running' or job['pstatus'] == 'transferring':
                    jd['pstatus']='transferring'
                else:
                    jd['pstatus']='starting'

            self.dbpanda.updateJobLazy(job['pandaid'],jd)
            ###self.dbpanda.updateJob(job['pandaid'],jd)

            # update arcstatus
            ###AF jb=self.dbpanda.getJob(job['pandaid'])
	    # this is very slow... 
            row=self.dbarc.getArcJob(job['pandaid'])
            ###AF if row is None or jb['tarcstatus'] != row['tarcstatus'] :
            if row is None or job['tarcstatus'] != row['tarcstatus'] :
                #self.log.info("jobtimes: %s %s" % (st,et))
                jd={}
                jd['tarcstatus']=job['tarcstatus']
                jd['cluster']=ji.cluster
		try:
                  jd['submissiontime']=ji.submission_time.str()
                  jd['completiontime']=ji.completion_time.str()
                  jd['usedcputime']=ji.used_cpu_time
                  jd['usedwalltime']=ji.used_wall_time
		except TimeError,x:
		  self.log.error(x)
		  pass
                jd['errors']=str(ji.errors).replace("'","")
                jd['exitcode']=ji.exitcode
                jd['executionnodes']=" ".join(ji.execution_nodes)
		# self.log.error(str(ji.proxy_expire_time))
		# Bug in arclib for time : 1970-01-01 00:59:59
		if str(ji.proxy_expire_time).find("1970") == -1 :
                  jd['proxyexpiretime']=Time(str(ji.proxy_expire_time)).str()
                jd['rerunable']=ji.rerunable

                # get proper start/end time 
                (st,et)=self.getJobTimes(ji)
                if st is not None:
                    jd['starttime']=st
                if et is not None:
                    jd['endtime']=et
                    
                self.dbarc.updateArcJobLazy(job['pandaid'],jd)
                ###self.dbarc.updateArcJob(job['pandaid'],jd)
        self.log.info("Done")
        #dbpanda.Commit() commits for ARC and Panda tables
        self.dbpanda.Commit()
    
    def processRerunable(self):
        """
        Try to rerun failed in download/upload
        """
            
        jobs=self.dbpanda.getJobs(" ( pstatus='running' or pstatus='transferring' or pstatus='starting' )  and trfstatus='torerun' limit 500")
        if len(jobs):
            self.log.info("%d" % len(jobs))
        else:
            return

        # add downtime logic!!!!
        if self.conf.get(['downtime','srmdown']) == "true":
            self.log.info("srm downtime: no rerunning")
            return
        if self.conf.get(['downtime','lfcdown']) == "true":
            self.log.info("lfc downtime: no rerunning")
            return

        c=JobFTPControl()
        for j in jobs:
            try:
                res=re.match('gsiftp://(.+):2811/jobs/(\w+)',str(j['arcjobid']))
                cluster=res.group(1)
                if cluster in self.conf.getList(['downtime','cluster','item']):
                    continue
                jd={}
                if j['nrerun'] is None:
                    jd['nrerun']=0
                else:
                    jd['nrerun']=j['nrerun']
                jd['nrerun']+=1
                self.log.info("%s try to rerun count %d " % (j['pandaid'],jd['nrerun']))
                if jd['nrerun']>=int(self.conf.get(['jobs','maxrerun'])):
                    # proper completion waiting!!!
                    #jd['trfstatus']='tofailed'
                    #jd['trfstatus']='toresubmit'
                    jd['trfstatus']='tofailed'
                else:
                    c.RenewCreds(str(j['arcjobid']))
                    c.Resume(str(j['arcjobid']))
                    jd['arcstatus']='rerunned'
                    jd['tarcstatus']=self.dbpanda.getTimeStamp(time.time()+300)
                    jd['trfstatus']='inarc'
                    self.log.info("%s reruned " % j['pandaid'])
                self.dbpanda.updateJob(j['pandaid'],jd)
            except FTPControlError,x:
                self.log.error(x)
		if str(x).find("Server responded: Job can't be restarted") != -1:
		  self.log.info("handling rerunning")
		  jd['arcstatus']='rerunned'
		  jd['tarcstatus']=self.dbpanda.getTimeStamp()
		  jd['trfstatus']='inarc'
		  self.log.info("%s moved back to inarc " % j['pandaid'])
		  self.dbpanda.updateJob(j['pandaid'],jd)
                #jd['trfstatus']='tofailed'
                #self.dbpanda.updateJob(j['pandaid'],jd)
                continue
            except Exception,x:
                self.log.error(x)
                continue

    def processResubmitted(self):
        """
        Cancel the job and resubmit. (for jobs queued to long or for clusters in downtime)
        """
        jobs=self.dbpanda.getJobs("( pstatus='running' or pstatus='transferring' or pstatus='starting' )  and trfstatus='toresubmit' limit 500")
        if len(jobs):
            self.log.info("%d" % len(jobs))
        else:
            return
        c=JobFTPControl()
        for j in jobs:
            try:
                self.log.info("try to resubmit %s " % j['pandaid'])
                try:
		    if j['arcjobid']:
                      c.Cancel(str(j['arcjobid']))
                except FTPControlError,x:
                    self.log.error(x)
		    pass
                try:
		    if j['arcjobid']:
                      c.Clean(str(j['arcjobid']))
                      RemoveJobID(str(j['arcjobid']))
                except FTPControlError,x:
                    self.log.error(x)
                    pass
                jd={}
                jd['pstatus']='sent'
                jd['tarcstatus']=self.dbpanda.getTimeStamp()
                jd['trfstatus']='tosubmit'
                self.log.info("%s resubmitted " % j['pandaid'])
                self.dbpanda.updateJob(j['pandaid'],jd)
            except Exception,x:
                self.log.error(x)
                continue


        
    def processCompleted(self,pstatus='done',trfstatus='toremove'):
        """
        Remove completed jobs from database and clean the jobs on clusters.
        """
        jobs=self.dbpanda.getJobs("pstatus like '"+pstatus+"' and trfstatus='"+trfstatus+"'")
        if len(jobs):
            self.log.info("%d" % len(jobs))
        else:
            return
        c=JobFTPControl()
        # TODOthreaded
        for j in jobs:
            self.log.info("Removing job %d %s" % (j['pandaid'],j['arcjobid']))
            try:
                jl=[]
                jl.append(str(j['arcjobid']))
                if trfstatus == 'tokill':
                    c.Cancel(str(j['arcjobid']))
                print "Removing",j['pandaid'],j['arcjobid']
                try:
                    c.Clean(str(j['arcjobid']))
                except:
                    pass
                try:
                    RemoveJobID(str(j['arcjobid']))
                except:
                    pass
                self.dbpanda.removeJobs(j['pandaid'])
                # clean xml and pickle
                try:
                    os.unlink(self.conf.get(['tmp','dir'])+"/xml/"+str(j['pandaid'])+".xml")
                except:
                    pass
                try:
                    os.unlink(self.conf.get(['tmp','dir'])+"/pickle/"+str(j['pandaid'])+".pickle")
                except:
                    pass
            except FTPControlError,x:
                self.log.error(x)
                continue
            except Exception,x:
                self.log.error("%s" % x )
                continue
            
    def processKilled(self):
        """
        not used
        """
        jobs=self.dbpanda.getJobs("trfstatus='tobekilled' or pstatus='tobekilled'")

        if len(jobs):
            self.log.info("%d" % len(jobs))
        else:
            return

        c=JobFTPControl()

        for j in jobs:
            aj=self.dbarc.getArcJob(j['pandaid'])
            try:
                if j['arcjobid']:
                    c.Cancel(str(j['arcjobid']))
            except FTPControlError,x:
                self.log.error(x)
                pass
            p={}
            p['siteName']=self.conf.get(['panda','site'])
            p['schedulerID']=self.conf.get(['panda','schedulerid'])
	    try:
              p['pilotID']=self.logUrl(str(j['arcjobid']))+"|Unknown|Unknown"
              p['node']=aj['executionnodes']
              #p['node']='None'
              #p['computingElement']='None'
              p['pilotLog']='!!FAILED!!1001!!Job was killed\narcstatus: %s\narcexitcode: %s\narcerrors %s' % (j['arcstatus'],aj['exitcode'],aj['errors'])
              p['pilotErrorDiag']="job was killed. arcerror: "+aj['errors']
              #p['pilotErrorCode']=1205
              p['pilotErrorCode']=1144
	      if int(aj['usedwalltime']) > 1000000 :
		aj['usedwalltime']="0"
              p['pilotTiming']="0|0|%s|0" % aj['usedwalltime']
              p['cpuConsumptionTime']=aj['usedcputime']
              p['cpuConversionFactor']=1
              p['cpuConsumptionUnit']='seconds'
              # set start/endtime
              p['startTime']=time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(aj['starttime']))
              p['endTime']=time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(aj['endtime']))
	    except:
	      pass

            f=open(self.conf.get(['tmp','dir'])+"/pickle/"+str(j['pandaid'])+".pickle","w")
            pickle.dump(p,f)
            f.close()
            jd={}
            jd['pstatus']='failed'
            jd['trfstatus']='topanda'
            self.dbpanda.updateJob(j['pandaid'],jd)
            
    
    def getDatasets(self,pandaid):
        """
        decode the job datasets from the pandajob description in database
        """
        j=self.dbpanda.getJob(pandaid)
        #print j
        jobdesc = cgi.parse_qs(j['pandajob'])
        datasets={}
        for (d,f) in zip(jobdesc['realDatasets'][0].split(","),jobdesc['outFiles'][0].split(",")):
            datasets[f]=d
        return datasets

    def checkFailed(self):
        jobs=self.dbpanda.getJobs("( pstatus='running' or pstatus='transferring' or pstatus='starting' )  and trfstatus='tofailed'")
	for j in jobs:
	  aj=self.dbarc.getArcJob(j['pandaid'])
	  resubmit=0
	  for error in self.conf.getList(['errors','toresubmit','arcerrors','item']):
	    if aj['errors'].find(error) != -1:
	      resubmit=1
	  if resubmit:
	    self.log.info("%d %s %s" % (j['pandaid'],j['arcjobid'],aj['errors']))
	    jd={}
	    jd['pstatus'] = 'starting'
	    jd['trfstatus']='toresubmit'
	    self.dbpanda.updateJob(j['pandaid'],jd)

    def downloadFailed(self):
        """
        process jobs failed for other reasons than athena (log_extracts was not created by pilot)
        """
        jobs=self.dbpanda.getJobs("( pstatus='running' or pstatus='transferring' or pstatus='starting' )  and trfstatus='tofailed' limit 300 ")
        if len(jobs):
            self.log.info("%d" % len(jobs))
        else:
            return

	# to avoid failures? unable to connect
        #c=self.ftpcontrol
        c=FTPControl()

        for j in jobs:
            xml=""
            log=""
            jid=str(j['arcjobid'])
            #reg=re.search('.+/(\w+)',jid)
            reg=re.match('gsiftp://(.+):2811/jobs/(\w+)',jid)
            sessionid=reg.group(2)
            #outd=self.conf.get(['tmp','dir'])+"/"+sessionid
            #outd=self.conf.get(['tmp','dir'])+"/failedjobs/"+str(j['pandaid'])
            res=re.match('gsiftp://(.+):2811/jobs/(\w+)',str(j['arcjobid']))
            cluster=res.group(1)
            if cluster in self.conf.getList(['downtime','cluster','item']):
                continue

            try:
                os.mkdir(self.conf.get(['joblog','dir']) + "/" + cluster )
            except:
                pass
            outd = self.conf.get(['joblog','dir']) + "/" + cluster + "/" + sessionid


            try:
                shutil.rmtree(outd)
            except:
                pass
            try:
                os.mkdir(outd)
                os.mkdir(outd+"/gmlog")
            except:
                pass
            
            self.log.debug("----------------------------------------------")
            self.log.debug("Downloading %s" % jid)


            # download the whole job directory
            try:
                #c.DownloadDirectory(jid ,str(outd))
		fl=c.ListDir(jid)
		res=re.match('gsiftp://(.+):2811/jobs/(\w+)',jid)
		cluster=res.group(1)
		logfile=None
		for f in fl:
			if f.filename.find("/log.") != -1:
				logfile = "gsiftp://"+cluster+f.filename
                c.DownloadDirectory(jid+"/gmlog" ,str(outd)+"/gmlog")
		if logfile is not None:
			c.Download(logfile,str(outd)+"/log")
            except FTPControlError,x:
                self.log.error(x)
		pass
                #return None
            except Exception,x:
                self.log.error("Failed downloading: %d" % x)
                return None

            # prepare extracts
            nlines=20
            log=""
	    try:
              f=open(outd+"/gmlog/failed","r")
              log+="---------------------------------------------------------------\n"
              log+="GMLOG: failed\n"
              log+="---------------------------------------------------------------\n"
              log+=''.join(f.readlines())
              f.close()
	    except:
	      pass
            

            import glob
            lf=glob.glob(outd+"/log*")
            try:
                f=open(lf[0],"r")
                lines=f.readlines()
                log+="---------------------------------------------------------------\n"
                log+="LOGFILE: tail\n"
                log+="---------------------------------------------------------------\n"
                lns=[]
                for l in lines:
                    if re.match('.*error',l,re.IGNORECASE):
                        lns.append(l)
                    if re.match('.*warning',l,re.IGNORECASE):
                        lns.append(l)
                    if re.match('.*failed',l,re.IGNORECASE):
                        lns.append(l)
                log+=''.join(lns[:nlines])
                # copy logfiles to failedlogs dir
                try:
                    os.mkdir(self.conf.get(["tmp","dir"])+"/failedlogs")
                except:
                    pass
                try:
                    f=open(self.conf.get(["tmp","dir"])+"/failedlogs/"+str(j['pandaid'])+".log","w")
                    f.write(log)
                    f.close()
                except:
                    pass

            except:
                pass

            print log

            xml=""
            # xml and log
            aj=self.dbarc.getArcJob(j['pandaid'])

            # set update, pickle from pilot is not available
            # some values might not be properly set
            # TODO synchronize error codes with the rest of production
            pupdate={}
            pupdate['xml']=str(xml)
            pupdate['siteName']='ARC'
            pupdate['computingElement']=aj['cluster']
            pupdate['schedulerID']=self.conf.get(['panda','schedulerid'])
            pupdate['pilotID']=self.logUrl(str(j['arcjobid']))+"|Unknown|Unknown"
            pupdate['node']=aj['executionnodes']
            pupdate['pilotLog']=log
            pupdate['cpuConsumptionTime']=aj['usedcputime']
            pupdate['cpuConsumptionUnit']='seconds'
            pupdate['cpuConversionFactor']=1
            pupdate['pilotTiming']="0|0|%s|0" % aj['usedwalltime']
            pupdate['exeErrorCode']=aj['exitcode']
            pupdate['exeErrorDiag']=aj['errors']
            pupdate['pilotErrorCode']=1008
	    codes=[]
	    codes.append("Job timeout")
	    codes.append("qmaster enforced h_rt limit")
	    codes.append("job killed: wall")
	    codes.append("Job exceeded time limit")
	    for errcode in codes:
	      res=re.match(".*"+errcode+".*",aj['errors'])
	      if res is not None:
                pupdate['pilotErrorCode']=1213
	        print pupdate['pilotErrorCode'],aj['errors']
	    codes=[]
	    codes.append("Job probably exceeded memory limit")
	    codes.append("job killed: vmem")
	    codes.append("pvmem exceeded")
	    for errcode in codes:
	      res=re.match(".*"+errcode+".*",aj['errors'])
	      if res is not None:
                pupdate['pilotErrorCode']=1212
	        print pupdate['pilotErrorCode'],aj['errors']
            pupdate['pilotErrorDiag']=aj['errors']
            # set start/endtime
            pupdate['startTime']=time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(aj['starttime']))
            pupdate['endTime']=time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(aj['endtime']))
            # save the pickle file to be used by aCTMain panda update
            f=open(self.conf.get(['tmp','dir'])+"/pickle/"+str(j['pandaid'])+".pickle","w")
            pickle.dump(pupdate,f)
            f.close()

            # clean up the tmp dir
	    # do not clean joblog
            ### try:
            ###    shutil.rmtree(outd)
            ### except:
            ###    pass

            jd={}
            jd['pstatus']='failed'
            jd['trfstatus']='topanda'
            self.dbpanda.updateJob(j['pandaid'],jd)

    def downloadPilotThr(self,trfstatus):
        """
        download job files in threads
        """
        if self.conf.get(['downtime','lfcdown']) == "true":
            self.log.info("lfc downtime: no downlading")
            return
        
        jobs=self.dbpanda.getJobs("( pstatus='running' or pstatus='transferring' or pstatus='starting' ) and trfstatus='"+trfstatus+"' limit %d" % int(self.conf.get(['loop','maxjobs','downloader'])))
        if len(jobs) == 0:
            return 0
        self.log.info("downloading: %d" % len(jobs))
        if len(jobs) == 0:
            return None
               
        tlist=[]
        tcount=0
        lfcinput={}

        jobsok=[]
        
        # prepare threads and arguments for downloadJobPilotThr 
        for j in jobs:
            # check for host down here
            reg=re.search('.+//([^:]+)',str(j['arcjobid']))
            cluster=reg.group(1)
            if cluster in self.conf.getList(['downtime','cluster','item']):
                continue
            tcount+=1
            aj=self.dbarc.getArcJob(j['pandaid'])
            datasets=self.getDatasets(j['pandaid'])
            turls=self.GetOutputTurls(j['pandaid'])
            t=DownloaderThr(self.downloadJobPilotThr,j,aj,datasets,turls)
            tlist.append(t)

        # tun threads
        aCTUtils.RunThreadsSplit(tlist,int(self.conf.get(['loop','threads','downloader'])))

        # wait for threads to complete
        for t in tlist:
	    if t.status == "tobekilled":
		jd={}
	        jd['trfstatus']="tobekilled"
		self.dbpanda.updateJob(t.job['pandaid'],jd)
            elif t.status is not None:
                jobsok.append(t.job)
                lfcinput.update(t.status)

        self.log.info("Threads finished")
        self.log.info("starting LFC registration")

        # register all output files in LFC at once
        outp=True
        try:
            # result = LFCTools.bulkRegisterFiles(str(self.conf.get(['lfc','host'])).split("/")[2], lfcinput)
            # #for guid in result:
	    # for guid in lfcinput.keys():
            #     #if isinstance(result[guid], LFCTools.LFCFileCatalogException):
	    #	if not ( result[guid] == True ) :
            #        self.log.error( 'ERROR: LFC exception during registration: %s %s' % (guid,result[guid]))
            #        outp = False
	    pass
        except LFCTools.LFCFileCatalogException,x:
            self.log.error( x )
            outp=False
        except Exception,x:
            self.log.error( x )
            outp=False
	except:
	    self.log.error("Unclassified exception during LFC bulk registration")
	    outp = False

        if outp:
            # update job for final panda status update
            jd={}
            jd['trfstatus']='topanda'
            jd['pstatus']='finished'
            print jd
            for j in jobsok:
                print "Updating job ",j['pandaid']
                self.dbpanda.updateJob(j['pandaid'],jd)
            self.log.info("LFC ok")
        else:
            self.log.error("Failed in LFC registration")

        return len(jobs)

    def downloadJobPilotThr(self,j,aj,datasets,turls):
        """
        download single job in a thread
        """

        self.log.info("processing: %d" % j['pandaid'])
        #c=self.ftpcontrol
        c=FTPControl()

        xml=""
        log=""
        jid=str(j['arcjobid'])
        #reg=re.search('.+/(\w+)',jid)
        reg=re.match('gsiftp://(.+):2811/jobs/(\w+)',jid)
        sessionid=reg.group(2)
        outd=self.conf.get(['tmp','dir'])+"/"+sessionid
        try:
            os.mkdir(outd)
        except:
            pass
        self.log.debug("----------------------------------------------")
        self.log.debug("Downloading %s" % jid)


        # download small files
        try:
	    if os.path.exists(str(outd+"/jobSmallFiles.tgz")):
	    	if os.path.getsize(str(outd+"/jobSmallFiles.tgz")) == 0:
                    os.unlink(str(outd+"/jobSmallFiles.tgz"))
	    if not os.path.exists(str(outd+"/jobSmallFiles.tgz")):
                c.Download(jid + "/jobSmallFiles.tgz",str(outd+"/jobSmallFiles.tgz"))
        except FTPControlError,x:
            self.log.error("%s: %s" % (jid,x))
            return None
        except Exception,x:
            self.log.error("Failed downloading: %d" % x)
            return None

        command="(cd "+outd+"; tar -zxf jobSmallFiles.tgz)"
        try:
            rc=commands.getstatusoutput(command)
        except Exception,x:
            self.log.error(x)
            # TODO something must be wrong with such job -> mark failed
            return None

        try:
            outputxml = minidom.parse(outd + "/metadata.xml")
        except Exception,x:
            self.log.error(x)
            # TODO something must be wrong with such job -> mark failed
            return None
        files = outputxml.getElementsByTagName ("POOLFILECATALOG")[0].getElementsByTagName ("File")

        # pilot status update file
        f=open(outd+"/panda_node_struct.pickle","r")
        pupdate = pickle.load(f)
        f.close()


        # output metadata file
        xml=""
        f = open(outd+"/metadata.xml",'r')
        for line in f:
            xml+=line
        f.close()

        # Rewrite update
        pupdate['xml']=str(xml)
        pupdate['siteName']='ARC'
        #pupdate['siteName']='ARC-T2'
	pupdate['computingElement']=aj['cluster']
        pupdate['schedulerID']=self.conf.get(['panda','schedulerid'])
        # set timings in pilot format
        t=pupdate['pilotID'].split("|")
        logurl=self.logUrl(str(j['arcjobid']))
        if len(t) > 2:
            pupdate['pilotID']=logurl+"|"+t[1]+"|"+t[2]
        else:
            pupdate['pilotID']=logurl+"|Unknown|Unknown"
        pupdate['node']=aj['executionnodes']

        # set start/endtime
        pupdate['startTime'] = time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(aj['starttime']))
        pupdate['endTime'] = time.strftime("%Y-%m-%d %H:%M:%S",time.gmtime(aj['endtime']))

        # save panda status update to pickle
        f=open(self.conf.get(['tmp','dir'])+"/pickle/"+str(j['pandaid'])+".pickle","w")
        pickle.dump(pupdate,f)
        f.close()

        # save xml description to file
        fxml=open(self.conf.get(['tmp','dir'])+"/xml/"+str(j['pandaid'])+".xml","w")
        fxml.write(xml)
        fxml.close()



        lfcinput = {}
        joboutputs=[]
        lguid=""
        outp=True


        # decode files/checksums needed for LFC update
        for file in files:
            try:
                lfn = file.getElementsByTagName("logical")[0].getElementsByTagName("lfn")[0].getAttribute("name")
                guid = str(file.getAttribute('ID'))
                size=""
                adler32=""
                for m in  file.getElementsByTagName ("metadata"):
                    v=m.getAttribute("att_value")
                    if m.getAttribute("att_name") == "fsize":
                        size=v
                    if m.getAttribute("att_name") == "adler32":
                        adler32=v
		    # rewrite surl in xml
		    if m.getAttribute("att_name") == "surl":
		        m.attributes["att_value"] = str(URL(turls[lfn]).CanonicalURL())
                dataset=datasets[lfn]
            except Exception,x:
                self.log.error(x)
                outp = False


            if outp:
                checksum = 'ad:' + adler32
                lfcarchival = 'P'
                #self.log.info("   lfn %s %s %s %s " % (LFCTools.to_native_lfn(dataset,lfn),guid,size,checksum))
                lfcinput[str(guid)] = {'lfn': str(lfn),
                                       'surl': str(URL(turls[lfn]).CanonicalURL()),
                                       'dsn': str(dataset),
                                       'fsize': int(size),
                                       'checksum': str(checksum),
                                       'archival': lfcarchival}
		
		# check srm files
		outok=checkSrm(str(URL(turls[lfn]).CanonicalURL()),str(adler32),False)
		#if outok:
		#  print "OK: ",str(URL(turls[lfn]).CanonicalURL()),str(checksum) 
		#else:
		#  print "NOT OK: ",str(URL(turls[lfn]).CanonicalURL()),str(checksum) 
		if not outok:
		  self.log.error("srmurl missing or wrong checksum for job: %s %s" % (j['pandaid'],lfcinput))
		  return "tobekilled"

            #print lfcinput
            self.log.debug("lfcinput: %s" % lfcinput)

	# rewrite xml in pickle	
        #f=open(outd+"/panda_node_struct.pickle","r")
        f=open(self.conf.get(['tmp','dir'])+"/pickle/"+str(j['pandaid'])+".pickle","r")
        pupdate = pickle.load(f)
        f.close()
	pupdate['xml']=outputxml.toxml()
        # save panda status update to pickle
        f=open(self.conf.get(['tmp','dir'])+"/pickle/"+str(j['pandaid'])+".pickle","w")
        pickle.dump(pupdate,f)
        f.close()


        if not outp:
            return None



        #clean tmpdir
	# moved to cleaner
        ### try:
        ###    shutil.rmtree(outd)
        ###except:
        ###    pass

        # upload the files for failed jobs
        #if j['trfstatus'] == "tofailed":
        #    for g,v in lfcinput.items():
        #        fi=jid+"/"+v['lfn']
        #        fo=turls[v['lfn']]
        #        self.log.debug("Uploading %s to %s" % (fi,fo))
        #        try:
        #            #do with ngcp at the moment
        #            command = "ngcp -t 200  " + fi + " '"+fo+"'"
        #            (ec,et) = commands.getstatusoutput(command)
        #            if ec != 0:
        #                raise Exception("failed to upload: code %d " % ec )
        #        except Exception,x:
        #            self.log.error( x)

        return lfcinput




    def GetOutputTurls(self,pandaid):
        """
        get output turls from the database
        """
        j=self.dbpanda.getJob(pandaid)
        turls = {}
        l=str(j['turls'])
        # strip ' at beginning and end
        l=l[1:][:-1]
        t=l.split("\n")
        for l in t:
            i=l.split(" ")
            turls[i[0]]=i[1]
        return turls
        
            

    def run(self):
        """
        Main loop
        """
        self.log.info("Start")
        try:
            while 1:
                # reparse config file
                self.conf.parse()
                # download failed jobs
		self.checkFailed()
                self.downloadFailed()
                # check job status
                # this crashes due to new cleaner !!!!!!!
                # more frequent checks for panda update
                self.checkJobsPStatus()
                #self.checkJobs('%')
                self.checkJobs('inarc')
                # moved to cleaner
                #self.processCompleted('%','toremove')
                self.processRerunable()
                self.processKilled()
                self.processResubmitted()
                # download finished jobs
        	if self.conf.get(['downtime','srmdown']) == "true":
            	  self.log.info("srm downtime: no downloading")
            	else:
                  i=self.downloadPilotThr('tofinished')
                #print i
                #print "Looping"
                aCTUtils.sleep(10)
                # restart periodicaly for gsiftp crash
                ip=int(self.conf.get(['periodicrestart','downloader']))
                if time.time()-self.starttime > ip and ip != 0 :
                    self.log.info("Downloader exited for periodic restart")
                    return
        except aCTSignal.ExceptInterrupt,x:
            self.log.error( x )
        
            
    def finish(self):
        """
        clean termination handled by signal
        """
        self.log.info("Cleanup")

if __name__ == '__main__':
    ad=aCTDownloader()
    ad.run()
    ad.finish()

