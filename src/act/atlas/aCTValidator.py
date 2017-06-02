from act.atlas.aCTATLASProcess import aCTATLASProcess
from act.common.aCTProxy import aCTProxy
from act.common import aCTUtils
from act.common.aCTSignal import ExceptInterrupt
from act.atlas.aCTPandaJob import aCTPandaJob
import datetime
import signal
import os
import shutil
import time
import tarfile
import arc
from xml.dom import minidom
import re
import json

class aCTValidator(aCTATLASProcess):
    '''
    Validate output files for finished jobs, cleanup output files for failed jobs.
    '''

    def __init__(self):
        aCTATLASProcess.__init__(self)
        
        # Use production role proxy for checking and removing files
        # Get DN from configured proxy file
        cred_type = arc.initializeCredentialsType(arc.initializeCredentialsType.SkipCredentials)
        uc = arc.UserConfig(cred_type)
        uc.ProxyPath(str(self.arcconf.get(['voms', 'proxypath'])))
        cred = arc.Credential(uc)
        dn = cred.GetIdentityName()

        actp = aCTProxy(self.log)
        # Beware hard-coded production role
        proxyfile = actp.path(dn, '/atlas/Role=production')
        if not proxyfile:
            raise Exception('Could not find proxy with production role in proxy table')
        self.log.info('set proxy path to %s' % proxyfile)
            
        self.uc = arc.UserConfig(cred_type)
        self.uc.ProxyPath(str(proxyfile))
        self.uc.UtilsDirPath(arc.UserConfig.ARCUSERDIRECTORY)
        
        # Possible file status
        self.ok = 0
        self.retry = 1
        self.failed = 2
        
    def _extractFromSmallFiles(self, aj, filename):
        jobid=aj['JobID']
        sessionid=jobid[jobid.rfind('/'):]
        localdir = str(self.arcconf.get(['tmp','dir'])) + sessionid
        smallfiles = tarfile.open(os.path.join(localdir,'jobSmallFiles.tgz'))
        return smallfiles.extractfile(filename)
        
    
    def copyFinishedFiles(self, arcjobid, extractmetadata):
        """
        - if extractmetadata: (normal arc jobs, not true pilot jobs) 
           - extract panda_node_struct.pickle from jobSmallFiles.tgz and store it under tmp/pickle
           - extract metadata-surl.xml and update pickle. store xml under tmp/xml
        - copy .job.log file to jobs/date/cluster/jobid
        - copy gmlog dir to jobs/date/cluster/jobid
        """
        
        columns = ['JobID', 'appjobid', 'cluster', 'UsedTotalWallTime', 'EndTime', 'ExecutionNode', 'stdout']
        aj = self.dbarc.getArcJobInfo(arcjobid, columns=columns)
        if not aj.has_key('JobID') or not aj['JobID']:
            self.log.error('No JobID in arcjob %s: %s'%(str(arcjobid), str(aj)))
            return False
        jobid=aj['JobID']
        sessionid=jobid[jobid.rfind('/')+1:]
        date = time.strftime('%Y%m%d')
        if aj['cluster'].find('://') == -1:
            cluster = aj['cluster'].split('/')[0]
        else:
            cluster = arc.URL(str(aj['cluster'])).Host()
        if extractmetadata:
            try:
                pandapickle = self._extractFromSmallFiles(aj, "panda_node_struct.pickle")
            except Exception,x:
                self.log.error("%s: failed to extract pickle for arcjob %s: %s" %(aj['appjobid'], sessionid, x))
                pandapickle = None
            try:
                metadata = self._extractFromSmallFiles(aj, "metadata-surl.xml")
            except Exception,x:
                self.log.error("%s: failed to extract metadata-surl.xml for arcjob %s: %s" %(aj['appjobid'], sessionid, x))
                metadata = None

            # update pickle and dump to tmp/pickle
            if pandapickle:
                try:
                    jobinfo = aCTPandaJob(filehandle=pandapickle)
                except:
                    jobinfo = aCTPandaJob(jobinfo={'jobId': aj['appjobid'], 'state': 'finished'})
            else:
                jobinfo = aCTPandaJob(jobinfo={'jobId': aj['appjobid'], 'state': 'finished'})
            if metadata:
                jobinfo.xml = str(metadata.read())
            jobinfo.computingElement = cluster
            jobinfo.schedulerID = self.conf.get(['panda','schedulerid'])
            if aj['EndTime']:
                jobinfo.startTime = aj['EndTime'] - datetime.timedelta(0, aj['UsedTotalWallTime'])
                jobinfo.endTime = aj['EndTime']
            else:
                self.log.warning('%s: no endtime found' % aj['appjobid'])
            jobinfo.node = aj['ExecutionNode']

            # Add url of logs
            if 'pilotID' in jobinfo.dictionary().keys() and jobinfo.pilotID:
                t = jobinfo.pilotID.split("|")
            else:
                t = ['Unknown'] * 5
            logurl = os.path.join(self.conf.get(["joblog","urlprefix"]), date, cluster, sessionid)
            jobinfo.pilotID = '|'.join([logurl] + t[1:])

            jobinfo.writeToFile(self.arcconf.get(['tmp','dir'])+"/pickle/"+aj['appjobid']+".pickle")
            
        # copy to joblog dir files downloaded for the job: gmlog errors and pilot log
        outd = os.path.join(self.conf.get(['joblog','dir']), date, cluster, sessionid)
        try:
            os.makedirs(outd, 0755)
        except:
            pass

        localdir = os.path.join(str(self.arcconf.get(['tmp','dir'])), sessionid)
        gmlogerrors = os.path.join(localdir,"gmlog","errors")
        
        if not os.path.exists(os.path.join(outd,"arc-ce.log")):
            try:
                shutil.copy(gmlogerrors, os.path.join(outd,"arc-ce.log"))
            except:
                self.log.error("Failed to copy %s" % os.path.join(outd,"arc-ce.log") ) 

        pilotlog = aj['stdout']
        if not pilotlog:
            pilotlogs = [f for f in os.listdir(localdir) if f.find('.log') != -1]
            if pilotlogs:
                pilotlog = pilotlogs[0]
        if pilotlog:
            try:
                shutil.copy(os.path.join(localdir,pilotlog),
                            os.path.join(outd,re.sub('.log.*$', '.out', pilotlog)))
            except Exception, e:
                self.log.error("Failed to copy file %s: %s" % (os.path.join(localdir,pilotlog), str(e)))
                return False
        # set right permissions
        aCTUtils.setFilePermissionsRecursive(outd)
        return True

    def extractOutputFilesFromMetadata(self, arcjobid):
        aj = self.dbarc.getArcJobInfo(arcjobid, columns=["JobID", "appjobid"])
        if not aj or 'JobID' not in aj or not aj['JobID']:
            self.log.error("failed to find arcjobid %s in database" % arcjobid)
            return {}

        jobid=aj['JobID']
        sessionid=jobid[jobid.rfind('/'):]
        try:
            metadata = self._extractFromSmallFiles(aj, "metadata-surl.xml")
        except Exception,x:
            self.log.error("%s: failed to extract metadata file for arcjob %s: %s" %(aj['appjobid'], sessionid, x))
            return {}

        try:
            outputxml = minidom.parse(metadata)
        except Exception, e:
            self.log.error("%s: failed to parse metadata file for arcjob %s: %s" % (aj['appjobid'], sessionid, str(e)))
            return {}

        files = outputxml.getElementsByTagName("POOLFILECATALOG")[0].getElementsByTagName("File")

        surls = {}
        outp = True
        for f in files:
            try:
                #lfn = f.getElementsByTagName("logical")[0].getElementsByTagName("lfn")[0].getAttribute("name")
                #guid = str(file.getAttribute('ID'))
                size=""
                adler32=""
                surl=""
                se=""
                for m in  f.getElementsByTagName ("metadata"):
                    v=m.getAttribute("att_value")
                    if m.getAttribute("att_name") == "fsize":
                        size=v
                    if m.getAttribute("att_name") == "adler32":
                        adler32=v
                    # rewrite surl in xml
                    if m.getAttribute("att_name") == "surl":
                        surl=v
                        se = arc.URL(str(surl)).Host()
            except Exception,x:
                self.log.error('%s: %s' % (aj['appjobid'], x))
                outp = False

            if outp:
                checksum = "adler32:"+ adler32
                if not surls.has_key(se):
                    surls[se]= []
                surls[se] += [{"surl":surl, "fsize":size, "checksum":checksum, "arcjobid":arcjobid}]
        
        return surls
            
    def checkOutputFiles(self, surls):
        '''
        Check if SURLs are working. Returns a dict of arcjobid:file status
        Do bulk arc.DataPoint.Stat() with max 100 files per request. The list
        of surls passed here all belong to the same SE.
        '''
        
        if self.arcconf.get(['downtime', 'srmdown']) == 'True':
            self.log.info("SRM down, will validate later")
            return dict((k['arcjobid'], self.retry) for k in surls)
        
        result = {}
        datapointlist = arc.DataPointList()
        surllist = []
        dummylist = []
        count = 0
        bulklimit = 100
        for surl in surls:
            count += 1
            if not surl['surl']:
                continue
            dp = aCTUtils.DataPoint(str(surl['surl']), self.uc)
            if not dp or not dp.h:
                continue
            datapointlist.append(dp.h)
            dummylist.append(dp) # to not destroy objects
            surllist.append(surl)
            
            if count % bulklimit != 0 and count != len(surls):
                continue
            
            # do bulk call
            (files, status) = dp.h.Stat(datapointlist)
            if not status:
                # If call fails it is generally a server or connection problem
                # and in most cases should be retryable
                if status.Retryable():
                    self.log.warning("Failed to query files on %s, will retry later: %s" % (dp.h.GetURL().Host(), str(status)))
                    result.update(dict((k['arcjobid'], self.retry) for k in surllist))
                else:
                    self.log.error("Failed to query files on %s: %s" % (dp.h.GetURL().Host(), str(status)))
                    result.update(dict((k['arcjobid'], self.failed) for k in surllist))
            
            else:
                # files is a list of FileInfo objects. If file is not found or has
                # another error in the listing FileInfo object will be invalid
                for i in range(len(datapointlist)):
                    if not files[i]:
                        self.log.warning("%s: Failed to find info on %s" % (surllist[i]['arcjobid'], datapointlist[i].GetURL().str()))
                        result[surllist[i]['arcjobid']] = self.failed
                    else:
                        # compare metadata
                        try:
                            self.log.debug("File %s for %s: expected size %d, checksum %s, actual size %d, checksum %s" %
                                           (datapointlist[i].GetURL().str(), surllist[i]['arcjobid'], int(surllist[i]['fsize']),
                                           surllist[i]['checksum'], int(files[i].GetSize()), files[i].GetCheckSum()))
                        except:
                            self.log.warning("Unhandled issue %d",i)
                            result[surllist[i]['arcjobid']] = self.failed
                            continue
                        if int(surllist[i]['fsize']) != int(files[i].GetSize()):
                            self.log.warning("File %s for %s: size on storage (%d) differs from expected size (%d)" %
                                             (datapointlist[i].GetURL().str(), surllist[i]['arcjobid'],
                                              int(files[i].GetSize()), int(surllist[i]['fsize'])))
                            result[surllist[i]['arcjobid']] = self.failed
                            continue
                        if not files[i].CheckCheckSum():
                            self.log.warning("File %s for %s: no checksum information available" %
                                             (datapointlist[i].GetURL().str(), surllist[i]['arcjobid']))
                        elif surllist[i]['checksum'] != files[i].GetCheckSum():
                            self.log.warning("File %s for %s: checksum on storage (%s) differs from expected checksum (%s)" %
                                             (datapointlist[i].GetURL().str(), surllist[i]['arcjobid'], 
                                              files[i].GetCheckSum(), surllist[i]['checksum']))
                            result[surllist[i]['arcjobid']] = self.failed
                            continue
                       
                        self.log.info("File %s validated for %s" % (datapointlist[i].GetURL().str(), surllist[i]['arcjobid']))
                        # don't overwrite previous failed file for this job
                        if surllist[i]['arcjobid'] not in result:
                            result[surllist[i]['arcjobid']] = self.ok
                            
            # Clear lists and go to next round
            datapointlist = arc.DataPointList()
            surllist = []
            dummylist = []
        
        return result
    

    def removeOutputFiles(self, surls):
        '''
        Remove SURLs.
        '''
        result = {}
        
        # As yet there is no bulk remove in ARC
        for surl in surls:
            dp = aCTUtils.DataPoint(str(surl['surl']), self.uc)
            if not dp.h:
                self.log.info("Removed %s for %s" % (surl['surl'], surl['arcjobid']))
                result[surl['arcjobid']] = self.ok
                continue
            status = dp.h.Remove()
            if not status:
                if status.Retryable():
                    self.log.warning("Failed to delete %s for %s, will retry later: %s" %
                                     (surl['surl'], surl['arcjobid'], str(status)))
                    result[surl['arcjobid']] = self.retry
                elif status.GetErrno() == os.errno.ENOENT:
                    self.log.info("File %s for %s does not exist" % (surl['surl'], surl['arcjobid']))
                    result[surl['arcjobid']] = self.ok
                else:
                    self.log.error("Failed to delete %s for %s: %s" % (surl['surl'], surl['arcjobid'], str(status)))
                    result[surl['arcjobid']] = self.failed
            else:
                self.log.info("Removed %s for %s" % (surl['surl'], surl['arcjobid']))
                result[surl['arcjobid']] = self.ok
                
        return result        
    
    
    def downloadSmallFiles(self, jobs):
        '''
        This method is for jobs which should be killed and resubmitted. An attempt
        is made to download jobSmallFiles.tgz but it is fine to fail as the job
        may still be running.
        '''
        
        for job in jobs:
            if not 'JobID' in job or not job['JobID']:
                continue
            jobid = job['JobID']
            sessionid = jobid[jobid.rfind('/'):]
            localdir = str(self.arcconf.get(['tmp','dir'])) + sessionid
            
            try:
                os.makedirs(localdir, 0755)
            except:
                pass
            
            source = aCTUtils.DataPoint(str(jobid + '/jobSmallFiles.tgz'), self.uc)
            dest = aCTUtils.DataPoint(str(localdir + '/jobSmallFiles.tgz'), self.uc)
            dm = arc.DataMover()
            status = dm.Transfer(source.h, dest.h, arc.FileCache(), arc.URLMap())
            if not status:
                self.log.debug('%s: Failed to download %s: %s' % (job['pandaid'], source.h.GetURL().str(), str(status)))


    def cleanDownloadedJob(self, arcjobid):
        '''
        Remove directory to which job was downloaded.
        '''

        job = self.dbarc.getArcJobInfo(arcjobid, columns=['JobID'])
        if job and job['JobID']:
            sessionid = job['JobID'][job['JobID'].rfind('/'):]
            localdir = str(self.arcconf.get(['tmp', 'dir'])) + sessionid
            shutil.rmtree(localdir, ignore_errors=True)


    def validateEvents(self, arcjobid):
        '''
        Take successful event service jobs and modify the eventranges to
        show what was actually processed.
        '''
        
        select = "arcjobid='"+str(arcjobid)+"'"
        esjobs = self.dbpanda.getJobs(select, ['eventranges', 'pandaid'])
        if len(esjobs) != 1:
            # unexpected
            self.log.error("Could not find eventranges for arcjobid %s" % str(arcjobid))
            return
        
        eventranges = esjobs[0]['eventranges']
        if not eventranges:
            # Not ES job
            return

        pandaid = esjobs[0]['pandaid']
        eventranges = json.loads(eventranges)
        
        # Get events processed from metadata-es.xml
        try:
            arcjob = self.dbarc.getArcJobInfo(arcjobid, ['JobID'])
            processedevents = self._extractFromSmallFiles(arcjob, 'metadata-es.xml')
        except Exception, e:
            self.log.error("%s: Failed to extract events processed from metadata-es.xml: %s" % (pandaid, str(e)))
            # Safer to mark all events as failed
            desc = {"eventranges": "[]"}
            self.dbpanda.updateJobLazy(pandaid, desc)
            return
        
        eventsdone = {}
        eventmeta = minidom.parseString(processedevents.read())
        events = eventmeta.getElementsByTagName("POOLFILECATALOG")[0].getElementsByTagName("File")
        for event in events:
            try:
                eventsdone[event.getAttribute('EventRangeID')] = event.getAttribute('Status')
            except:
                eventsdone[event.getAttribute('EventRangeID')] = 'finished'
    
        # Check that events done corresponds to events asked
        for event in eventsdone.keys():
            if event not in [e['eventRangeID'] for e in eventranges]:
                self.log.warning("%s: Event ID %s was processed but was not in eventranges!" % (pandaid, event))
                del eventsdone[event]

        # Update DB with done events
        self.log.info("%s: %d events successful, %d failed out of %d" % (pandaid, len([k for k,v in eventsdone.items() if v == 'finished']), len([k for k,v in eventsdone.items() if v == 'failed']), len(eventranges)))                
        finaleventranges = []
        for e in eventranges:
            if e['eventRangeID'] in eventsdone:
                e['status'] = eventsdone[e['eventRangeID']]
                finaleventranges.append(e)

        desc = {"eventranges": json.dumps(finaleventranges)}
        self.dbpanda.updateJobLazy(pandaid, desc)
        

    def validateFinishedJobs(self):
        '''
        Check for jobs with actpandastatus tovalidate and pandastatus running
        Check if the output files in metadata.xml are valid.
        If yes, move to actpandastatus to finished, if not, move pandastatus
        and actpandastatus to failed. 
        '''
        
        # get all jobs with pandastatus running and actpandastatus tovalidate
        select = "(pandastatus='transferring' and actpandastatus='tovalidate') limit 100000"
        columns = ["arcjobid", "pandaid", "sendhb"]
        jobstoupdate=self.dbpanda.getJobs(select, columns=columns)

        if len(jobstoupdate)==0:
            # nothing to do
            return
        
        # Skip validation for the true pilot jobs, just copy logs, set to done and clean arc job
        for job in jobstoupdate[:]:
            if not job['sendhb']:
                self.log.info('%s: Skip validation' % job['pandaid'])
                if not self.copyFinishedFiles(job["arcjobid"], False):
                    self.log.warning("%s: Failed to copy log files" % job['pandaid'])
                select = "arcjobid='"+str(job["arcjobid"])+"'"
                desc = {"pandastatus": None, "actpandastatus": "done"}
                self.dbpanda.updateJobs(select, desc)
                # set arcjobs state toclean
                desc = {"arcstate":"toclean", "tarcstate": self.dbarc.getTimeStamp()}
                self.dbarc.updateArcJobLazy(job['arcjobid'], desc)
                self.cleanDownloadedJob(job['arcjobid'])
                jobstoupdate.remove(job)

        # pull out output file info from metadata.xml into dict, order by SE

        surls = {}
        for job in jobstoupdate:
            jobsurls = self.extractOutputFilesFromMetadata(job["arcjobid"])
            if not jobsurls:
                # Problem extracting files, fail the job
                self.log.error("%s: Cannot validate output of arc job %s" % (job['pandaid'], job["arcjobid"]))
                select = "arcjobid='"+str(job["arcjobid"])+"'"
                desc = {"actpandastatus": "failed", "pandastatus": "failed"}
                self.dbpanda.updateJobs(select, desc)
            else:
                for se in jobsurls:
                    try:
                        surls[se].extend(jobsurls[se])
                    except:
                        surls[se] = jobsurls[se]
        if not surls:
            # nothing to validate
            return

        # check if surls valid, update pandastatus accordingly
        for se in surls:
            checkedsurls = self.checkOutputFiles(surls[se])
            for id, result in checkedsurls.items():
                if result == self.ok:
                    # For ES jobs, modify eventranges to what was produced
                    self.validateEvents(id)

                    select = "arcjobid='"+str(id)+"'"
                    desc = {"pandastatus": "finished", "actpandastatus": "finished"}
                    self.dbpanda.updateJobsLazy(select, desc) 
                    if not self.copyFinishedFiles(id, True):
                        # id was gone already
                        continue
                    # set arcjobs state toclean
                    desc = {"arcstate":"toclean", "tarcstate": self.dbarc.getTimeStamp()}
                    self.dbarc.updateArcJobLazy(id, desc)
                    self.cleanDownloadedJob(id)
                elif result == self.failed:
                    select = "arcjobid='"+str(id)+"'"
                    # output file failed, set to toresubmit to clean up output and resubmit
                    desc = {"pandastatus": "starting", "actpandastatus": "toresubmit"}
                    self.dbpanda.updateJobsLazy(select, desc)
                else:
                    # Retry next time
                    pass
                
        self.dbpanda.Commit()
        self.dbarc.Commit()
                
                
    def cleanFailedJobs(self):
        '''
        Check for jobs with actpandastatus toclean and pandastatus transferring.
        Delete the output files in metadata.xml.
        Move actpandastatus to failed. 
        '''
        # get all jobs with pandastatus transferring and actpandastatus toclean
        select = "(pandastatus='transferring' and actpandastatus='toclean') limit 100000"
        columns = ["arcjobid", "pandaid", "sendhb"]
        jobstoupdate=self.dbpanda.getJobs(select, columns=columns)

        if len(jobstoupdate)==0:
            # nothing to do
            return

        # pull out output file info from metadata.xml into dict, order by SE

        surls = {}
        cleandesc = {"arcstate":"toclean", "tarcstate": self.dbarc.getTimeStamp()}
        
        # For truepilot jobs, don't try to clean outputs (too dangerous), just clean arc job
        for job in jobstoupdate[:]:
            if not job['sendhb']:
                self.log.info("%s: Skip cleanup of output files" % job['pandaid'])
                select = "arcjobid='"+str(job["arcjobid"])+"'"
                desc = {"actpandastatus": "failed", "pandastatus": "failed"}
                self.dbpanda.updateJobs(select, desc)
                # set arcjobs state toclean
                self.dbarc.updateArcJob(job["arcjobid"], cleandesc)
                self.cleanDownloadedJob(job["arcjobid"])
                jobstoupdate.remove(job)
        
        for job in jobstoupdate:
            jobsurls = self.extractOutputFilesFromMetadata(job["arcjobid"])
            if not jobsurls:
                # Problem extracting files, just continue to failed
                self.log.error("%s: Cannot remove output of arc job %s" % (job['pandaid'], job["arcjobid"]))
                select = "arcjobid='"+str(job["arcjobid"])+"'"
                desc = {"actpandastatus": "failed", "pandastatus": "failed"}
                self.dbpanda.updateJobs(select, desc)
                # set arcjobs state toclean
                self.dbarc.updateArcJob(job["arcjobid"], cleandesc)
                self.cleanDownloadedJob(job["arcjobid"])
            else:
                surls.update(jobsurls)

        if not surls:
            # nothing to clean
            return
        
        for se in surls:
            removedsurls = self.removeOutputFiles(surls[se])
            for id, result in removedsurls.items():
                # If failed, not much we can do except continue
                if result == self.ok or result == self.failed:
                    select = "arcjobid='"+str(id)+"'"
                    desc = {"actpandastatus": "failed", "pandastatus": "failed"}
                    self.dbpanda.updateJobsLazy(select, desc)
                    # set arcjobs state toclean
                    self.dbarc.updateArcJobLazy(id, cleandesc)
                    self.cleanDownloadedJob(id)
                else:
                    # Retry next time
                    pass
                
        self.dbpanda.Commit()
        self.dbarc.Commit()
        

    def cleanResubmittingJobs(self):
        '''
        Check for jobs with actpandastatus toresubmit and pandastatus starting.
        Delete the output files in metadata.xml.
        Move actpandastatus to starting. 
        '''
        
        # First check for resubmitting jobs with no arcjob id defined
        select = "(actpandastatus='toresubmit' and arcjobid=NULL) limit 100000"
        columns = ["pandaid", "id"]
        
        jobstoupdate=self.dbpanda.getJobs(select, columns=columns)

        for job in jobstoupdate:
            self.log.info('%s: resubmitting' % job['pandaid'])
            select = "id="+str(job['id'])
            desc = {"actpandastatus": "starting", "arcjobid": None}
            self.dbpanda.updateJobs(select, desc)

        # Get all other jobs with pandastatus starting and actpandastatus toresubmit
        # 2 possibilities for these jobs:
        # - job failed and aCT decided to resubmit: clean output files
        # - job was manually set toresubmit by aCT admin: set arc job tocancel,
        #   attempt to get jobSmallFiles and clean but don't fail if not possible.
        #   In this case don't wait for cancellation to finish as A-REX may be
        #   broken. There is always the possibility of a race condition where
        #   jobSmallFiles is produced and uploaded between checking for it and
        #   cancelling the job.
        select = "actpandastatus='toresubmit' and arcjobs.id=pandajobs.arcjobid limit 100"
        columns = ["pandajobs.arcjobid", "pandajobs.pandaid", "arcjobs.JobID", "arcjobs.arcstate", "arcjobs.restartstate"]
        jobstoupdate=self.dbarc.getArcJobsInfo(select, columns=columns, tables='arcjobs, pandajobs')

        if len(jobstoupdate)==0:
            # nothing to do
            return

        killedbymanual = [j for j in jobstoupdate if j['arcstate'] != 'donefailed' and j['arcstate'] != 'done' and j['arcstate'] != 'lost' and j['arcstate'] != 'cancelled']
        
        # TODO: make data transfer separate from main validator thread
        self.downloadSmallFiles(killedbymanual)
        # Cancel the jobs manually set toresubmit (TODO: when the jobs eventually go 
        # to cancelled the arc id will not be in pandajobs any more so they will
        # never be cleaned up)
        for job in killedbymanual:
            self.log.info('%s: manually asked to resubmit, cancelling arc job %s' %
                          (job['pandaid'], job['JobID']))
            desc = {'arcstate': 'tocancel', 'tarcstate': self.dbarc.getTimeStamp()}
            self.dbarc.updateArcJobLazy(job['arcjobid'], desc)
        self.dbpanda.Commit()
            
        # pull out output file info from metadata.xml into dict, order by SE
        surls = {}
        for job in jobstoupdate:
            jobsurls = self.extractOutputFilesFromMetadata(job["arcjobid"])
            if not jobsurls:
                if job in killedbymanual or (job['restartstate'] != 'Finishing' and job['arcstate'] != 'done'):
                    # If job failed before finishing there is probably no
                    # jobSmallFiles, but also nothing to clean. Just let it be
                    # resubmitted and clean arc job
                    self.cleanDownloadedJob(job['arcjobid'])
                    select = "arcjobid="+str(job['arcjobid'])
                    desc = {"actpandastatus": "starting", "arcjobid": None}
                    self.dbpanda.updateJobs(select, desc)
                else:
                    # Can't clean outputs so mark as failed (see more detail below)
                    self.log.error("%s: Cannot remove output of arc job %s" % (job['pandaid'], job["arcjobid"]))
                    select = "arcjobid='"+str(job["arcjobid"])+"'"
                    desc = {"actpandastatus": "toclean", "pandastatus": "transferring"}
                    self.dbpanda.updateJobs(select, desc)
            else:
                surls.update(jobsurls)

        if not surls:
            # nothing to clean
            return
        
        for se in surls:
            removedsurls = self.removeOutputFiles(surls[se])
            for id, result in removedsurls.items():
                # if manually killed the cleaning is allowed to fail
                if id in [j['arcjobid'] for j in killedbymanual]:
                    select = "arcjobid='"+str(id)+"'"
                    # Setting arcjobid to NULL lets Panda2Arc pick up the job for resubmission
                    desc = {"actpandastatus": "starting", "arcjobid": None}
                    self.dbpanda.updateJobsLazy(select, desc)
                    continue
                
                if result == self.ok:
                    self.cleanDownloadedJob(id)
                    select = "arcjobid='"+str(id)+"'"
                    # Setting arcjobid to NULL lets Panda2Arc pick up the job for resubmission
                    desc = {"actpandastatus": "starting", "arcjobid": None}
                    self.dbpanda.updateJobsLazy(select, desc)
                elif result == self.failed:
                    # If we couldn't clean outputs the next try of the job will
                    # also fail. Better to return to panda for an increased
                    # attempt no. Setting to toclean and pandastatus=transferring
                    # means it will be processed by cleanFailedJobs() so don't 
                    # clean the arc job here
                    select = "arcjobid='"+str(id)+"'"
                    desc = {"actpandastatus": "toclean", "pandastatus": "transferring"}
                    self.dbpanda.updateJobsLazy(select, desc)
                else:
                    # Retry next time
                    pass

        self.dbpanda.Commit()
        self.dbarc.Commit()


    def process(self):
        self.logger.arclogfile.setReopen(True)
        self.logger.arclogfile.setReopen(False)
        self.validateFinishedJobs()
        self.cleanFailedJobs()
        self.cleanResubmittingJobs()

        # Validator suffers from memory leaks in arc bindings, so exit once per day
        if time.time() - self.starttime > 60*60*24:
            self.log.info("%s exited for periodic restart", self.name)
            raise ExceptInterrupt(signal.SIGTERM)


if __name__ == '__main__':

    am=aCTValidator()
    am.run()
    am.finish()
