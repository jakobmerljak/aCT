from aCTATLASProcess import aCTATLASProcess
from aCTProxy import aCTProxy
import aCTUtils
import os
import shutil
import time
import tarfile
import arc
from xml.dom import minidom
import pickle
import re

class aCTValidator(aCTATLASProcess):
    '''
    Validate output files for finished jobs, cleanup output files for failed jobs.
    '''

    def __init__(self):
        aCTATLASProcess.__init__(self)
        
        # Use production role proxy for checking and removing files
        # Get DN from configured proxy file
        uc = arc.UserConfig()
        uc.ProxyPath(str(self.arcconf.get(['voms', 'proxypath'])))
        cred = arc.Credential(uc)
        dn = cred.GetIdentityName()

        actp = aCTProxy(self.log)
        # Beware hard-coded production role
        proxyfile = actp.path(dn, '/atlas/Role=production')
        if not proxyfile:
            raise Exception('Could not find proxy with production role in proxy table')
            
        self.uc = arc.UserConfig()
        self.uc.ProxyPath(str(proxyfile))
        
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
        
    
    def copyFinishedFiles(self, arcjobid):
        """
        - extract panda_node_struct.pickle from jobSmallFiles.tgz and store it under tmp/pickle
        - extract metadata-surl.xml and update pickle. store xml under tmp/xml
        - copy .job.log file to jobs/date/cluster/jobid
        - copy gmlog dir to jobs/date/cluster/jobid
        """
        aj = self.dbarc.getArcJobInfo(arcjobid)
        if not aj.has_key('JobID'):
            self.log.error('No JobID in arcjob %s: %s'%(str(arcjobid), str(aj)))
            return False
        jobid=aj['JobID']
        sessionid=jobid[jobid.rfind('/'):]
        date = time.strftime('%Y%m%d')
        select="arcjobid='"+str(arcjobid)+"'"
        j = self.dbpanda.getJobs(select, ["pandaid", "sitename"])[0]
        try:
            pandapickle = self._extractFromSmallFiles(aj, "panda_node_struct.pickle")
            metadata = self._extractFromSmallFiles(aj, "metadata-surl.xml")
        except Exception,x:
            self.log.error("%s: failed to extract smallFiles for arcjob %s: %s" %(aj['appjobid'], sessionid, x))

        # update pickle and dump to tmp/pickle
        cluster=aj['cluster'].split('/')[0]
        pupdate = pickle.load(pandapickle)
        pupdate['xml'] = str(metadata.read())
        pupdate['siteName']=j["sitename"]
        pupdate['computingElement']=cluster
        pupdate['schedulerID']=self.conf.get(['panda','schedulerid'])
        pupdate['startTime'] = aj['StartTime']
        pupdate['endTime'] = aj['EndTime']
        t=pupdate['pilotID'].split("|")
        logurl=self.conf.get(["joblog","urlprefix"])+"/"+date+"/"+cluster+sessionid
        if len(t) > 4:
            pupdate['pilotID']=logurl+"|"+t[1]+"|"+t[2]+"|"+t[3]+"|"+t[4]
        else:
            pupdate['pilotID']=logurl+"|Unknown|Unknown|Unknown|Unknown"
        pupdate['node']=aj['ExecutionNode']

        try:
            os.mkdir(self.conf.get(['tmp','dir'])+"/pickle")
        except:
            pass
        f=open(self.conf.get(['tmp','dir'])+"/pickle/"+str(j['pandaid'])+".pickle","w")
        pickle.dump(pupdate, f)
        f.close()
        try:
            os.mkdir(self.conf.get(['tmp','dir'])+"/xml")
        except:
            pass
        f=open(self.conf.get(['tmp','dir'])+"/xml/"+str(j['pandaid'])+".xml","w")
        f.write(pupdate['xml'])
        f.close()

        # copy files to joblog dir
        try:
            os.mkdir(self.conf.get(['joblog','dir']) + "/" + date)
        except:
            pass
        try:
            os.mkdir(self.conf.get(['joblog','dir']) + "/" + date + "/" + cluster )
        except:
            pass
        outd = self.conf.get(['joblog','dir']) + "/" + date + "/" + cluster + sessionid
        try:
            os.mkdir(outd)
        except:
            pass
        # copy from tmp to outd.
        localdir = str(self.arcconf.get(['tmp','dir'])) + sessionid
        gmlogdir = os.path.join(localdir,"gmlog")
        
        if not os.path.exists(os.path.join(outd,"gmlog")):
            shutil.copytree(gmlogdir, os.path.join(outd,"gmlog"))

        pilotlog = [f for f in os.listdir(localdir) if f.find('.job.log') != -1]
        if pilotlog:
            shutil.copy(os.path.join(localdir,pilotlog[0]), 
                        os.path.join(outd,re.sub('.job.log.*$', '.out', pilotlog[0])))
        # set right permissions
        aCTUtils.setFilePermissionsRecursive(outd)
        # todo: unlink localdir
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

        outputxml = minidom.parse(metadata)
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
        count = 0
        bulklimit = 100
        for surl in surls:
            count += 1
            dp = arc.datapoint_from_url(str(surl['surl']), self.uc)
            datapointlist.append(dp)
            surllist.append(surl)
            
            if count % bulklimit != 0 and count != len(surls):
                continue
            
            # do bulk call
            (files, status) = dp.Stat(datapointlist)
            if not status:
                # If call fails it is generally a server or connection problem
                # and in most cases should be retryable
                if status.Retryable():
                    self.log.warning("Failed to query files on %s, will retry later: %s" % (dp.GetURL().Host(), str(status)))
                    result.update(dict((k['arcjobid'], self.retry) for k in surllist))
                else:
                    self.log.error("Failed to query files on %s: %s" % (dp.GetURL().Host(), str(status)))
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
                        self.log.debug("File %s for %s: expected size %d, checksum %s, actual size %d, checksum %s" %
                                       (surllist[i]['arcjobid'], datapointlist[i].GetURL().str(), int(surllist[i]['fsize']),
                                        surllist[i]['checksum'], int(files[i].GetSize()), files[i].GetCheckSum()))
                        if int(surllist[i]['fsize']) != int(files[i].GetSize()):
                            self.log.warning("File %s for %s: size on storage (%d) differs from expected size (%d)" %
                                             (surllist[i]['arcjobid'], datapointlist[i].GetURL().str(),
                                              int(files[i].GetSize()), int(surllist[i]['fsize'])))
                            result[surllist[i]['arcjobid']] = self.failed
                        elif surllist[i]['checksum'] != files[i].GetCheckSum():
                            self.log.warning("File %sf for %s: checksum on storage (%s) differs from expected checksum (%s)" %
                                             (surllist[i]['arcjobid'], datapointlist[i].GetURL().str(),
                                              files[i].GetCheckSum(), surllist[i]['checksum']))
                            result[surllist[i]['arcjobid']] = self.failed
                        else:
                            self.log.info("File %s validated for %s" % (datapointlist[i].GetURL().str(), surllist[i]['arcjobid']))
                            # don't overwrite previous failed file for this job
                            if surllist[i]['arcjobid'] not in result:
                                result[surllist[i]['arcjobid']] = self.ok
                            
            # Clear lists and go to next round
            datapointlist = arc.DataPointList()
            surllist = []
        
        return result
    

    def removeOutputFiles(self, surls):
        '''
        Remove SURLs.
        '''
        result = {}
        
        # As yet there is no bulk remove in ARC
        for surl in surls:
            dp = arc.datapoint_from_url(str(surl['surl']), self.uc)
            status = dp.Remove()
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
                os.makedirs(localdir)
            except:
                pass
            
            source = arc.datapoint_from_url(str(jobid + '/jobSmallFiles.tgz'))
            dest = arc.datapoint_from_url(str(localdir + '/jobSmallFiles.tgz'))
            dm = arc.DataMover()
            status = dm.Transfer(source, dest, arc.FileCache(), arc.URLMap())
            if not status:
                self.log.debug('%s: Failed to download %s: %s' % (job['pandaid'], source.GetURL().str(), str(status)))
        

    def validateFinishedJobs(self):
        '''
        Check for jobs with actpandastatus tovalidate and pandastatus running
        Check if the output files in metadata.xml are valid.
        If yes, move to actpandastatus to finished, if not, move pandastatus
        and actpandastatus to failed. 
        '''
        
        # get all jobs with pandastatus running and actpandastatus tovalidate
        select = "(pandastatus='transferring' and actpandastatus='tovalidate') limit 100000"
        columns = ["arcjobid", "pandaid"]
        jobstoupdate=self.dbpanda.getJobs(select, columns=columns)

        if len(jobstoupdate)==0:
            # nothing to do
            return
        
        # pull out output file info from metadata.xml into dict, order by SE

        surls = {}
        for job in jobstoupdate:
            jobsurls = self.extractOutputFilesFromMetadata(job["arcjobid"])
            if not jobsurls:
                # Problem extracting files, resubmit the job
                self.log.error("%s: Cannot validate output of arc job %s, will resubmit" % (job['pandaid'], job["arcjobid"]))
                select = "arcjobid='"+str(job["arcjobid"])+"'"
                desc = {"actpandastatus": "toresubmit", "pandastatus": "starting"}
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
                    select = "arcjobid='"+str(id)+"'"
                    desc = {"pandastatus": "finished", "actpandastatus": "finished"}
                    self.dbpanda.updateJobsLazy(select, desc) 
                    if not self.copyFinishedFiles(id):
                        # id was gone already
                        continue
                    # set arcjobs state toclean
                    desc = {"arcstate":"toclean", "tarcstate": self.dbarc.getTimeStamp()}
                    self.dbarc.updateArcJobLazy(id, desc)
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
        # get all jobs with pandastatus running and actpandastatus tovalidate
        select = "(pandastatus='transferring' and actpandastatus='toclean') limit 100000"
        columns = ["arcjobid", "pandaid"]
        jobstoupdate=self.dbpanda.getJobs(select, columns=columns)

        if len(jobstoupdate)==0:
            # nothing to do
            return

        # pull out output file info from metadata.xml into dict, order by SE

        surls = {}
        for job in jobstoupdate:
            jobsurls = self.extractOutputFilesFromMetadata(job["arcjobid"])
            if not jobsurls:
                # Problem extracting files, just continue to failed
                self.log.error("%s: Cannot remove output of arc job %s" % (job['pandaid'], job["arcjobid"]))
                select = "arcjobid='"+str(job["arcjobid"])+"'"
                desc = {"actpandastatus": "failed", "pandastatus": "failed"}
                self.dbpanda.updateJobs(select, desc)
                # set arcjobs state toclean
                desc = {"arcstate":"toclean", "tarcstate": self.dbarc.getTimeStamp()}
                self.dbarc.updateArcJob(job["arcjobid"], desc)
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
                    desc = {"arcstate":"toclean", "tarcstate": self.dbarc.getTimeStamp()}
                    self.dbarc.updateArcJobLazy(id, desc)
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
        select = "actpandastatus='toresubmit' and arcjobs.id=pandajobs.arcjobid limit 100000"
        columns = ["pandajobs.arcjobid", "pandajobs.pandaid", "arcjobs.JobID", "arcjobs.arcstate"]
        jobstoupdate=self.dbarc.getArcJobsInfo(select, columns=columns, tables='arcjobs, pandajobs')

        if len(jobstoupdate)==0:
            # nothing to do
            return

        killedbymanual = [j for j in jobstoupdate if j['arcstate'] != 'donefailed']
        
        self.downloadSmallFiles(killedbymanual)
        # Cancel the jobs manually set toresubmit (when the jobs eventually go 
        # to cancelled they will be cleaned by ATLASStatus but pandastatus will
        # not be changed because the arc id will not be in pandajobs any more)
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
                if job in killedbymanual:
                    # Nothing to clean - just let it be resubmitted
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
                    select = "arcjobid='"+str(id)+"'"
                    # Setting arcjobid to NULL lets Panda2Arc pick up the job for resubmission
                    desc = {"actpandastatus": "starting", "arcjobid": None}
                    self.dbpanda.updateJobsLazy(select, desc)
                    # set arcjobs state toclean
                    desc = {"arcstate":"toclean", "tarcstate": self.dbarc.getTimeStamp()}
                    self.dbarc.updateArcJobLazy(id, desc)
                elif result == self.failed:
                    # If we couldn't clean outputs the next try of the job will
                    # also fail. Better to return to panda for an increased
                    # attempt no. Setting to tovalidate and pandastatus=failed
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
        self.validateFinishedJobs()
        self.cleanFailedJobs()
        self.cleanResubmittingJobs()

if __name__ == '__main__':

    am=aCTValidator()
    am.run()
    am.finish()
