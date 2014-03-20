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
        jobid=aj['JobID']
        sessionid=jobid[jobid.rfind('/'):]
        date = time.strftime('%Y%m%d')
        select="arcjobid='"+str(arcjobid)+"'"
        j = self.dbpanda.getJobs(select, ["pandaid"])[0]
        try:
            pandapickle = self._extractFromSmallFiles(aj, "panda_node_struct.pickle")
            metadata = self._extractFromSmallFiles(aj, "metadata-surl.xml")
        except Exception,x:
            self.log.error("failed to extract smallFiles for arcjob %s: %s" %(sessionid, x))

        # update pickle and dump to tmp/pickle
        cluster=aj['cluster'].split('/')[0]
        pupdate = pickle.load(pandapickle)
        pupdate['xml'] = str(metadata.read())
        pupdate['siteName']='ARC'
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
        shutil.copytree(gmlogdir, os.path.join(outd,"gmlog"))
        pilotlog = [f for f in os.listdir(localdir) if f.endswith('.job.log')][0]
        shutil.copy(os.path.join(localdir,pilotlog), 
                    os.path.join(outd,pilotlog.replace('.job.log','.out')))
        # set right permissions
        aCTUtils.setFilePermissionsRecursive(outd)
        # todo: unlink localdir

    def extractOutputFilesFromMetadata(self, arcjobid):
        aj = self.dbarc.getArcJobInfo(arcjobid, columns=["JobID"])
        if not aj or 'JobID' not in aj:
            self.log.error("failed to find arcjobid %s in database" % arcjobid)
            return {}

        jobid=aj['JobID']
        sessionid=jobid[jobid.rfind('/'):]
        try:
            metadata = self._extractFromSmallFiles(aj, "metadata-surl.xml")
        except Exception,x:
            self.log.error("failed to extract metadata file for arcjob %s: %s" %(sessionid, x))
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
                self.log.error(x)
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
                        self.log.warning("Failed to find info on %s" % datapointlist[i].GetURL().str())
                        result[surllist[i]['arcjobid']] = self.failed
                    else:
                        # compare metadata
                        self.log.debug("File %s: expected size %d, checksum %s, actual size %d, checksum %s" %
                                       (datapointlist[i].GetURL().str(), int(surllist[i]['fsize']),
                                        surllist[i]['checksum'], int(files[i].GetSize()), files[i].GetCheckSum()))
                        if int(surllist[i]['fsize']) != int(files[i].GetSize()):
                            self.log.warning("File %s: size on storage (%d) differs from expected size (%d)" %
                                             (datapointlist[i].GetURL().str(), int(files[i].GetSize()), int(surllist[i]['fsize'])))
                            result[surllist[i]['arcjobid']] = self.failed
                        elif surllist[i]['checksum'] != files[i].GetCheckSum():
                            self.log.warning("File %s: checksum on storage (%s) differs from expected checksum (%s)" %
                                             (datapointlist[i].GetURL().str(), files[i].GetCheckSum(), surllist[i]['checksum']))
                            result[surllist[i]['arcjobid']] = self.failed
                        else:
                            self.log.info("File %s validated" % datapointlist[i].GetURL().str())
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
                    self.log.warning("Failed to delete %s, will retry later: %s" % (surl['surl'], str(status)))
                    result[surl['arcjobid']] = self.retry
                elif status.GetErrno() == os.errno.ENOENT:
                    self.log.info("File %s does not exist" % surl['surl'])
                    result[surl['arcjobid']] = self.ok
                else:
                    self.log.error("Failed to delete %s: %s" % (surl['surl'], str(status)))
                    result[surl['arcjobid']] = self.failed
            else:
                self.log.info("Removed %s" % surl['surl'])
                result[surl['arcjobid']] = self.ok
                
        return result                  

    def validateFinishedJobs(self):
        '''
        Check for jobs with actpandastatus tovalidate and pandastatus running
        Check if the output files in metadata.xml are valid.
        If yes, move to actpandastatus to finished, if not, move pandastatus
        and actpandastatus to failed. 
        '''
        
        # get all jobs with pandastatus running and actpandastatus tovalidate
        select = "(pandastatus='running' and actpandastatus='tovalidate') limit 100000"
        columns = ["arcjobid"]
        jobstoupdate=self.dbpanda.getJobs(select, columns=columns)

        if len(jobstoupdate)==0:
            # nothing to do
            return
        
        # pull out output file info from metadata.xml into dict, order by SE

        surls = {}
        for job in jobstoupdate:
            surls.update(self.extractOutputFilesFromMetadata(job["arcjobid"]))

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
                    self.copyFinishedFiles(id)
                    # set arcjobs state toclean
                    desc = {"arcstate":"toclean", "tarcstate": self.dbarc.getTimeStamp()}
                    self.dbarc.updateArcJobLazy(id, desc)
                elif result == self.failed:
                    select = "arcjobid='"+str(id)+"'"
                    # output file failed, set to toresubmit to clean up output and resubmit
                    desc = {"actpandastatus": "toresubmit", "pandastatus": "starting"}
                    self.dbpanda.updateJobsLazy(select, desc)
                else:
                    # Retry next time
                    pass
                
        self.dbpanda.Commit()
        self.dbarc.Commit()
                
                
    def cleanFailedJobs(self):
        '''
        Check for jobs with actpandastatus tovalidate and pandastatus failed
        Delete the output files in metadata.xml.
        Move actpandastatus to failed. 
        '''
        # get all jobs with pandastatus running and actpandastatus tovalidate
        select = "(pandastatus='failed' and actpandastatus='tovalidate') limit 100000"
        columns = ["arcjobid"]
        jobstoupdate=self.dbpanda.getJobs(select, columns=columns)

        if len(jobstoupdate)==0:
            # nothing to do
            return

        # pull out output file info from metadata.xml into dict, order by SE

        surls = {}
        for job in jobstoupdate:
            surls.update(self.extractOutputFilesFromMetadata(job["arcjobid"]))

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
        Check for jobs with actpandastatus toresubmit and pandastatus starting
        Delete the output files in metadata.xml.
        Move actpandastatus to starting. 
        '''
        # get all jobs with pandastatus running and actpandastatus toresubmit
        select = "(pandastatus='starting' and actpandastatus='toresubmit') limit 100000"
        columns = ["arcjobid"]
        jobstoupdate=self.dbpanda.getJobs(select, columns=columns)

        if len(jobstoupdate)==0:
            # nothing to do
            return

        # pull out output file info from metadata.xml into dict, order by SE

        surls = {}
        for job in jobstoupdate:
            surls.update(self.extractOutputFilesFromMetadata(job["arcjobid"]))

        if not surls:
            # nothing to clean
            return
        
        for se in surls:
            removedsurls = self.removeOutputFiles(surls[se])
            for id, result in removedsurls.items():
                if result == self.ok:
                    select = "arcjobid='"+str(id)+"'"
                    # Setting arcjobid to NULL lets Panda2Arc pick up the job for resubmission
                    desc = {"actpandastatus": "starting", "pandastatus": "starting", "arcjobid": None}
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
                    desc = {"actpandastatus": "tovalidate", "pandastatus": "failed"}
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
