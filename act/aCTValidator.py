from aCTATLASProcess import aCTATLASProcess
from aCTProxy import aCTProxy
import os
import tarfile
import re
import arc
from xml.dom import minidom

class aCTValidator(aCTATLASProcess):
    '''
    Validate output files for finished jobs, cleanup output files for failed jobs.
    '''

    def __init__(self):
        aCTATLASProcess.__init__(self)
        
        # Use production role proxy for checking and removing files
        # Get DN from configured proxy file
        uc = arc.UserConfig()
        uc.ProxyPath(str(self.conf.get(['voms', 'proxypath'])))
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
        

    def extractOutputFilesFromMetadata(self, arcjobid):
        self.log.debug(arcjobid)
        aj = self.dbarc.getArcJobInfo(arcjobid, columns=['JobID'])
        jobid=aj['JobID']
        sessionid=jobid[jobid.rfind('/'):]
        localdir = str(self.arcconf.get(['tmp','dir'])) + sessionid
        try:
            smallfiles = tarfile.open(os.path.join(localdir,'jobSmallFiles.tgz'))
            metadata = smallfiles.extractfile("metadata-surl.xml")
        except Exception,x:
            self.log.error("failed to extract metadata file for arcjob %s: %s" %(sessionid, x))
            return []
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
                        se = re.search("srm://(.*?)/.*", surl)
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
        result = {}
        datapointlist = arc.DataPointList()
        surllist = []
        count = 0
        bulklimit = 100
        for surl in surls:
            count += 1
            dp = arc.datapoint_from_url(surl['surl'], self.uc)
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
                                       (datapointlist[i].GetURL().str(), surllist[i]['fsize'],
                                        surllist[i]['checksum'], files[i].GetSize(), files[i].GetCheckSum()))
                        if surllist[i]['fsize'] != files[i].GetSize():
                            self.log.warning("File %s: size on storage (%d) differs from expected size (%d)" %
                                             (datapointlist[i].GetURL().str(), files[i].GetSize(), surllist[i]['fsize']))
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
        pass

    def validateFinishedJobs(self):
        '''
        Check for jobs with actpandastate tovalidate and pandastatus running
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

        # check if surls valid, update pandastate accordingly
        for se in surls:
            checkedsurls = self.checkOutputFiles(surls[se])
            for id, result in checkedsurls.items():
                if result == self.ok:
                    select = "arcjobid='"+str(id)+"'"
                    desc = {}
                    desc["pandastatus"] = "finished"
                    desc["actpandastatus"] = "done"
                    self.dbpanda.updateJobsLazy(select, desc)
                    # set arcjobs state toclean
                    desc = {"arcstate":"toclean", "tarcstate": self.dbarc.getTimeStamp()}
                    self.dbarc.updateArcJobLazy(id, desc)
                elif result == self.failed:
                    # todo: output file failed, should clean up output
                    select = "arcjobid='"+str(id)+"'"
                    desc = {}
                    desc["actpandastatus"] = "tobekilled"
                    self.dbpanda.updateJobsLazy(select, desc)
                else:
                    # Retry next time
                    pass
                
        self.dbpanda.Commit()
        self.dbarc.Commit()            
                
                
    def cleanFailedJobs(self):
        '''
        Check for jobs with actpandastate tovalidate and pandastatus failed
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
            self.removeOutputFiles(surls[se])
        
        for job in jobstoupdate:
            select = "arcjobid='"+str(job["arcjobid"])+"'"
            desc = {}
            desc["actpandastatus"] = "done"
            self.dbpanda.updateJobsLazy(select, desc)
            desc = {}
            # set arcjobs state toclean
            desc = {"arcstate":"toclean", "tarcstate": self.dbarc.getTimeStamp()}
            self.dbarc.updateArcJobLazy(job["arcjobid"], desc)
        self.dbpanda.Commit()
        self.dbarc.Commit()
        

    def cleanResubmittingJobs(self):
        '''
        Check for jobs with actpandastate toresubmit and pandastatus starting
        Delete the output files in metadata.xml.
        Move actpandastatus to starting. 
        '''
        # get all jobs with pandastatus running and actpandastatus tovalidate
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
            self.removeOutputFiles(surls[se])

        for job in jobstoupdate:
            select = "arcjobid='"+str(job["arcjobid"])+"'"
            desc = {}
            desc["actpandastatus"] = "done"
            self.dbpanda.updateJobsLazy(select, desc)
            desc = {}
            # set arcjobs state toclean
            desc = {"arcstate":"toclean", "tarcstate": self.dbarc.getTimeStamp()}
            self.dbarc.updateArcJobLazy(job["arcjobid"], desc)
        self.dbarc.Commit()


    def process(self):
        self.validateFinishedJobs()
        self.cleanFailedJobs()
        self.cleanResubmittingJobs()

if __name__ == '__main__':

    am=aCTValidator()
    am.run()
    am.finish()
