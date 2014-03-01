from aCTATLASProcess import aCTATLASProcess
import os
import tarfile
import re
from xml.dom import minidom

class aCTValidator(aCTATLASProcess):
    '''
    Validate output files for finished jobs, cleanup output files for failed jobs.
    '''

    def __init__(self):
        aCTATLASProcess.__init__(self)

    def extractSRMFilesFromMetadata(self, arcjobid):
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
                checksum = "ad:"+ adler32
                if not surls.has_key(se):
                    surls[se]= []
                surls[se] += [{"surl":surl, "fsize":size, "checksum":checksum, "arcjobid":arcjobid}]
        
        return surls
            

    def checkSRMFiles(self, surls):
        '''
        Check if SURLs are working. Returns a dict of arcjobid:success
        '''
        pass

    def removeSRMFiles(self, surls):
        '''
        Remove SURLs.
        '''
        pass

    def validateFinishedJobs(self):
        '''
        Check for jobs with actpandastate tovalidate and pandastatus running
        Check if the srms in metadata.xml are valid.
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
        
        # pull out srm info from metadata.xml into dict, order by SE

        surls = {}
        for job in jobstoupdate:
            surls.update(self.extractSRMFilesFromMetadata(job["arcjobid"]))

        if not surls:
            # nothing to validate
            return

        # check if surls valid, update pandastate accordingly
        for se in surls:
            checkedsurls = self.checkSRMFiles(surls[se])
            for id,passed in checkedsurls.items():
                if passed:
                    select = "arcjobid='"+str(id)+"'"
                    desc = {}
                    desc["pandastatus"] = "finished"
                    desc["actpandastatus"] = "done"
                    self.dbpanda.updateJobsLazy(select, desc)
                    # set arcjobs state toclean
                    desc = {"arcstate":"toclean", "tarcstate": self.dbarc.getTimeStamp()}
                    self.dbarc.updateArcJobLazy(id, desc)
                else:
                    # todo: output file failed, should clean up output
                    select = "arcjobid='"+str(id)+"'"
                    desc = {}
                    desc["actpandastatus"] = "tobekilled"
                    self.dbpanda.updateJobsLazy(select, desc)
        self.dbpanda.Commit()
        self.dbarc.Commit()            
                
                
    def cleanFailedJobs(self):
        '''
        Check for jobs with actpandastate tovalidate and pandastatus failed
        Delete the srm files in metadata.xml.
        Move actpandastatus to failed. 
        '''
        # get all jobs with pandastatus running and actpandastatus tovalidate
        select = "(pandastatus='failed' and actpandastatus='tovalidate') limit 100000"
        columns = ["arcjobid"]
        jobstoupdate=self.dbpanda.getJobs(select, columns=columns)

        if len(jobstoupdate)==0:
            # nothing to do
            return

        # pull out srm info from metadata.xml into dict, order by SE

        surls = {}
        for job in jobstoupdate:
            surls.update(self.extractSRMFilesFromMetadata(job["arcjobid"]))

        if not surls:
            # nothing to clean
            return
        
        for se in surls:
            self.removeSRMFiles(surls[se])
        
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
        Delete the srm files in metadata.xml.
        Move actpandastatus to starting. 
        '''
        # get all jobs with pandastatus running and actpandastatus tovalidate
        select = "(pandastatus='starting' and actpandastatus='toresubmit') limit 100000"
        columns = ["arcjobid"]
        jobstoupdate=self.dbpanda.getJobs(select, columns=columns)

        if len(jobstoupdate)==0:
            # nothing to do
            return

        # pull out srm info from metadata.xml into dict, order by SE

        surls = {}
        for job in jobstoupdate:
            surls.update(self.extractSRMFilesFromMetadata(job["arcjobid"]))

        if not surls:
            # nothing to clean
            return
        
        for se in surls:
            self.removeSRMFiles(surls[se])

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
