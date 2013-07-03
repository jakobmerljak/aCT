import os
import time
import aCTDBPanda
from arclib import *
import aCTConfig
import re
import commands
from xml.dom import minidom
from xml.sax import SAXParseException
import LFCTools
import shutil
import aCTLogger
import aCTSignal
import aCTUtils
import cgi
import pickle
from threading import Thread,enumerate


class CleanerThr(Thread):
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


class aCTCleaner:
    """
    Class to handle job checking and job downloading/retreival.
    """

    def __init__(self):
        # logger
        self.logger=aCTLogger.aCTLogger("cleaner")
        self.log=self.logger()

        # config
        self.conf=aCTConfig.aCTConfig()
        # database
        self.db=aCTDBPanda.aCTDBPanda(self.log,self.conf.get(["db","file"]))
        # ARC FTPControl
        self.ftpcontrol=FTPControl()
        
        
    def processCompleted(self,pstatus='done',trfstatus='toremove'):
        """
        Remove completed jobs from database and clean the jobs on clusters.
        """
        jobs=self.db.getJobs("pstatus like '"+pstatus+"' and trfstatus='"+trfstatus+"'")
        if len(jobs):
            self.log.info("%d" % len(jobs))
        else:
            return
        c=JobFTPControl()
        # TODO threaded
        for j in jobs:
            self.log.info("Removing job %d %s" % (j['pandaid'],j['arcjobid']))
            try:
	        try:
                  jl=[]
                  jl.append(str(j['arcjobid']))
                  if trfstatus == 'tokill':
                      c.Cancel(str(j['arcjobid']))
                  print "Removing",j['pandaid'],j['arcjobid']
		except URLError:
		    pass
		except:
		  pass
                #try:
                #    c.Cancel(str(j['arcjobid']))
                #except:
                #    pass
                try:
		    if j['arcjobid'] is not None:
                      c.Clean(str(j['arcjobid']))
		except URLError:
		    pass
                except:
                    pass
                try:
		    if j['arcjobid'] is not None:
                      RemoveJobID(str(j['arcjobid']))
		except URLError:
		    pass
                except:
                    pass
                self.db.removeJobs(j['pandaid'])
                # clean xml and pickle
                try:
                    os.unlink(self.conf.get(['tmp','dir'])+"/xml/"+str(j['pandaid'])+".xml")
                except:
                    pass
                try:
                    os.unlink(self.conf.get(['tmp','dir'])+"/pickle/"+str(j['pandaid'])+".pickle")
                except:
                    pass
                try:
		    jid=str(j['arcjobid'])
                    #reg=re.search('.+/([0-9]+)',jid)
		    reg=re.match('gsiftp://(.+):2811/jobs/(\w+)',jid)
                    sessionid=reg.group(2)
        	    outd=self.conf.get(['tmp','dir'])+"/"+sessionid
                    shutil.rmtree(outd)
                except:
                    pass
            except FTPControlError,x:
                self.log.error(x)
                continue
            except Exception,x:
                self.log.error("%s" % x )
                continue
            
            

    def run(self):
        """
        Main loop
        """
        self.log.info("Start")
        try:
            while 1:
                # reparse config file
                self.conf.parse()
                self.processCompleted('%','toremove')
                aCTUtils.sleep(10)
        except aCTSignal.ExceptInterrupt,x:
            self.log.error( x )
        
            
    def finish(self):
        """
        clean termination handled by signal
        """
        self.log.info("Cleanup")

if __name__ == '__main__':
    ad=aCTCleaner()
    ad.run()
    ad.finish()

