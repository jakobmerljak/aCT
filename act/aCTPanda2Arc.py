import os
import sys
import re
import aCTUtils
import aCTSignal
import aCTPanda2Xrsl
import aCTDB
import aCTConfig
import aCTLogger
import aCTDBPanda
import aCTDBArc

class aCTPanda2Arc:

	def __init__(self):

	        # xml config file
	        self.conf=aCTConfig.aCTConfigATLAS()
        	self.arcconf=aCTConfig.aCTConfigARC()
	        # logger	
        	self.logger=aCTLogger.aCTLogger("panda2arc")
        	self.log=self.logger()
        	self.log.info("Start")

		# database
	        self.dbp=aCTDBPanda.aCTDBPanda(self.log,self.arcconf.get(["db","file"]))
	        self.dba=aCTDBArc.aCTDBArc(self.log,self.arcconf.get(["db","file"]))

		self.sites={}
	  	for sitename in self.conf.getList(["sites","site","name"]):
             	  self.sites[sitename] = {}
                  self.sites[sitename]['endpoints'] = self.conf.getListCond(["sites","site"],"name=" + sitename ,["endpoints","item"])
                  self.sites[sitename]['schedconfig'] = self.conf.getListCond(["sites","site"],"name=" + sitename ,["schedconfig"])[0]
		print self.sites

	def createArcJobs(self):

	  jobs=self.dbp.getJobs("arcjobid is NULL")


	  for job in jobs:
		print job['pandajob']
		parser=aCTPanda2Xrsl.aCTPanda2Xrsl(job['pandajob'],self.sites[job['siteName']]['schedconfig'])
		parser.parse()
		xrsl =  parser.getXrsl()
		if xrsl is not None:
 		  print xrsl
		  endpoint=self.sites[job['siteName']]['endpoints'][0]
		  from urlparse import urlparse
		  cl=urlparse(endpoint).hostname
		  aid = self.dba.insertArcJobDescription(xrsl, maxattempts=5,cluster=cl)
		  jd={}
		  jd['arcjobid']=aid['LAST_INSERT_ID()']
		  self.dbp.updateJob(job['pandaid'],jd)
		

	def Run(self):
	 try:
           self.log.info("Running")

           while 1:
                self.conf.parse()

                self.createArcJobs()

                aCTUtils.sleep(100000)

         except aCTSignal.ExceptInterrupt,x:
           print x
           return


	def Finish(self):
		pass



if __name__ == '__main__':

	am=aCTPanda2Arc()
	am.Run()
	am.Finish()
