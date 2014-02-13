import os
import time
from aCTDB import aCTDB

class aCTDBPanda(aCTDB):
    
    def createTables(self):
        # jobs:
        #   - pandaid:
        #   - tstamp: timestamp of last record update
        #   - pandajob: xml panda job description
        #   - arcjobid: gsiftp://cluster.domain/id
        #   - jobname: arc(panda) job name
        #   - arcstatus: submitted + GM job status (FINISHED, FAILED, INRLS:Q...)
        #   - tarcstatus: time stamp of last arcstatus
        #   - arcexitcode:
        #   - pstatus: panda job status (sent, submitted, running, ...)
        #   - theartbeat: time stamp of last heartbeat (pstatus set)
        #   - trfstatus: inarc, downloaded, lfcregisterred,
        #   - nrerun: rerun count
        # Workflow:
        #   - getjobs -> pandaid,pandajob,pstatus=sent
        #   - pstatus=sent, arcstatus=NULL -> submit -> arcstatus=submitted
        #   - pstatus=sent, arcstatus=submitted -> updatePandaSubmitted -> pstatus=running, theartbeat
        #   - pstatus=running, arcstatus=submitted, theartbeat old -> updatePandaHeartbeat ->theartbeat
        #   - pstatus=running, arcstatus=FINISHED,FAILED -> updatePandaFinished -> pstatus=finished?,theartbeat
        #   - pstatus=running, arcstatus=not NULL, trfstatus=inarc -> checkJobs -> arcstatus, fill arcjobs
        #   - pstatus=running, arcstatus=FINISHED -> downloadFinished -> pandaid=holding,transfer ?
        #   - pstatus=running, arcstatus=FAILED -> processFailed ->
        #             1. rerunable -> arcstatus=submitted
        #             2. if not -> pstatus=failed, cleanup, logfiles, etc... TODO
        aCTDB.createTables(self)
        #str="create table jobs (pandaid integer, tstamp timestamp, pandajob text, arcjobid text, jobname text, arcstatus text, tarcstatus timestamp, arcexitcode integer, pstatus text, theartbeat timestamp, trfstatus text, nrerun integer,lfns text, turls text)"
        str="""create table pandajobs (
		id INTEGER PRIMARY KEY AUTO_INCREMENT,
            	modified TIMESTAMP,
		created TIMESTAMP,
                pandaid integer, 
		pandajob text,
		siteName VARCHAR(255),
		prodSourceLabel VARCHAR(255),
		arcjobid integer,
		pandastatus text, 
		theartbeat timestamp,
                priority integer
	)
"""
        c=self.getCursor()
        try:
            c.execute("drop table pandajobs")
        except:
            self.log.warning("no pandajobs table")
            pass
        try:
            c.execute(str)
            self.conn.commit()
        except Exception,x:
            self.log.error("failed create table %s" %x)
            pass

    def insertJob(self,pandaid,pandajob,desc={}):
        desc['created']=self.getTimeStamp()
        desc['pandaid']=pandaid
        desc['pandajob']=pandajob
        s="insert into pandajobs (" + ",".join([k for k in desc.keys()]) + ") values (" + ",".join(['%s' for k in desc.keys()]) + ")"
        c=self.getCursor()
        c.execute(s,desc.values())
        self.conn.commit()

    def deleteJob(self,pandaid):
        c=self.getCursor()
        c.execute("delete from pandajobs where pandaid="+str(pandaid))
        self.conn.commit()

    def updateJob(self,pandaid,desc):
        self.updateJobLazy(pandaid,desc)
        self.conn.commit()

    def updateJobLazy(self,pandaid,desc):
        desc['modified']=self.getTimeStamp()
        s="UPDATE pandajobs SET " + ",".join(['%s=%%s' % (k) for k in desc.keys()])
        s+=" WHERE pandaid="+str(pandaid)
        c=self.getCursor()
        c.execute(s,desc.values())

    def updateJobs(self, select, desc):
        self.updateJobsLazy(select, desc)
        self.conn.commit()

    def updateJobsLazy(self, select, desc):
        desc['modified']=self.getTimeStamp()
        s="UPDATE pandajobs SET " + ",".join(['%s=%%s' % (k) for k in desc.keys()])
        s+=" WHERE "+select
        c=self.getCursor()
        c.execute(s,desc.values())
        
    def getJob(self,pandaid,columns=[]):
        c=self.getCursor()
        c.execute("SELECT "+self._column_list2str(columns)+" FROM pandajobs WHERE pandaid="+str(pandaid))
        row=c.fetchone()
        return row

    def getJobs(self,select,columns=[]):
        c=self.getCursor()
        c.execute("SELECT "+self._column_list2str(columns)+" FROM pandajobs WHERE "+select)
        rows=c.fetchall()
        return rows

    def getNJobs(self,select):
        c=self.getCursor()
        c.execute("select count(*) from pandajobs where " + select)
        njobs=c.fetchone()['count(*)']
        return njobs

    def getJobReport(self):
        c=self.getCursor()
        c.execute("select arcjobid,arcstatus from pandajobs")
        rows=c.fetchall()
        return rows

if __name__ == '__main__':
    import logging
    import aCTConfig
    logging.basicConfig(level=logging.DEBUG)

    conf = aCTConfig.aCTConfigATLAS()

    adb = aCTDBPanda(logging.getLogger(),dbname=conf.get(["db","file"]))

    #adb=aCTDB(logging.getLogger('test'),dbname='test.sqlite')
    #adb=aCTDBPanda(logging.getLogger('test'))
    adb.createTables()
    n={}
    #n['trfstatus']='tolfc'
    adb.insertJob(1,"testblanj",n)
    exit(0)
    #adb.insertJob(2,"testbla tepec")
    #time.sleep(2)
    jd={}
    jd['pstatus']='sent'
    adb.updateJob(1,jd)
    job=adb.getJob(1)
    print job['pstatus'],job['pandaid']
    
    jobs=adb.getJobs("pstatus='sent'")
    for j in jobs:
        for k,v in j.items():
            if v != None:
                print k,v

