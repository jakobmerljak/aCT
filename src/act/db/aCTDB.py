import os
import time
import datetime
from aCTDBMS import aCTDBMS

class aCTDB(aCTDBMS):

    def __init__(self,logger,dbname="aCTjobs.db"):
        # inherit DB Mgmt System from aCTDBMS
        aCTDBMS.__init__(self, logger, dbname)
    
    def _column_list2str(self,columns):
        s=""
        if columns:
            for col in columns:
                s+=col+", "
            s=s.strip(", ")
        else:
            s="*"
        return s
    
    
    def getTimeStamp(self, seconds=None):
        # todo: move to aCTDBMS and use internal dbms mechanisms for converting
        if seconds:
            return datetime.datetime.utcfromtimestamp(seconds).isoformat()
        else:
            return datetime.datetime.utcnow().isoformat()
    
    
    def createTables(self):
        # jobs (see aCTDBPanda.py):
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
        # arcjobs (see aCTDBArc.py):
        #   - pandaid:
        #   - tstamp: timestamp of last record update
        #   - tarcstatus: time stamp of last arcstatus ?
        #   - cluster: hostname of the cluster
        #   - submissiontime: time stamp of submission
        #   - completiontime: time stamp of FINISHED, FAILED
        #   - usedcputime:
        #   - usedwalltime:
        #   - errors: as reported by arc
        #   - exitcode:
        #   - executionnodes:
        #   - proxyexpiretime: time stamp of proxy
        #   - rerunable:
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
        str="create table schedconfig (cluster text, status text)"
        c=self.getCursor()
        try:
            c.execute("drop table schedconfig")
        except:
            self.log.warning("no schedconfig table")
            pass
        try:
            c.execute(str)
            self.conn.commit()
        except Exception,x:
            self.log.error("failed create tables %s" %x)
            pass

    def insertSchedconfig(self,cluster,status):
        c=self.getCursor()
        c.execute("insert into schedconfig (cluster,status) values ('%s','%s')" % (cluster,status))
        self.conn.commit()

    def updateSchedconfig(self,cluster,status):
        c=self.getCursor()
        c.execute("update schedconfig set status='%s' where cluster='%s'" % (status,cluster))
        self.conn.commit()

    def getSchedconfig(self,cluster):
        c=self.getCursor()
        c.execute("select status from schedconfig where cluster='%s'" % cluster)
        row=c.fetchone()
        return row

    def Commit(self, lock=False):
        self.conn.commit()
        if lock:
            c=self.getCursor()
            c.execute("UNLOCK TABLES")

    def removeJobs(self,pandaid):
        c=self.getCursor()
        try:
            c.execute("delete from jobs where pandaid="+str(pandaid))
            c.execute("delete from arcjobs where pandaid="+str(pandaid))
            self.conn.commit()
            return True
        except Exception,x:
            self.log.error(x)
            self.conn.rollback()
            return False

if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.DEBUG)
    #adb=aCTDB(logging.getLogger('test'),dbname='test.sqlite')
    adb=aCTDB(logging.getLogger('test'))
    adb.createTables()
    exit(0)
