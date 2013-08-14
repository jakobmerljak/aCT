import os
import time
from aCTDB import aCTDB

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

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
        str="create table jobs (pandaid integer, tstamp timestamp, pandajob text, arcjobid text, jobname text, arcstatus text, tarcstatus timestamp, arcexitcode integer, pstatus text, theartbeat timestamp, trfstatus text, nrerun integer,lfns text, turls text)"
        c=self.getCursor()
        try:
            c.execute("drop table jobs")
        except:
            self.log.warning("no jobs table")
            pass
        try:
            c.execute(str)
            self.conn.commit()
        except Exception,x:
            self.log.error("failed create table %s" %x)
            pass

    def insertJob(self,pandaid,pandajob,desc={}):
        desc['tstamp']=time.time()
        k="(pandaid,pandajob,pstatus,"+",".join(['%s' % key for key in desc.keys()])+")"
        v="("+str(pandaid)+",'"+pandajob+"','sent',"+",".join(['"%s"' % val for val in desc.values()])+")"
        s="insert into jobs "+k+" values "+v
        c=self.getCursor()
        #c.execute("insert into jobs (tstamp,pandaid,pandajob,pstatus) values ("+str(time.time())+","+str(pandaid)+",'"+pandajob+"','sent')")
        c.execute(s)
        self.conn.commit()

    def deleteJob(self,pandaid):
        c=self.getCursor()
        c.execute("delete from jobs where pandaid="+str(pandaid))
        self.conn.commit()

    def updateJob(self,id,desc):
        desc['tstamp']=time.time()
        s="update jobs set "+",".join(['%s="%s"' % (k, v) for k, v in desc.items()])
        s+=" where pandaid="+str(id)
        c=self.getCursor()
        c.execute(s)
        self.conn.commit()

    def updateJobLazy(self,id,desc):
        desc['tstamp']=time.time()
        s="update jobs set "+",".join(['%s="%s"' % (k, v) for k, v in desc.items()])
        s+=" where pandaid="+str(id)
        c=self.getCursor()
        c.execute(s)

    def getJob(self,pandaid,columns=[]):
        c=self.getCursor()
        c.execute("SELECT "+self._column_list2str(columns)+" FROM jobs WHERE pandaid="+str(pandaid))
        row=c.fetchone()
        return row

    def getJobs(self,select,columns=[]):
        c=self.getCursor()
        c.execute("SELECT "+self._column_list2str(columns)+" FROM jobs WHERE "+select)
        rows=c.fetchall()
        return rows

    def getNJobs(self):
        c=self.getCursor()
        c.execute("select count(*) from jobs")
        njobs=c.fetchone()['count(*)']
        return njobs

    def getJobReport(self):
        c=self.getCursor()
        c.execute("select arcjobid,arcstatus from jobs")
        rows=c.fetchall()
        return rows

if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.DEBUG)
    #adb=aCTDB(logging.getLogger('test'),dbname='test.sqlite')
    adb=aCTDBPanda(logging.getLogger('test'))
    adb.createTables()
    n={}
    n['trfstatus']='tolfc'
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

