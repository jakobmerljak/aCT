import os
import pysqlite2.dbapi2 as sqlite
import time

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

class aCTDB:

    def __init__(self,logger,dbname="aCTjobs.sqlite"):
        self.conn=sqlite.connect(dbname,1800)
        self.conn.row_factory=dict_factory
	self.conn.execute('''PRAGMA synchronous=OFF''')
        self.log=logger


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
        # arcjobs:
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
        str="create table jobs (pandaid integer, tstamp timestamp, pandajob text, arcjobid text, jobname text, arcstatus text, tarcstatus timestamp, arcexitcode integer, pstatus text, theartbeat timestamp, trfstatus text, nrerun integer,lfns text, turls text)"
        str2="create table arcjobs (pandaid integer, tstamp timestamp, tarcstatus timestamp, cluster text,  submissiontime timestamp, completiontime timestamp, usedcputime integer, usedwalltime integer, errors text, exitcode integer, executionnodes text, proxyexpiretime timestamp, rerunable text, xml text, log text, starttime timestamp, endtime timestamp)"
        str3="create table schedconfig (cluster text, status text)"
        c=self.conn.cursor()
        try:
            c.execute("drop table jobs")
        except:
            self.log.warning("no jobs table")
            pass
        try:
            c.execute(str)
            c.execute(str2)
            c.execute(str3)
            self.conn.commit()
        except Exception,x:
            self.log.error("failed create tables %s" %x)
            pass

    def insertSchedconfig(self,cluster,status):
        c=self.conn.cursor()
        c.execute("insert into schedconfig (cluster,status) values ('%s','%s')" % (cluster,status))
        self.conn.commit()

    def updateSchedconfig(self,cluster,status):
        c=self.conn.cursor()
        c.execute("update schedconfig set status='%s' where cluster='%s'" % (status,cluster))
        self.conn.commit()

    def getSchedconfig(self,cluster):
        c=self.conn.cursor()
        c.execute("select status from schedconfig where cluster='%s'" % cluster)
        row=c.fetchone()
        return row

    def insertJob(self,pandaid,pandajob,desc={}):
        desc['tstamp']=time.time()
        k="(pandaid,pandajob,pstatus,"+",".join(['%s' % key for key in desc.keys()])+")"
        v="("+str(pandaid)+",'"+pandajob+"','sent',"+",".join(['"%s"' % val for val in desc.values()])+")"
        s="insert into jobs "+k+" values "+v
        c=self.conn.cursor()
        #c.execute("insert into jobs (tstamp,pandaid,pandajob,pstatus) values ("+str(time.time())+","+str(pandaid)+",'"+pandajob+"','sent')")
        c.execute(s)
        self.conn.commit()

    def insertArcJob(self,pandaid):
        c=self.conn.cursor()
        c.execute("insert into arcjobs (tstamp,pandaid) values ("+str(time.time())+","+str(pandaid)+")")
        self.conn.commit()

    def deleteJob(self,pandaid):
        c=self.conn.cursor()
        c.execute("delete from jobs where pandaid="+str(pandaid))
        self.conn.commit()

    def deleteArcJob(self,pandaid):
        c=self.conn.cursor()
        c.execute("delete from arcjobs where pandaid="+str(pandaid))
        self.conn.commit()

    def updateJob(self,id,desc):
        desc['tstamp']=time.time()
        s="update jobs set "+",".join(['%s="%s"' % (k, v) for k, v in desc.items()])
        s+=" where pandaid="+str(id)
        c=self.conn.cursor()
        c.execute(s)
        self.conn.commit()

    def updateArcJob(self,id,desc):
        desc['tstamp']=time.time()
        s="update arcjobs set "+",".join(['%s=\'%s\'' % (k, v) for k, v in desc.items()])
        s+=" where pandaid="+str(id)
        c=self.conn.cursor()
        c.execute("select pandaid from arcjobs where pandaid="+str(id))
        row=c.fetchone()
        if(row is None) :
            self.insertArcJob(id)
        c.execute(s)
        self.conn.commit()

    def updateJobLazy(self,id,desc):
        desc['tstamp']=time.time()
        s="update jobs set "+",".join(['%s="%s"' % (k, v) for k, v in desc.items()])
        s+=" where pandaid="+str(id)
        c=self.conn.cursor()
        c.execute(s)

    def updateArcJobLazy(self,id,desc):
        desc['tstamp']=time.time()
        s="update arcjobs set "+",".join(['%s=\'%s\'' % (k, v) for k, v in desc.items()])
        s+=" where pandaid="+str(id)
        c=self.conn.cursor()
        c.execute("select pandaid from arcjobs where pandaid="+str(id))
        row=c.fetchone()
        if(row is None) :
            self.insertArcJob(id)
        c.execute(s)

    def Commit(self):
        self.conn.commit()


    def getJob(self,pandaid):
        c=self.conn.cursor()
        c.execute("select * from jobs where pandaid="+str(pandaid))
        row=c.fetchone()
        return row

    def getJobs(self,select):
        c=self.conn.cursor()
        c.execute("select * from jobs where "+select)
        rows=c.fetchall()
        return rows

    def getArcJob(self,pandaid):
        c=self.conn.cursor()
        c.execute("select * from arcjobs where pandaid="+str(pandaid))
        row=c.fetchone()
        return row

    def getArcJobs(self,select):
        c=self.conn.cursor()
        c.execute("select * from arcjobs where "+select)
        rows=c.fetchall()
        return rows

    def removeJobs(self,pandaid):
        c=self.conn.cursor()
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
    n={}
    n['trfstatus']='tolfc'
    adb.insertJob(1,"testblanj",n)
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
