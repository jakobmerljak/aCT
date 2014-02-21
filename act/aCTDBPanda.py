from aCTDB import aCTDB

class aCTDBPanda(aCTDB):
    
    def createTables(self):
        '''
         pandajobs:
           - id: Auto-incremented counter
           - modified: Auto-updated modification time
           - created: Creation time of panda job
           - pandaid: Panda job ID
           - pandajob: String of panda job description
           - siteName: Panda Resource
           - prodSourceLabel: Type of job (managed, test, etc)
           - arcjobid: Row ID of job in arcjobs table
           - pandastatus: Panda job status corresponding to state on the panda server
                 sent: job is retrieved from panda
                 starting: job is in aCT but not yet running
                 running: job is running on worker node
                 transferring: job is finished but uploading output files or in aCT post-processing
                 finished: job finished successfully
                 failed: job failed (code or grid failure)
           - actpandastatus: aCT internal state of panda jobs
                 In addition to above states:
                 tovalidate: job has finished or failed and output files should
                   be validated or cleaned
                 toresubmit: job will be resubmitted but first output files
                   should be cleaned
                 done: aCT is finished with this job, nothing more needs to be done
                 tobekilled: panda requests that the job is cancelled
                 cancelled: job was cancelled, nothing more needs to be done
           - theartbeat: Timestamp of last heartbeat (pstatus set)
           - priority: Job priority
           - node: Worker node on which the job is running
           - startTime: Job start time
           - endTime: Job end time
           - computingElement: CE where the job is running
        '''
        aCTDB.createTables(self)
        str="""
        create table pandajobs (
        id INTEGER PRIMARY KEY AUTO_INCREMENT,
        modified TIMESTAMP,
        created TIMESTAMP,
        pandaid integer, 
        pandajob text,
        siteName VARCHAR(255),
        prodSourceLabel VARCHAR(255),
        arcjobid integer,
        pandastatus VARCHAR(255),
        actpandastatus VARCHAR(255),
        theartbeat timestamp,
        priority integer,
        node VARCHAR(255),
        startTime TIMESTAMP,
        endTime TIMESTAMP,
        computingElement VARCHAR(255)
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
    adb.createTables()
