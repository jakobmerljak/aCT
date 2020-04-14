from act.db.aCTDB import aCTDB

class aCTDBLDMX(aCTDB):

    def __init__(self, log):
        aCTDB.__init__(self, log, 'ldmxjobs')

    def createTables(self):
        '''
         ldmxjobs:
           - id: Auto-incremented counter
           - modified: Auto-updated modification time
           - created: Creation time of ldmx job
           - sitename: the site on which the job ran or is submitted to
           - arcjobid: Row ID of job in arcjobs table
           - description: LDMX job description (config file)
           - template: LDMX job template file
           - ldmxstatus: LDMX job status
                 new: job has been entered but not processed yet
                 waiting: job is waiting to be submitted
                 submitted: job is submitted to ARC CE
                 queueing: job is queued in CE batch system
                 running: job is running in the site batch system
                 tovalidate: job has finished and is in aCT post-processing
                 finished: job finished successfully
                 failed: job failed
                 toresubmit: job will be cancelled and resubmitted
                 tocancel: job will be cancelled
                 cancelling: job is being cancelled
                 cancelled: job was cancelled
           - priority: Job priority
           - starttime: Job start time
           - endtime: Job end time
           - computingelement: CE where the job is running
           - proxyid: ID of proxy in proxies table to use for this job

        ldmxarchive:
          - Selected fields from above list:
            - id, siteName, ldmxstatus, starttime, endtime
        '''

        table_create = """
        create table ldmxjobs (
        id INTEGER PRIMARY KEY AUTO_INCREMENT,
        modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        sitename VARCHAR(255),
        arcjobid integer,
        description VARCHAR(255),
        template VARCHAR(255)
        ldmxstatus VARCHAR(255),
        priority integer,
        starttime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        endtime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        computingElement VARCHAR(255),
        proxyid integer
        )
"""

        # First check if table already exists
        c = self.db.getCursor()
        c.execute("show tables like 'ldmxjobs'")
        row = c.fetchone()
        self.Commit()
        if row:
            answer = input("Table ldmxjobs already exists!\nAre you sure you want to recreate it? (y/n) ")
            if answer != 'y':
                return True
            c.execute("drop table ldmxjobs")

        try:
            c.execute(table_create)
            # add indexes
            c.execute("ALTER TABLE ldmxjobs ADD INDEX (arcjobid)")
            c.execute("ALTER TABLE ldmxjobs ADD INDEX (ldmxstatus)")
            c.execute("ALTER TABLE ldmxjobs ADD INDEX (sitename)")
        except Exception as x:
            self.log.error(f"Failed to create table ldmxjobs: {x}")
            return False

        archive_table_create = """
        create table ldmxarchive (
        id bigint,
        sitename VARCHAR(255),
        ldmxstatus VARCHAR(255),
        starttime TIMESTAMP NOT NULL,
        endtime TIMESTAMP NOT NULL
        )
"""

        try:
            c.execute("drop table ldmxarchive")
        except:
            self.log.warning("no ldmxarchive table")
        try:
            c.execute(archive_table_create)
        except Exception as x:
            self.log.error(f"Failed to create table ldmxarchive: {x}")
            return False

        self.Commit()
        return True


    def insertJob(self, description, template, proxyid, priority=0):
        '''Insert new job description'''
        desc = {'description': description,
                'template': template,
                'proxyid': proxyid,
                'priority': priority,
                'ldmxstatus': 'new'}
        s = f"insert into ldmxjobs ({','.join([k for k in desc.keys()])}) values ({','.join(['%s' for k in desc.keys()])})"
        c = self.db.getCursor()
        c.execute(s, list(desc.values()))
        c.execute("SELECT LAST_INSERT_ID()")
        row = c.fetchone()
        self.Commit()
        return row

    def insertJobArchiveLazy(self, desc={}):
        s = f"insert into ldmxarchive ({','.join([k for k in desc.keys()])}) values ({','.join(['%s' for k in desc.keys()])})"
        c = self.db.getCursor()
        c.execute(s, list(desc.values()))

    def deleteJob(self, id):
        c = self.db.getCursor()
        c.execute(f"delete from ldmxjobs where id={id}")
        self.Commit()

    def updateJob(self, id, desc):
        self.updateJobLazy(id, desc)
        self.Commit()

    def updateJobLazy(self, id, desc):
        s = f"UPDATE ldmxjobs SET {','.join(['%s=%%s' % (k) for k in desc.keys()])} WHERE id={id}"
        c = self.db.getCursor()
        c.execute(s, list(desc.values()))

    def updateJobs(self, select, desc):
        self.updateJobsLazy(select, desc)
        self.Commit()

    def updateJobsLazy(self, select, desc):
        s = f"UPDATE ldmxjobs SET {','.join(['%s=%%s' % (k) for k in desc.keys()])} WHERE {select}"
        c = self.db.getCursor()
        c.execute(s, list(desc.values()))

    def getJob(self, id, columns=[]):
        c = self.db.getCursor()
        c.execute(f"SELECT {self._column_list2str(columns)} FROM ldmxjobs WHERE id={id}")
        row = c.fetchone()
        return row

    def getJobs(self, select, columns=[]):
        c = self.db.getCursor()
        c.execute(f"SELECT {self._column_list2str(columns)} FROM ldmxjobs WHERE {select}")
        rows = c.fetchall()
        return rows

    def getNJobs(self, select):
        c = self.db.getCursor()
        c.execute(f"select count(*) from ldmxjobs where {select}")
        njobs = c.fetchone()['count(*)']
        return int(njobs)

if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.DEBUG)
    adb = aCTDBLDMX(logging.getLogger())
    adb.createTables()
