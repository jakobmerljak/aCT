import datetime
from act.db import aCTDBMS
from act.common.aCTConfig import aCTConfigARC

class aCTDB(object):
    '''Superclass representing a general table in the DB'''

    def __init__(self, logger, tablename):
        self.log = logger
        self.table = tablename
        self.conf = aCTConfigARC()
        self.db = aCTDBMS.getDB(self.log, self.conf)

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
        if seconds:
            return datetime.datetime.utcfromtimestamp(seconds).isoformat()
        else:
            return datetime.datetime.utcnow().isoformat()

    def timeStampLessThan(self, column, timediff):
        return self.db.timeStampLessThan(column, timediff)

    def Commit(self, lock=False):
        if lock:
            res = self.db.releaseMutexLock(self.table)
            if not res:
                self.log.warning("Could not release lock: %s" % str(res))
        self.db.conn.commit()
        if lock:
            c = self.db.getCursor()
            c.execute("UNLOCK TABLES")
