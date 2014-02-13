import sqlite3 as sqlite

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

class aCTDBSqlite(object):
    """Class for Sqlite specific db operations."""
    
    def __init__(self,logger):
        try:
            self.conn=sqlite.connect(self.dbname,1800)
        except Exception, x:
            raise Exception, "Could not connect to sqlite: " + str(x)
        self.conn.row_factory=dict_factory
        self.conn.execute('''PRAGMA synchronous=OFF''')
        self.log.info("initialized aCTDBSqlite")

    def getCursor(self):
        return self.conn.cursor()

    def timeStampLessThan(self,column,timediff):
        return "datetime("+column+") < datetime('now', '-"+str(timediff)+" seconds')"

    def timeStampGreaterThan(self,column,timediff):
        return "datetime("+column+") > datetime('now', '-"+str(timediff)+" seconds')"

    def addLock(self):
        # SQLite does not support row locking
        return ""
