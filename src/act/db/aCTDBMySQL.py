import mysql.connector as mysql

class MySQLCursorDict(mysql.cursor.MySQLCursor):
    def _row_to_python(self, rowdata, desc=None):
        row = super(MySQLCursorDict, self)._row_to_python(rowdata, desc)
        if row:
            return dict(zip(self.column_names, row))
        return None

class aCTDBMySQL(object):
    """Class for MySQL specific db operations."""

    def __init__(self,logger):
        try:
            self._connect(self.dbname)
        except Exception, x:
            print Exception, x
            # if db doesnt exist, create it
            if x.errno!=1049:
                raise x
            self._connect()
            c=self.conn.cursor()
            c.execute("CREATE DATABASE "+self.dbname)

        self.log.debug("initialized aCTDBMySQL")
    
    def _connect(self, dbname=None):
        if self.socket != 'None':
            self.conn=mysql.connect(unix_socket=self.socket,db=dbname)
        elif self.user and self.passwd:
            if self.host != 'None' and self.port != 'None':
                self.conn=mysql.connect(user=self.user, password=self.passwd, host=self.host, port=self.port, db=dbname)
            else:
                self.conn=mysql.connect(user=self.user, password=self.passwd, db=dbname)
        
    def getCursor(self):
        # make sure cursor reads newest db state
        self.conn.commit()
        try:
            return self.conn.cursor(dictionary=True)
        except:
            return self.conn.cursor(cursor_class=MySQLCursorDict)

    def timeStampLessThan(self,column,timediff):
        return "UNIX_TIMESTAMP("+column+") < UNIX_TIMESTAMP(UTC_TIMESTAMP()) - "+str(timediff)
    
    def timeStampGreaterThan(self,column,timediff):
        return "UNIX_TIMESTAMP("+column+") > UNIX_TIMESTAMP(UTC_TIMESTAMP()) - "+str(timediff)
    
    def addLock(self):
        return " FOR UPDATE"

    def getMutexLock(self, lock_name, timeout=2):
        """
        Function to get named lock. Returns 1 if lock was obtained, 0 if attempt timed out, None if error occured.
        """
        c=self.getCursor()
        select="GET_LOCK('"+lock_name+"',"+str(timeout)+")"
        c.execute("SELECT "+select)
        return c.fetchone()[select]
    
    def releaseMutexLock(self, lock_name):
        """
        Function to release named lock. Returns 1 if lock was released, 0 if someone else owns the lock, None if error occured.
        """
        c=self.getCursor()
        select="RELEASE_LOCK('"+lock_name+"')"
        c.execute("SELECT "+select)
        return c.fetchone()[select]
