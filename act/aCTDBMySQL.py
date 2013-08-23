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
            print self.socket, self.dbname
            self.conn=mysql.connect(unix_socket=self.socket,db=self.dbname)
        except Exception, x:
            print Exception, x
            # if db doesnt exist, create it
            if x.errno==1049:
                self.conn=mysql.connect(unix_socket=self.socket)
                c=self.conn.cursor()
                c.execute("CREATE DATABASE "+self.dbname)
            else:
                raise "Could not connect to db "+self.dbname
        self.log.info("initialized aCTDBMySQL")

    def getCursor(self):
        # make sure cursor reads newest db state
        self.conn.commit()
        return self.conn.cursor(cursor_class=MySQLCursorDict)

    def timeStampLessThan(self,column,timediff):
        return "UNIX_TIMESTAMP("+column+") < UNIX_TIMESTAMP(UTC_TIMESTAMP()) - "+str(timediff)
    
    def addLock(self):
        return " FOR UPDATE"
