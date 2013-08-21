class aCTDBOracle(object):
    """Class for Oracle specific db operations."""

    def __init__(self,logger,socket="/tmp/act.mysql.socket",dbname="act"):
        raise Exception, "Oracle class is not implemented yet"

    def getCursor(self):
        return self.conn.cursor()

    def timeStampLessThan(self, column, timediff):
        # should be tested...
        return "(SYSDATE - TO_DATE('01-JAN-1970','DD-MON-YYYY')) * (86400)"

    def addLock(self):
        return " FOR UPDATE"
