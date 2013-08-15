class aCTDBOracle(object):
    """Class for Oracle specific db operations."""

    def __init__(self,logger,socket="/tmp/act.mysql.socket",dbname="act"):
        raise Exception, "Oracle class is not implemented yet"

    def getCursor(self):
        return self.conn.cursor()

    def getNowStr(self):
        return "NOW()"
