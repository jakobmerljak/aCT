import aCTConfig

supported_dbms={}

try:
    from aCTDBSqlite import aCTDBSqlite
    supported_dbms['sqlite']=aCTDBSqlite
except:
    pass
try:
    from aCTDBMySQL import aCTDBMySQL
    supported_dbms['mysql']=aCTDBMySQL
except:
    pass
try:
    from aCTDBOracle import aCTDBOracle
    supported_dbms['oracle']=aCTDBOracle
except:
    pass

config=aCTConfig.aCTConfig()
dbtype=config.get(('db', 'type')).lower()

class aCTDBMS(supported_dbms[dbtype]):
    """Class for generic DB Mgmt System db operations. Inherit specifics from its speciallized superclass depending on configured dbtype."""
    
    def __init__(self,logger,dbname="act"):
        self.log=logger
        self.dbname=dbname
        # TODO: Find more generic way to get db config vars
        self.dbtype=dbtype
        if self.dbtype=='sqlite':
            aCTDBSqlite.__init__(self, logger)
        elif self.dbtype=='mysql':
            self.socket=str(config.get(('db', 'socket')))
            self.dbname=str(config.get(('db', 'name')))
            aCTDBMySQL.__init__(self, logger)
        elif self.dbtype=='oracle':
            aCTDBOracle.__init__(self, logger)
        else:
            raise Exception, "DB type %s is not implemented."%self.dbtype

    def getCursor(self):
        return super(aCTDBMS, self).getCursor()

    def timeStampLessThan(self,column,timediff):
        return super(aCTDBMS, self).timeStampLessThan(column,timediff)
    
    def addLock(self):
        return super(aCTDBMS, self).addLock()
