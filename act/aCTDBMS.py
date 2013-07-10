import aCTConfig
import time

try:
    import pysqlite2.dbapi2 as sqlite
except:
    pass
try:
    import mysql.connector as mysql
except:
    pass


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

class aCTDBSqlite:
    """Class for Sqlite specific db operations."""
    
    def __init__(self,logger):
        try:
            self.conn=sqlite.connect(dbname,1800)
        except Exception, x:
            raise Exception, "Could not connect to sqlite. Have you installed pysqlite2?"
        self.conn.row_factory=dict_factory
        self.conn.execute('''PRAGMA synchronous=OFF''')
        self.log.info("initialized aCTDBSqlite")

class aCTDBMySQL:
    """Class for MySQL specific db operations."""

    def __init__(self,logger):
        try:
            print self.socket, self.dbname
            self.conn=mysql.connect(unix_socket=self.socket,db=self.dbname)
        except Exception, x:
            print Exception, x
            # if db doesnt exist, create it
            if x.errno==1049:
                self.conn=mysql.connect(unix_socket=socket)
                c=self.conn.cursor()
                c.execute("CREATE DATABASE "+dbname)
            else:
                raise "Could not connect to db "+dbname
        self.log.info("initialized aCTDBMySQL")

class aCTDBOracle:
    """Class for Oracle specific db operations."""

    def __init__(self,logger,socket="/tmp/act.mysql.socket",dbname="act"):
        raise Exception, "Oracle class is not implemented yet"


class aCTDBMS(aCTDBSqlite, aCTDBMySQL, aCTDBOracle):
    """Class for generic DB Mgmt System db operations. Inherit specifics from its speciallized superclasses."""
    
    def __init__(self,logger,dbname="act"):
        self.log=logger
        self.dbname=dbname
        config = aCTConfig.aCTConfig()
        # TODO: Find more generic way to get db config vars
        self.dbtype=config.get(('db', 'type')).lower()
        if self.dbtype.startswith('sqlite'):
            aCTDBSqlite.__init__(self, logger)
        elif self.dbtype.startswith('mysql'):
            self.socket=str(config.get(('db', 'socket')))
            self.dbname=str(config.get(('db', 'name')))
            aCTDBMySQL.__init__(self, logger)
        elif self.dbtype.startswith('oracle'):
            aCTDBOracle.__init__(self, logger)
        else:
            raise Exception, "DB type %s is not implemented."%self.dbtype

