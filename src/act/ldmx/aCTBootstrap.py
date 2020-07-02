from act.common.aCTLogger import aCTLogger
from act.ldmx.aCTDBLDMX import aCTDBLDMX

def bootstrap():

    logger = aCTLogger('aCTBootstrap')
    log = logger()
    dbldmx = aCTDBLDMX(log)
    if not dbldmx.createTables():
        print('Failed to create LDMX tables, see aCTBootstrap.log for details')
