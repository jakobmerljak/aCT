from act.common.aCTLogger import aCTLogger
from act.atlas.aCTDBPanda import aCTDBPanda

def bootstrap():

    logger = aCTLogger('aCTBootstrap')
    log = logger()
    dbpanda = aCTDBPanda(log)
    if not dbpanda.createTables():
        print('Failed to create Panda tables, see aCTBootstrap.log for details')
