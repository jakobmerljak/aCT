from act.common.aCTLogger import aCTLogger
from act.client.clientdb import ClientDB

def bootstrap():

    logger = aCTLogger('aCTBootstrap')
    log = logger()
    clientdb = ClientDB(log)
    if not clientdb.createTables():
        print('Error creating client tables, see aCTBootstrap.log for details')
