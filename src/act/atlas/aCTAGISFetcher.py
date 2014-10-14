# Handler for fetching site info from AGIS. Fetches data and stores it in json file.

import time
import datetime
import re
import os
import shutil
import httplib, urllib
import urllib2

from act.common import aCTSignal
from act.common import aCTUtils
from act.common import aCTConfig

from aCTATLASProcess import aCTATLASProcess

class aCTAGISFetcher(aCTATLASProcess):
                 
    def __init__(self):
        aCTATLASProcess.__init__(self)
        self.srv=self.conf.get(['agis','server'])

    def fetchFromAgis(self):
        response = urllib2.urlopen(self.srv+'/query/list/?json&preset=schedconf.all')
        urldata = response.read()
        self.log.debug("Fetched "+self.srv+'/query/list/?json&preset=schedconf.all')
        return urldata
    
    def storeToFile(self, agisjson):
        filename = self.conf.get(['agis','jsonfilename'])
        tmpfile=filename+'_'
        f = open(tmpfile, 'w')
        f.write(agisjson)
        f.close()
        os.rename(tmpfile, filename)
        self.log.debug("Wrote "+filename)

    def process(self):
        """
        Main loop
        """        
        try:
            self.log.info("Running")
            # todo: check if agis.json exists and return if too new
            # fetch data from AGIS
            agisjson = self.fetchFromAgis()
            # store data to file
            self.storeToFile(agisjson)
            # temporary hack to avoid too much agis fetching
            time.sleep(3600)
        except aCTSignal.ExceptInterrupt,x:
            self.log.error(x)
            return

        
if __name__ == '__main__':
    aaf=aCTAGISFetcher()
    aaf.run()
