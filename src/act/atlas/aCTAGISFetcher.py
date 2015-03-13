# Handler for fetching site info from AGIS. Fetches data and stores it in json file.

import os
import time
import urllib2

from act.common import aCTSignal
from aCTATLASProcess import aCTATLASProcess

class aCTAGISFetcher(aCTATLASProcess):
                 
    def __init__(self):
        aCTATLASProcess.__init__(self)
        self.srv = self.conf.get(['agis','server'])
        self.pilotmanager = self.conf.get(['agis','pilotmanager'])

    def fetchFromAgis(self):
        url = '%s&pilot_manager=%s' % (self.srv, self.pilotmanager)
        response = urllib2.urlopen(url)
        urldata = response.read()
        self.log.debug("Fetched %s" % url)
        return urldata
    
    def storeToFile(self, agisjson):
        filename = self.conf.get(['agis','jsonfilename'])
        tmpfile=filename+'_'
        try:
            with open(tmpfile, 'w') as f:
                f.write(agisjson)
        except:
            os.makedirs(tmpfile[:tmpfile.rfind('/')], 0755)
            with open(tmpfile, 'w') as f:
                f.write(agisjson)

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
