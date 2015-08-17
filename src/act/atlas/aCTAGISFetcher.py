# Handler for fetching site info from AGIS. Fetches data and stores it in json file.

from datetime import datetime, timedelta
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
        try:
            response = urllib2.urlopen(self.srv)
        except urllib2.URLError as e:
            self.log.warning("Failed to contact AGIS: %s" % str(e))
            # Check if the cached data is getting old, if so raise a critical error
            filename = self.conf.get(['agis','jsonfilename'])
            try:
                mtime = os.stat(filename).st_mtime
                if datetime.fromtimestamp(mtime) < datetime.now() - timedelta(hours=1):
                    self.log.critical("AGIS info has not been updated since more than 1 hour ago")
            except:
                # file may not have been created yet
                pass
            return ''

        urldata = response.read()
        self.log.debug("Fetched %s" % self.srv)
        return urldata
    
    def storeToFile(self, agisjson):
        if not agisjson:
            return
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
        self.log.info("Running")
        # todo: check if agis.json exists and return if too new
        # fetch data from AGIS
        agisjson = self.fetchFromAgis()
        # store data to file
        self.storeToFile(agisjson)
        # temporary hack to avoid too much agis fetching
        time.sleep(600)
        
if __name__ == '__main__':
    aaf=aCTAGISFetcher()
    aaf.run()
