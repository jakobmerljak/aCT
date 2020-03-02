# Handler for fetching site info from AGIS. Fetches data and stores it in json file.

from datetime import datetime, timedelta
import os
import time
import urllib.request, urllib.error

from act.atlas.aCTATLASProcess import aCTATLASProcess

class aCTAGISFetcher(aCTATLASProcess):
                 
    def __init__(self):
        aCTATLASProcess.__init__(self)
        self.queues = self.conf.get(['agis','server'])
        self.oses = self.conf.get(['agis','objectstores'])
        self.queuesfile =self.conf.get(['agis', 'jsonfilename'])
        self.osesfile =self.conf.get(['agis', 'osfilename'])


    def fetchFromAgis(self, url, filename):
        try:
            response = urllib.request.urlopen(url, timeout=60)
        except urllib.error.URLError as e:
            self.log.warning("Failed to contact AGIS: %s" % str(e))
            # Check if the cached data is getting old, if so raise a critical error
            try:
                mtime = os.stat(filename).st_mtime
                if datetime.fromtimestamp(mtime) < datetime.now() - timedelta(hours=1):
                    self.log.critical("AGIS info has not been updated since more than 1 hour ago")
                    self.criticallog.critical("AGIS info has not been updated since more than 1 hour ago")
            except:
                # file may not have been created yet
                pass
            return ''

        urldata = response.read().decode('utf-8')
        self.log.debug("Fetched %s" % url)
        return urldata
    
    def storeToFile(self, agisjson, filename):
        if not agisjson:
            return
        tmpfile=filename+'_'
        try:
            with open(tmpfile, 'w') as f:
                f.write(agisjson)
        except:
            os.makedirs(tmpfile[:tmpfile.rfind('/')], 0o755)
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
        queuesjson = self.fetchFromAgis(self.queues, self.queuesfile)
        osesjson = self.fetchFromAgis(self.oses, self.osesfile)
        # store data to file
        self.storeToFile(queuesjson, self.queuesfile)
        self.storeToFile(osesjson, self.osesfile)
        # temporary hack to avoid too much agis fetching
        time.sleep(600)
        
if __name__ == '__main__':
    aaf=aCTAGISFetcher()
    aaf.run()
