import os
import re
from xml.dom import minidom
import time

class aCTConfig:

    def __init__(self):
        self.top=[]
        self.log="aCTConfig.log"
        self.tparse=0
        self.parse()

    def parse(self):
        try:
            mtime=os.stat("aCTConfig.xml")[8]
            if mtime<=self.tparse:
                return
            xml=minidom.parse("aCTConfig.xml")
            self.top=xml.getElementsByTagName('config')
            self.tparse=mtime
            #fd=open(self.log,"a")
            #print >> fd,  "Config reloaded",time.asctime(time.localtime(mtime))
            #fd.close()
        except Exception,x:
            fd=open(self.log,"a")
            print >> fd,  "parsing failed",x
            fd.close()

        
    def getList(self,nodes):
        n0=self.top
        for name in nodes:
            tn=[]
            for n in n0:
                n1=n.getElementsByTagName(name)
                tn.extend(n1)
            n0=tn
        l=[]
        for nn in n0:
            l.append(nn.firstChild.data)
        return l
        
    def get(self,nodes):
        l = self.getList(nodes)
        if l:
            return l[0]
        return None

if __name__ == '__main__':

    actconf=aCTConfig()
    #actconf.printConfig()
    while 1:
        actconf.parse()
        print actconf.get(['jobs','maxqueued'])
        print actconf.get(['rls','server'])
        print actconf.getList(['test','a1','a2'])
        print actconf.getList(['atlasgiis','item'])
        print actconf.getList(['clustersreject','item'])
        print actconf.getList(['srm','token','name'])
        print actconf.getList(['srm','token','dir'])
        print actconf.get(['srm','prefix'])
        print actconf.getList(['brokerlist','broker','qreject','item'])
        for (i,j) in zip(actconf.getList(['srm','token','name']),actconf.getList(['srm','token','dir'])):
            print i,j
        exit(1)
        time.sleep(1)
    
