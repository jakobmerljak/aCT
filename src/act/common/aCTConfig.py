import os
import time
from xml.dom import minidom

class aCTConfig:

    def __init__(self, configfile):
        self.configfile = configfile
        self.top=[]
        self.tparse=0
        self.parse()

    def parse(self):
        mtime=os.stat(self.configfile)[8]
        if mtime<=self.tparse:
            return
        xml=minidom.parse(self.configfile)
        self.top=xml.getElementsByTagName('config')
        self.tparse=mtime


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


    def getListCond(self,nodesc,cond,nodes):
        c=cond.split("=")
        n0=self.top
        for name in nodesc:
            tn=[]
            for n in n0:
                n1=n.getElementsByTagName(name)
                tn.extend(n1)
            n0=tn

        el=[]
        for t in tn:
            n1=t.getElementsByTagName(c[0])
            if( n1[0].firstChild.data == c[1] ):
                el.append(t)

        n0=el
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

class aCTConfigARC(aCTConfig):

    def __init__(self):
        if 'ACTCONFIGARC' in os.environ:
            configfile = os.environ['ACTCONFIGARC']
        else:
            configfile="aCTConfigARC.xml"
        aCTConfig.__init__(self, configfile)

class aCTConfigATLAS(aCTConfig):

    def __init__(self):
        if 'ACTCONFIGATLAS' in os.environ:
            configfile = os.environ['ACTCONFIGATLAS']
        else:
            configfile="aCTConfigATLAS.xml"
        aCTConfig.__init__(self, configfile)


if __name__ == '__main__':

    actconf=aCTConfig("aCTConfigARC.xml")
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
    
