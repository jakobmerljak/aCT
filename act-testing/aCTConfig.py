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
        return self.getList(nodes)[0]

if __name__ == '__main__':

    actconf=aCTConfig()
    while 1:
        actconf.parse()
        for (i,j) in zip(actconf.getList(['srm','token','name']),actconf.getList(['srm','token','dir'])):
            print i,j
        exit(1)
        time.sleep(1)
    
