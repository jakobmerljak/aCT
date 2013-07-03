import os
import aCTConfig
import getpass
import time
import subprocess

class aCTProxy:
    def __init__(self,Interval=3600):
        self.conf=aCTConfig.aCTConfig()
        self.cmd=self.conf.get(["voms","bindir"])+"/voms-proxy-init -rfc -voms atlas:/atlas/Role=production -pwstdin -valid "+self.conf.get(["voms","proxylifetime"])
        self.passwd=getpass.getpass("VOMS Password: ")
        self.__vomsinit__()
        self.tstamp=time.time()
        self.interval=Interval
        

    def __vomsinit__(self):
        p = subprocess.Popen(self.cmd, shell=True,stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
        (fi,fo,fe)=(p.stdin, p.stdout, p.stderr)
        fi.writelines(self.passwd+"\n")
        out=fo.readlines()
        eout=fe.readlines()
        p.wait()
        fi.close()
        fo.close()
        fe.close()
        if len(out) == 0:
            raise Exception("VOMS proxy not initialized")

    def renew(self):
        t=time.time()
        if t-self.tstamp<self.interval:
            return
        self.tstamp=t
        if self.timeleft() <= int(self.conf.get(["voms","minlifetime"])) :
            self.__vomsinit__()
            if self.timeleft() == 0:
                raise Exception("VOMS proxy not extended")

    def timeleft(self):
        cmd=self.conf.get(["voms","bindir"])+"/voms-proxy-info -timeleft"
        p = subprocess.Popen(cmd,shell=True,stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
        (fi,fo,fe)=(p.stdin, p.stdout, p.stderr)
        out=fo.readlines()
        if len(out) == 0:
            tleft=0
        else:
            tleft=out[0][:-1]
        fi.close()
        fo.close()
        fe.close()
        return int(tleft)

    def path(self):
        cmd=self.conf.get(["voms","bindir"])+"/voms-proxy-info -path"
        p = subprocess.Popen(cmd,shell=True,stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
        (fi,fo,fe)=(p.stdin, p.stdout, p.stderr)
        out=fo.readlines()
        path=out[0][:-1]
        fi.close()
        fo.close()
        fe.close()
        return path

if __name__ == '__main__':
    p=aCTProxy(1)
    print p.path()
    time.sleep(30)
    p.renew()

