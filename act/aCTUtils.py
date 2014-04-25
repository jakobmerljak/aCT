import time
import os
import arc

def sleep(t):
    for i in range(0,t):
        time.sleep(1)

def setFilePermissionsRecursive(path, dirmod=0755, filemod=0644):
    for root,dirs,files in os.walk(path):
        for d in dirs:
            os.chmod(os.path.join(root,d), dirmod)
        for f in files:
            os.chmod(os.path.join(root,f), filemod)
    # set permissions for the path itself as well
    os.chmod(path, dirmod)
            
def RunThreadsSplit(plist,nthreads=1):
    it=0
    while it < len(plist):
        tl=[]
        for i in range(0,nthreads):
            try:
                t=plist[it]
                tl.append(t)
                t.start()
            except:
                pass
            it+=1
        for t in tl:
            t.join()

def datapointFromURL(url, uc):
    '''
    Replacement for arc.datapoint_from_url() which does not clean up DataPoints
    when python objects are destroyed, leading to connection leaking when used
    with gridftp. This method should be used instead of arc.datapoint_from_url().
    It can be called like (_, __, dp) = datapointFromURL('gsiftp://...', uc)
    where uc is an arc.UserConfig object.
    '''
    u = arc.URL(url)
    h = arc.DataHandle(u, uc)
    source = h.__ref__()
    return (h, u, source)
