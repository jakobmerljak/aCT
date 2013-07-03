import time

def sleep(t):
    for i in range(0,t):
        time.sleep(1)

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
