import signal

class ExceptInterrupt(Exception):
    def __init__(self,signal):
        self.signal=signal
    def __call__(self):
        print("called")
    def __str__(self):
        return str("Interrupt Exception: signal %d" % self.signal)


def SignalHandler(signum,frame):
    raise ExceptInterrupt(signum)

signal.signal(signal.SIGINT,SignalHandler)
signal.signal(signal.SIGTERM,SignalHandler)

if __name__ == '__main__':

    try:
        import time
        #time.sleep(20)
        try:
            raise Exception("low except")
        except:
            print("b")
            time.sleep(10)
            pass

    except ExceptInterrupt as x:
        print(x)

    print("A")
