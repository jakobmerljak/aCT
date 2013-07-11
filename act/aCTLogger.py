import logging
import logging.handlers
import aCTConfig

import arc

LEVELS = {'debug': logging.DEBUG,
          'info': logging.INFO,
          'warning': logging.WARNING,
          'error': logging.ERROR,
          'critical': logging.CRITICAL}


class aCTLogger:

    def __init__(self,name):
        self.conf=aCTConfig.aCTConfig()
        self.logger=logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        level = LEVELS.get(self.conf.get(["logger","level"]), logging.NOTSET)
        self.logger.setLevel(level)
        self.handler=logging.handlers.RotatingFileHandler(
            self.conf.get(["logger",name]),
            maxBytes=int(self.conf.get(["logger","size"])),
            backupCount=int(self.conf.get(["logger","rotate"])))
        self.formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(filename)s:%(lineno)d:%(funcName)s - %(message)s")
        self.handler.setFormatter(self.formatter)
        self.logger.addHandler(self.handler)

        self.arclogfile = arc.LogFile(str(self.conf.get(["logger",name])))
        self.arclogfile.setFormat(arc.LongFormat)
        arc.Logger_getRootLogger().addDestination(self.arclogfile)
        if self.conf.get(["logger", "arclevel"]):
            arc.Logger_getRootLogger().setThreshold(arc.string_to_level(str(self.conf.get(["logger", "arclevel"])).upper()))
        else:
            arc.Logger_getRootLogger().setThreshold(arc.ERROR)

    def log(self,level,message,*args, **kwargs):
        lvl = LEVELS.get(level, logging.NOTSET)
        self.logger.log(lvl,message,*args, **kwargs)

    def write(self,s):
        if s == "\n":
            return
        self.logger.info(s)

    def __call__(self):
        return self.logger

if __name__ == '__main__':
    l=aCTLogger("downloader")
    #l.log("info","test")
    #for i in range(0,100000):
    #    l.log("error","error")
    #l.logger.info("test")
    l().info("test")
    import sys
    sys.stdout=l
    print "test print"
    print "test print1"
