import arc
import inspect
import re

class aCTJob:

    def __init__(self):
        self.attrmap={}
        self.attrmap[int]='integer'
        self.attrmap[str]='varchar(255)'
        self.attrmap[arc.JobState]='varchar(255)'
        self.attrmap[arc.StringList]='text'
        self.attrmap[arc.URL]='text'
        self.attrmap[arc.Period]='int'
        self.attrmap[arc.Time]='datetime'
        self.attrmap[arc.StringStringMap]='text'
        self.attrs={}
        for k in self.attrmap.keys():
            self.attrs[k]=[]
        j=arc.Job()


        ignoremems=['STDIN',
                    'STDOUT',
                    'STDERR',
                    'STAGEINDIR',
                    'STAGEOUTDIR',
                    'SESSIONDIR',
                    'JOBLOG',
                    'JOBDESCRIPTION']

        for i in dir(j):
            f=getattr(j,i)
            for k in self.attrmap.keys():
                if re.match('^__',i):
                    continue
                if i in ignoremems:
                    continue
                if isinstance(f,k):
                    self.attrs[k].append(i)
            
    def DbColumnsCreate(self):
        s=""
        for k in self.attrs.keys():
            for i in self.attrs[k]:
                s+= i+" "+self.attrmap[k]+","
        return s


if __name__ == '__main__':
    j=aCTJob()
    print j.DbColumnsCreate()
