#!/usr/bin/python
"""
count the number of measurement for each year
"""
import sys
sys.path.append('/usr/lib/python2.6/dist-packages')
from mrjob.job import MRJob
import re
from sys import stderr

cat='TMAX'

class MRWeather(MRJob):

    def mapper(self, _, line):
        try:
            self.increment_counter('MrJob Counters','mapper-all',1)
            elements=line.split(',')
            
            if elements[0]!='station' and elements[1]==cat:
                #out = (None, elements)
                #self.increment_counter('MrJob Counters for ' + cat, elements[0],1)
                out = (elements[0],1)
            else:
                out = None
                     
        except Exception, e:
            stderr.write('Error in line:\n'+line)
            stderr.write(e)
            self.increment_counter('MrJob Counters','mapper-error',1)
            out = None # ('error',(1,1))

        finally:
            if out != None:
                yield out
        
    def combiner(self, word, counts):
        self.increment_counter('MrJob Counters','combiner',1)
        yield (word, sum(counts))

    def reducer(self, word, counts):
        self.increment_counter('MrJob Counters','reducer',1)
        yield (word, sum(counts))
        
        

if __name__ == '__main__':
    MRWeather.run()