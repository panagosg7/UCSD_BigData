#!/usr/bin/python
"""
count the number of measurement for each year
"""
import sys
sys.path.append('/usr/lib/python2.6/dist-packages')
from mrjob.job import MRJob
import re
from sys import stderr

class MRWeight(MRJob):

    def mapper(self, _, line):
        try:
            self.increment_counter('MrJob Counters','mapper-all',1)
            elements=line.split(',')             
   
            if elements[0]!='station':
                out = (elements[0],1)
            else:
                out = None
                
#                         try:
#             self.increment_counter('MrJob Counters','mapper-all',1)
#             elements=line.split(',')
   
#             if elements[0]!='station':
                
#                 if elements[1] in ['TMIN','TMAX','PRCP']:
                    
#                     emp = elements.count('')
#                     tot = len(elements) - 3
                    
#                     # more than 70% full
#                     if float(emp)/tot < 0.3:
#                         yield (elements[0],1)
                    
                     
                
                     
        except Exception, e:
            stderr.write('Error in line:\n'+line)
            stderr.write(e)
            self.increment_counter('MrJob Counters','mapper-error',1)
            out = None

        finally:
            if out != None:
                pass
            #yield out
        
    def combiner(self, word, counts):
        self.increment_counter('MrJob Counters','combiner',1)
        yield (word, sum(counts))

    def reducer(self, word, counts):
        self.increment_counter('MrJob Counters','reducer',1)
        yield (word, sum(counts))      


if __name__ == '__main__':
    MRWeight.run()