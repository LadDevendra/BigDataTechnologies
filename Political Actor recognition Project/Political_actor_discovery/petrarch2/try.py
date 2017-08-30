'''
Created on May 6, 2017

@author: devendralad
'''

from wiki import wikidata

ww = wikidata()

aa = ww.getProbability("Salman Khan")
print str(aa)
    