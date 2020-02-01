#!/usr/bin/env python3
import sys
import urllib.request

with open(sys.argv[1]) as fp:
       cnt = 0
       for line in fp:
           filename = line.split('/')[-1].replace('\n','')
           print(filename)
           urllib.request.urlretrieve(line,filename)


