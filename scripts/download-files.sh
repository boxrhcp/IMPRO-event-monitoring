#!/usr/bin/env python3
import urllib.request
import zipfile
import os

page = urllib.request.urlopen('http://data.gdeltproject.org/gdeltv2/lastupdate.txt')
content = page.read()
text = content.decode('utf-8')
gkg_url = text.split(' ')[-1]
filename = gkg_url.split('/')[-1].replace('\n','')
print(filename)
urllib.request.urlretrieve(gkg_url,filename)
with zipfile.ZipFile(filename, 'r') as zip_ref:
    zip_ref.extractall('/home/ubuntu/source')
os.remove(filename)

