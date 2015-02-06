#!/usr/bin/python
import boto
import os
from boto.s3.key import Key
from boto.utils import get_instance_metadata
from datetime import datetime
import time

def get_region():
    metadata = get_instance_metadata()
    return metadata['placement']['availability-zone'][:-1]


s3_conn = boto.connect_s3(get_region())
buckt = s3_conn.get_bucket('myapplogsbabupe') 
mtime = os.stat('/home/ec2-user/write_to_s3.py')[8]
while 1:
	if mtime != os.stat('/home/ec2-user/write_to_s3.py')[8]:
		try:
			f.close()
		except:
			print 'None'
		sys.exit(0)
	currTime = str(datetime.now())
	mincomp = int(currTime[14:16]) -1
	if len(str(mincomp)) == 1:
		mincomp = '0'+str(mincomp)
	if os.path.isfile('/fs-data/applogs-'+currTime[:4]+currTime[5:7]+currTime[8:10]+currTime[11:13]+str(mincomp)+'.log'):
		k = Key(buckt)
		k.key = 'applogs/'+currTime[:4]+'/'+currTime[5:7]+'/'+currTime[8:10]+'/'+currTime[11:13]+'/'+str(mincomp)+'.log'
		k.set_contents_from_filename('/fs-data/applogs-'+currTime[:4]+currTime[5:7]+currTime[8:10]+currTime[11:13]+str(mincomp)+'.log')	
		os.remove('/fs-data/applogs-'+currTime[:4]+currTime[5:7]+currTime[8:10]+currTime[11:13]+str(mincomp)+'.log')
	time.sleep(10)
