#!/usr/bin/python
from datetime import datetime
from random import randint
import os
swrevision = ['1.2.3','2.3.4','3.4.5','4.5.6','5.6.7','6.7.8']
logtype = ['crash','notif','download']
devinfo = ['iphone-ios','android-kitkat','fire-fireos']
appnm = ['app1','app2','app3','app4']
mtime = os.stat('/home/ec2-user/gen_data.py')[8]
while 1:
	if mtime != os.stat('/home/ec2-user/gen_data.py')[8]:
		try:
			f.close()
		except:
			print 'None'
		sys.exit(0)
	currTime = str(datetime.now())
	fname = "/fs-data/applogs-"+currTime[:4]+currTime[5:7]+currTime[8:10]+currTime[11:13]+currTime[14:16]+".log"
	f = open(fname,'a')
	while str(currTime[14:16]) == str(datetime.now())[14:16]:
		currTm = str(datetime.now())
		eventTmstp = currTime[:4]+'-'+currTime[5:7]+'-'+currTime[8:10]+' '+currTime[11:13]+':'+currTime[14:16]+':'+currTime[17:19]
		f.write("AppName:"+appnm[randint(0,len(appnm)-1)]+",DeviceInfo:"+devinfo[randint(0,len(devinfo)-1)]+",logType:"+logtype[randint(0,len(logtype)-1)]+",SWRevision:"+swrevision[randint(0,len(swrevision)-1)]+",eventTimestamp:"+eventTmstp+"\n")
	f.close()
