#!/usr/bin/env python
import sys
import re

def main(argv):
  logLine = sys.stdin.readline()
  try:
    while logLine:
      logLine = logLine.strip().split(',')
      for logComp in logLine:
        if str(logComp.split(':')[0]) == 'SWRevision':
          swRevision = str(logComp.split(':')[1])
        if str(logComp.split(':')[0]) == 'logType':
          logType = str(logComp.split(':')[1])
        if str(logComp.split(':')[0]) == 'eventTimestamp':
          datePart = str(logComp.split(':')[1])+':'+str(logComp.split(':')[2])
          minComp = str(int(datePart[14:16])/10*10)
          if len(minComp) == 1:
            minComp = '0'+ minComp
          eventTimestamp = ''.join(re.findall('\d+',datePart[:13]))+minComp
      print swRevision+","+logType+","+eventTimestamp + "\t" + "1"
      logLine = sys.stdin.readline()
  except "end of file":
    return None
if __name__ == "__main__":
  main(sys.argv)