#!/usr/bin/python
import sys
import re

def main(argv):
  logLine = sys.stdin.readline()
  try:
    while logLine:
      logLine = logLine.strip().split(',')
      for logComp in logLine:
        if str(logComp.split(':')[0]) == 'eventtimestamp':
          eventtimestamp = str(logComp.split(':')[1])
        if str(logComp.split(':')[0]) == 'eventtimestamp':
          eventtimestamp = str(logComp.split(':')[1])
        if str(logComp.split(':')[0]) == 'eventTimestamp':
          datePart = str(logComp.split(':')[1])+':'+str(logComp.split(':')[2])
          minComp = str(int(datePart[14:16])/10*10)
          if len(minComp) == 1:
            minComp = '0'+ minComp
          eventTimestamp = ''.join(re.findall('\d+',datePart[:13]))+minComp
      print eventtimestamp+','+eventtimestamp+','+eventTimestamp + "\t" + "1"
      logLine = sys.stdin.readline()
  except "end of file":
    return None
if __name__ == "__main__":
  main(sys.argv)

sudo yum -y install python-psycopg2
f.write("AppName:"+appnm[randint(0,len(appnm)-1)]+",eventtimestamp:"+devinfo[randint(0,len(devinfo)-1)]+",eventtimestamp:"+eventtimestamp[randint(0,len(eventtimestamp)-1)]+",eventtimestamp:"+eventtimestamp[randint(0,len(eventtimestamp)-1)]+",eventTimestamp:"+eventTmstp+"\n")  
import boto.emr
conn = boto.emr.connect_to_region('us-east-1')
from boto.emr.step import StreamingStep
args = ['-files','s3://elasticmapreduce/samples/wordcount/wordSplitter.py']
step = StreamingStep(name='My wordcount example',mapper='s3://elasticmapreduce/samples/wordcount/wordSplitter.py',reducer='aggregate',input='s3://elasticmapreduce/samples/wordcount/input', output='s3://babupeopresults1/newres22',step_args=args)
step = StreamingStep(name='My wordcount example',mapper='wordSplitter.py',reducer='aggregate',input='s3://elasticmapreduce/samples/wordcount/input', output='s3://babupeopresults1/newres22',step_args=args)
jobid = conn.run_jobflow(name='My jobflow',log_uri='s3://emrlogsbabupe/',steps=[],action_on_failure='CANCEL_AND_WAIT',master_instance_type='m1.large', slave_instance_type='m1.large',num_instances=2,ami_version='3.3.1',keep_alive=True,job_flow_role='AppRole',service_role='EMR_DefaultRole',ec2_keyname='MyFirstKeyPair',visible_to_all_users=True)
conn.set_termination_protection(jobid,True)
stp1 = conn.add_jobflow_steps('j-14UW2BQOWNXH6',[step])
conn.list_steps('j-14UW2BQOWNXH6').__dict__['steps'][0].__dict__['id']
conn.terminate_jobflow(jobid)

status = conn.describe_jobflow(jobid)
status.__dict__

substring(eventtimestamp,charindex(':',eventtimestamp)+1,length(eventtimestamp)-charindex(':',eventtimestamp))



