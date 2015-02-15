#!/usr/bin/python
import sys,getopt
from generic_functions import *
import logging
import threading
import time
import os

def show_usage(exit_code):
    print 'Usage: ./setupJobs.py -d <DynamoDB table with config file name>'
    sys.exit(exit_code)

def validate_args(argv,dynamodb_conn):
    if len(argv) == 0:
        show_usage(1)
    try:
        opts,args = getopt.getopt(argv,"hd:",["help","ddb_table_name="])
    except getopt.GetoptError:
        show_usage(1)
    input_params = {}
    param_count = 0
    for opt,arg in opts:
        if opt == '-h':
            show_usage(1)
        elif opt in ("-d","--ddb_table_name"):
          check_ddb_tbl_exists(dynamodb_conn,arg,'N')
          input_params['ddb_table_name'] = arg
          param_count += 1 

    if param_count != 1:
        show_usage(1)
    return input_params

class trackStepClass(threading.Thread):
    def __init__(self, target, *args):
        self._target = target
        self._args = args
        threading.Thread.__init__(self)
 
    def run(self):
        self._target(*self._args)

def main():
  if int(os.popen('ps -ef|grep setupJobs.py|grep -v grep|wc -l').read()) == 1:
    global logger
    logger = logging.getLogger('setupJobs')
    logger.setLevel(logging.INFO)
 
    # create the logging file handler
    fh = logging.FileHandler("/tmp/setupJobs.log")
 
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(formatter)
 
    # add handler to logger object
    logger.addHandler(fh)
    logger.addHandler(consoleHandler)
    dynamodb_conn = conn_to_dynamodb()
    sqs_conn = conn_to_sqs()
    s3_conn = conn_to_s3()
    emr_conn = conn_to_emr()
    rs_conn = conn_to_rs()
    input_params = validate_args(sys.argv[1:],dynamodb_conn)
    quitLoop = 0
    while quitLoop == 0:
      try:
        currItem = return_value_from_dynamodb(dynamodb_conn,input_params['ddb_table_name'],1)
        quitLoop = 1
      except boto.dynamodb.exceptions.DynamoDBKeyNotFoundError:
        logger.warn('No Config File found! Publish the config file in your S3 Config directory!!')
        sleep(30)
    currConfigFile = currItem['current_version_file']
    logger.info('Config to use '+currConfigFile)
    configValues=getS3KeyContents(s3_conn,currConfigFile)
    logger.info('Config values extracted!!')
    mtime = os.stat('/home/ec2-user/setupJobs.py')[8]
    while 1:
    	if mtime != os.stat('/home/ec2-user/setupJobs.py')[8]:
		try:
			trackCurrentStep.join()
		except:
			sys.exit(0)
		sys.exit(0)
    	try:
      		jobflowId = return_value_from_dynamodb(dynamodb_conn,configValues['ddbTableNameForState'],1)
      		if check_emr_jobflow(str(jobflowId['jobflowid'])) not in ('RUNNING','WAITING','STARTING','BOOTSTRAPPING') and getRedshiftClusterState(rs_conn,configValues['rsClusterId']).upper() != 'AVAILABLE':
        		logger.error('Either Redshift cluster or EMR Job is not yet up. Will retry again after logstreamer does its work!!!')
        		sys.exit(1)
      		else:
        		check_cluster_running(emr_conn,str(jobflowId['jobflowid']))
    	except boto.dynamodb.exceptions.DynamoDBKeyNotFoundError:
      		logger.error('No JobFlow available to add steps! Quitting!!!')
      		sys.exit(1)
    	logger.info('Getting input files to add as steps to EMR JobFLow!!!')
    	keystoProcess = getKeystoProcess(sqs_conn,str(configValues['sqsQueueName']))
    	if len(keystoProcess) == 0:
      		logger.info('No Input files to process! Quitting to retry during next run!!!')
    	else:
      		logger.info('We have Input files to be processed!!')
      		fileList = getFileList(keystoProcess,configValues['s3BuckettoStream'])
      		outputDirectory = getOutputDirectory()
      		stepName = ''.join(outputDirectory.split('/'))
      		outputDirectory = 's3://'+str(configValues['outputS3Bucket'])+outputDirectory
      		mapperKey = configValues['mapperS3Location'].split('/')[-1]
      		logger.info('Creating new Streaming step with input files!')
      		step = createNewStreamingStep(stepName,fileList,outputDirectory,mapperKey,configValues['mapperS3Location'],configValues['reducerS3Location'])
      		addSteptoJobFlow(emr_conn,str(jobflowId['jobflowid']),step)
      		stepid = getCurrentStep(emr_conn,str(jobflowId['jobflowid']))
      		trackCurrentStep = trackStepClass(trackStep,sqs_conn,keystoProcess,emr_conn,str(jobflowId['jobflowid']),stepid)
      		trackCurrentStep.daemon = True
      		trackCurrentStep.start()
      		logger.info('Creating the job for Redshift to copy!!')
      		createRsJob(s3_conn,configValues['S3ManifestBucket'],fileList,sqs_conn,configValues['rsJobQueueName'])
      		logger.info('Added new Streaming step to JobFlow '+str(jobflowId['jobflowid'])+' and added the job for redshift to process!')
	sleep(180)
if __name__ == "__main__":
  main()
