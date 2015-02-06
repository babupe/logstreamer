#!/usr/bin/python
import sys,getopt
from generic_functions import *
import logging
import threading
import time

def main():
    global logger
    logger = logging.getLogger('copyToDynamo')
    logger.setLevel(logging.INFO)
 
    # create the logging file handler
    fh = logging.FileHandler("/tmp/copyToDynamo.log")
 
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

    while 1:
      quitLoop = 0
      awscreds = getInstanceCredentials()
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
      if sqs_conn.get_queue(configValues['emrOutputqueue']):
        keystoProcess = getKeystoProcess(sqs_conn,str(configValues['emrOutputqueue']))
        if len(keystoProcess) == 0:
            logger.info('No Input files to process! Quitting to retry during next run!!!')
        else:
            logger.info('We have Input files to be processed!!')
            fileList = getFileList(keystoProcess,configValues['outputS3Bucket'])
