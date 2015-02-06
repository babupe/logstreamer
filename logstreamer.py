#!/usr/bin/python
import sys,getopt
from generic_functions import *
import logging
import os

def show_usage(exit_code):
    print 'Usage: ./logstreamer.py -d <DynamoDB table with config file name>'
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

def main():
  if int(os.popen('ps -ef|grep logstreamer.py|grep -v grep|wc -l').read()) == 1:
    global logger
    logger = logging.getLogger('logstreamer')
    logger.setLevel(logging.INFO)
 
    # create the logging file handler
    fh = logging.FileHandler("/tmp/logstreamer.log")
 
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
    if configValues['rsClusterId'] != '':
      createRsCluster = 0
      try:
        if getRedshiftClusterState(rs_conn,configValues['rsClusterId']).upper() != 'AVAILABLE':
          createRsCluster = 1
        else:
          logger.info('Redshift cluster is already available!!!')
      except boto.redshift.exceptions.ClusterNotFound:
        createRsCluster = 1
      if createRsCluster == 1:
        logger.info('Creating Redshift cluster with Identifier '+configValues['rsClusterId']+'!!!')
        try:
          clusterEndPoint = creatersCluster(rs_conn,configValues['rsClusterId'],configValues['nodeType'],configValues['dbName'],configValues['dbPort'],configValues['masterUserName'],configValues['masterPassword'],configValues['vpcSecurityGroupIds'],configValues['clusterSubnetGroupName'],configValues['vpcSubnetId'],configValues['numberOfNodes'],configValues['encrypted'])
        except:
          logger.error('Redshift cluster creation failed!!!')
          sys.exit(1)
        logger.info('Validating presence of required tables!')
      else:
        clusterEndPoint = rs_conn.describe_clusters(cluster_identifier=configValues['rsClusterId'])['DescribeClustersResponse']['DescribeClustersResult']['Clusters'][0]['Endpoint']['Address']
      connString = "host='"+clusterEndPoint+"' dbname='"+configValues['dbName']+"' user='"+configValues['masterUserName']+"' password='"+configValues['masterPassword']+"' port="+configValues['dbPort']
      createTableIfNotExists(connString,configValues['tableName'],configValues['stagingTableName'],configValues['createTableNameStmt'],configValues['createStagingTableNameStmt'])
    try:
      jobflowId = return_value_from_dynamodb(dynamodb_conn,configValues['ddbTableNameForState'],1)
      if str(jobflowId['state']) not in ('RUNNING','WAITING','STARTING','BOOTSTRAPPING'):
        createNewJobFlow = 1
      else:
        createNewJobFlow = 0
    except boto.dynamodb.exceptions.DynamoDBKeyNotFoundError:
      createNewJobFlow = 1  
    if createNewJobFlow == 0:
      if check_emr_jobflow(str(jobflowId['jobflowid'])) not in ('RUNNING','WAITING','STARTING','BOOTSTRAPPING'):
        createNewJobFlow = 1
      else:
        logger.info('Streaming job with jobid '+str(jobflowId['jobflowid'])+' already running') 
        logger.info('Checking for parameter changes in config now!')
        if int(jobflowId['numinstances']) < int(configValues['numInstances']):
          logger.warn('Request for resize instances to '+str(configValues['numInstances']))
          increaseInstances(emr_conn,str(jobflowId['jobflowid']),str(configValues['numInstances']),dynamodb_conn,configValues['ddbTableNameForState'])
          logger.info('Number of Instances increased to '+str(jobflowId['jobflowid'])+'!!!')
          change = 1
        else:
          change = 0
        if str(jobflowId['terminationprotect']) != str(configValues['terminationProtect']):
          logger.warn('Toggling Termination protection to '+str(configValues['terminationProtect'])+' on config change request!!')
          toggleTermProtect(emr_conn,str(jobflowId['jobflowid']),configValues['terminationProtect'],dynamodb_conn,configValues['ddbTableNameForState'])
          logger.info('Termination protection set to '+str(configValues['terminationProtect'])+'!!!')
          change = 1
        else:
          change = 0
        if change == 0:
          logger.info('No changes requested in config!!!')
    if createNewJobFlow == 1:
      logger.info('No Streaming job running. Starting one now!!!')
      strmJobRetVal = createNewStreamingJob(dynamodb_conn,configValues)   
      if strmJobRetVal == 1:
        logger.error('Error writing to DynamoDB table '+configValues['ddbTableNameForState']+'. Quitting!')
        sys.exit(1)  
      if strmJobRetVal == 2:
        logger.error('Error creating streaming job '+configValues['jobflowName']+'. Quitting!')
        sys.exit(1)
if __name__ == "__main__":
  main()