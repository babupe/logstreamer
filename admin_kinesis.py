#!/usr/bin/python
import sys,getopt
from generic_functions import *
import subprocess
import ast

def show_usage(exit_code):
    print 'Usage: ./admin_kinesis.py -c <Path to S3 config file>'
    sys.exit(exit_code)

def validate_args(argv):
    if len(argv) == 0:
        show_usage(1)
    try:
        opts,args = getopt.getopt(argv,"hs:c:r:z:n:",["help","stream_name=","create_if_not_exists=","records_per_sec=","record_size=","no_Of_Consumers="])
    except getopt.GetoptError:
        show_usage(1)
    input_params = {}
    param_count = 0
    for opt,arg in opts:
        if opt == '-h':
            show_usage(1)
        elif opt in ("-s","--stream_name"):
            input_params['stream_name'] = arg
            param_count += 1 
        elif opt in ("-c","--create_if_not_exists"):
            input_params['create_if_not_exists'] = arg
            param_count += 1 
        elif opt in ("-r","--records_per_sec"):
            input_params['records_per_sec'] = arg
            param_count += 1 
        elif opt in ("-z","--record_size"):
            record_size_limit = return_value_from_dynamodb(dynamodb_conn,'KinesisLimits','RecordSize')['LimitValue']
            if int(arg) > int(record_size_limit):
                logger.info('Record size cannot exceed '+str(record_size_limit)+'. Please try after compressing your record input. Quitting now!!!')
                sys.exit(1)
            input_params['record_size'] = arg
            param_count += 1
        elif opt in ("-n","--no_Of_Consumers"):
            input_params['no_Of_Consumers'] = arg
            param_count += 1

    if param_count != 5:
        show_usage(1)
    return input_params

def returnDictOutput(command):
	proc = subprocess.Popen([command], stdout=subprocess.PIPE, shell=True)
	(out, err) = proc.communicate()
	return ast.literal_eval(out)

def getMetricStats(namespace,metric_name,statistic_name,dimension_name,dimension_value,period,start_time,end_time):
	command = "aws cloudwatch get-metric-statistics --metric-name " + metric_name + " --start-time " +start_time + " --end-time " +end_time+ " --period " +period+ " --namespace AWS/" +namespace+ " --statistics " +statistic_name+ " --dimensions Name=" +dimension_name+ ",Value=" +dimension_value
	statDict =  returnDictOutput(command)
	if len(statDict['Datapoints']) == 0:
		return 0
	else:
		return statDict

def getConfigFromS3():

def main():
	global logger
    logger = logging.getLogger('admin_kinesis')
    logger.setLevel(logging.INFO)
 
    # create the logging file handler
    fh = logging.FileHandler("/tmp/admin_kinesis.log")
 
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(formatter)
 
    # add handler to logger object
    logger.addHandler(fh)
    logger.addHandler(consoleHandler)

    metricData = getMetricStats('Kinesis','PutRecord.Bytes','Sum','StreamName','abc','300','2014-12-21T22:50:00','2014-12-21T23:50:00')
	if metricData == 0:
		print "No data put into the stream for the past 1 hour"
	else:
		print metricData['Datapoints']

if __name__ == "__main__":
	main()