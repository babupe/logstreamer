#!/usr/bin/python
import sys,getopt
from generic_functions import *
import logging


def show_usage(exit_code):
    print 'Usage: ./create_stream.py -s <Stream Name> -c <create_if_not_exists Y or N> -r <Records per second> -z <Size of record> -n <No of Consumers>'
    sys.exit(exit_code)

def validate_args(argv,dynamodb_conn):
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

def main():
    global logger
    logger = logging.getLogger('createStream')
    logger.setLevel(logging.INFO)
 
    # create the logging file handler
    fh = logging.FileHandler("/tmp/create_stream.log")
 
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(formatter)
 
    # add handler to logger object
    logger.addHandler(fh)
    logger.addHandler(consoleHandler)
    kinesis_conn = conn_to_kinesis()
    dynamodb_conn = conn_to_dynamodb()
    input_params = validate_args(sys.argv[1:],dynamodb_conn)
    validate_stream(input_params['stream_name'],input_params['create_if_not_exists'],input_params['records_per_sec'],input_params['record_size'],input_params['no_Of_Consumers'],kinesis_conn,dynamodb_conn)

if __name__ == "__main__":
    main()