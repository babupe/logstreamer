#!/usr/bin/python
import boto.kinesis
import boto.utils
from boto.utils import get_instance_metadata
from boto.dynamodb.condition import *
import math
import time

def sleep(time_to_sleep):
    time.sleep(time_to_sleep)

def get_region():
    metadata = get_instance_metadata()
    return metadata['placement']['availability-zone'][:-1]

def conn_to_dynamodb():
    return boto.connect_dynamodb(get_region())

def return_value_from_dynamodb(dynamodb_conn,tablename,hashkey):
    KinesisLimits = dynamodb_conn.get_table(tablename)
    return KinesisLimits.get_item(hash_key=hashkey)

def conn_to_kinesis():
    return boto.kinesis.connect_to_region(get_region())

def calculate_shard_count(records_per_sec,record_size,dynamodb_conn):
    shardWriteParams = return_value_from_dynamodb(dynamodb_conn,'KinesisLimits','ShardWriteSize')
    ShardWriteSize = shardWriteParams['LimitValue']
    shardWriteUnit = shardWriteParams['LimitUnit']
    shardWriteVelocity = return_value_from_dynamodb(dynamodb_conn,'KinesisLimits','ShardWritePerSec')['LimitValue']
    maxNoOfShards = return_value_from_dynamodb(dynamodb_conn,'KinesisLimits','MaxNoOfShards')['LimitValue']
    shardCountUsingVelocity = int(math.ceil(int(records_per_sec)/int(shardWriteVelocity)))+1
    if shardCountUsingVelocity > int(maxNoOfShards):
        print 'Your current limit on Max number of shards is not enough to process the velocity of input!'
        print 'Proceeding with Stream creation with the max no of shards='+str(maxNoOfShards)+'!!!'
        return maxNoOfShards
    else:
        numberOfShardsVelocity = shardCountUsingVelocity
    writeSizeMB = math.ceil(int(records_per_sec)*int(record_size)/1024)
    if shardWriteUnit.upper() == 'MB':
        shardCountUsingSize = int(math.ceil(writeSizeMB/ShardWriteSize))+1
        if shardCountUsingSize > maxNoOfShards:
            print 'Your current limit on Max number of shards is not enough to process the size of input!'
            print 'Proceeding with Stream creation with the max no of shards='+str(maxNoOfShards)+'!!!'
            return maxNoOfShards
        else:
            numberOfShardsSize = shardCountUsingSize
    return max(numberOfShardsVelocity,numberOfShardsSize)

def get_stream_status(stream_name,kinesis_conn):
    return str(kinesis_conn.describe_stream(stream_name)['StreamDescription']['StreamStatus']).upper()

def check_stream_active(stream_name,kinesis_conn):
    while get_stream_status(stream_name,kinesis_conn) != 'ACTIVE':
        print 'Stream not yet Active.. Sleeping!!'
        sleep(5)
    if get_stream_status(stream_name,kinesis_conn) == 'ACTIVE':
        print 'Stream '+stream_name+' created and is Active!!'
    else:
        print 'Issue with creating the Stream '+stream_name+'. Please retry later!!!'
        sys.exit(1)

def create_stream(stream_name,kinesis_conn,records_per_sec,record_size,dynamodb_conn):
    shard_count = calculate_shard_count(records_per_sec,record_size,dynamodb_conn)
    try:
        print 'Creating Stream '+stream_name+' with '+str(shard_count)+' shard(s) now!'
        kinesis_conn.create_stream(stream_name,shard_count)
        print 'Submitted Stream Creation.. Will check back again to make sure it is created!'
        sleep(5)
        check_stream_active(stream_name,kinesis_conn)
    except:
        print 'Error while trying to create your Stream. Please retry later!!!'
        sys.exit(1)

def validate_stream(stream_name,create_if_not_exists,records_per_sec,record_size,kinesis_conn,dynamodb_conn):
    try:
        desc_response_json = kinesis_conn.describe_stream(stream_name)
    except boto.kinesis.exceptions.ResourceNotFoundException:
        if create_if_not_exists.upper() == 'Y':
            print 'Stream '+stream_name+' does not exist!!'
            create_stream(stream_name,kinesis_conn,records_per_sec,record_size,dynamodb_conn)
        else:
            print 'Stream '+stream_name+' not found. Quitting program'
            sys.exit(1)
    check_stream_active(stream_name,kinesis_conn)