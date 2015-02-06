#!/usr/bin/env python

from operator import itemgetter
import sys
import boto
from boto.utils import get_instance_metadata
from boto.dynamodb.condition import *
from boto.dynamodb2.table import Table

current_word = None
current_count = 0
word = None

def get_region():
    metadata = get_instance_metadata()
    return metadata['placement']['availability-zone'][:-1]

def conn_to_dynamodb():
    return boto.connect_dynamodb(get_region())

def write_to_dynamodb(current_word,current_count):
  swrevision = current_word.split(',')[0]
  logType = current_word.split(',')[1]
  aggregDate = int(current_word.split(',')[2])
  try:
    currItem = table.get_item(swrevidLogtype = swrevision+'|'+logType,aggdate = aggregDate)
    currItem['count'] = currItem['count'] + current_count
    currItem.save(overwrite=True)
  except boto.dynamodb2.exceptions.ItemNotFound:
    try:
      table.put_item(data = {
        'swrevidLogtype' : swrevision+'|'+logType,
        'aggdate' : aggregDate,
        'count'   : current_count
        })
    except:
      return 1
  except:
    return 1
dynamoconn = conn_to_dynamodb()
table = Table('tblswrevgrouping')
# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    word, count = line.split('\t', 1)

    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_word == word:
        current_count += count
    else:
        if current_word:
            # write result to STDOUT
            print '%s\t%s' % (current_word, current_count)
            write_to_dynamodb(current_word,current_count)
        current_count = count
        current_word = word

# do not forget to output the last word if needed!
if current_word == word:
    print '%s\t%s' % (current_word, current_count)
    write_to_dynamodb(current_word,current_count)