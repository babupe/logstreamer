#!/bin/bash
echo "*/1 * * * * ./logstreamer.py -d logstreamerconfig" > /tmp/streamercrontab
echo "*/5 * * * * ./configManager.py -d logstreamerconfig -q logstreamerconfig" >> /tmp/streamercrontab
echo "*/2 * * * * ./setupJobs.py -d logstreamerconfig" >> /tmp/streamercrontab
crontab /tmp/streamercrontab