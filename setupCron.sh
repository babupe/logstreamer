#!/bin/bash
echo "*/1 * * * * ./logstreamer.py -d logstreamerconfig" > /tmp/crontab
echo "*/5 * * * * ./configManager.py -d logstreamerconfig -q logstreamerconfig" >> /tmp/crontab
echo "*/2 * * * * ./setupJobs.py -d logstreamerconfig" >> /tmp/crontab
crontab /tmp/crontab