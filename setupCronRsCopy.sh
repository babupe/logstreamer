#!/bin/bash
echo "*/5 * * * * ./configManager.py -d logstreamerconfig -q logstreamerconfig" > /tmp/rscopycrontab
echo "00 * * * * ./copyToRS.py -d logstreamerconfig" >> /tmp/rscopycrontab
crontab /tmp/rscopycrontab