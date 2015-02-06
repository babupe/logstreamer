#!/bin/bash
echo "*/3 * * * * ./gen_data.py" > /tmp/gendatacrontab
echo "*/1 * * * * ./write_to_s3.py" >> /tmp/gendatacrontab
crontab /tmp/gendatacrontab