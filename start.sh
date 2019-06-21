#!/bin/bash

cd ./app
python run.py

echo "Waiting for logs to flush to CloudWatch Logs..."
sleep 10  # twice the `buffer_duration` default of 5 seconds
