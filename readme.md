
# Athena history

This is a project to get athena query history and save it to s3 <br />

The cutoff date in config can be in formats e.g. "2 months ago" or "2019-05-20" or "" or "yesterday" . Empty string means taking all the history 

cutoff_date is the "from date" exclusive. up_to_date is the "to date" by default i.e. empty string in config is today exclusive.

output_type can be either "csv" or "json". By default is "json"

Note: Starting on December 1, 2017, Athena retains query history for 45 days. So you might see there're queries before 2017-12-1 if you try to run all dates i.e. putting empty string in config. Reference: https://docs.aws.amazon.com/athena/latest/ug/queries-viewing-history.html

## Install required packages

pip install -r requirements.txt


## Setup environment variable 

setup .env (you can rename the .env.sample to .env and fill the api username and password )  <br />

setup config.json


## Run the app 

in root directory, change dir to app 

```cd app```

inside app/ as working directory

```python run.py```
