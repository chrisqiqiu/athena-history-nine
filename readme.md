
# Athena history

This is a project to get athena query history and save it to s3 <br />

The cutoff date in config can be in formats e.g. "2 months ago" or "2019-05-20" or "" . Empty string means taking all the history 

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
