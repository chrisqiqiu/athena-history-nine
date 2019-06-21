# from dotenv import load_dotenv
import os
from s3 import S3
from lib.log import setup_logger
from lib.notification import SlackNotification
from config import Config
import json
from functools import partial
from itertools import chain
import datetime
from awsretry import AWSRetry
import boto3

from multiprocessing.dummy import Pool as ThreadPool
from multiprocessing import Pool as ProcessPool

# load_dotenv()
logger = setup_logger(__name__)
slackBot = SlackNotification(__name__)


def main():
    client = boto3.client('athena', region_name='ap-southeast-2')
    logger.info("Read config file for cut-off date... ")
    # os.environ['CONTROLCONFIGPATH'] export "../configs/prod.json" to os.environ['CONTROLCONFIGPATH']
    cutoff_date = Config(
        os.environ['CONTROLCONFIGPATH']).data.get("cutoff_date")

    get_each_execution_with_client = partial(get_each_execution, client)

    get_each_batch_execution_with_client = partial(
        get_each_batch_execution, client)

    query_execution_ids = get_all_execution_ids()

    if cutoff_date:
        max_query_ids_index = binary_search_index_of_query_submission_date(get_each_execution_with_client,
                                                                           query_execution_ids, cutoff_date)

        query_execution_ids = query_execution_ids[:max_query_ids_index+1]

    query_ids_chunks = get_query_ids_in_chunks(query_execution_ids, 50)

    loop_start = datetime.datetime.now()
    pool = ThreadPool(4)
    final_list_of_list = pool.map(
        get_each_batch_execution_with_client, query_ids_chunks)
    pool.close()
    pool.join()
    loop_end = datetime.datetime.now()

    def days_hours_minutes(td):
        return td.days, td.seconds//3600, (td.seconds//60) % 60

    days, hours, minutes = days_hours_minutes(loop_end-loop_start)

    final_list_of_dict = list(chain.from_iterable(final_list_of_list))


def get_query_ids_in_chunks(query_ids, chunk_size):
    return [query_ids[i:i+chunk_size]
            for i in range(0, len(query_ids), chunk_size)]


def get_query_from_s3(queryBucket, queryKey):
    query_s3 = S3(bucket=queryBucket)
    query_s3.get(queryKey, "query.sql")
    with open("query.sql") as f:
        query = f.read()
    return query


@AWSRetry.backoff(tries=3, delay=3, added_exceptions=["ThrottlingException"])
def get_each_batch_execution(client, ids_chunk):
    resp = client.batch_get_query_execution(QueryExecutionIds=ids_chunk)
    print(resp["QueryExecutions"][0]["Status"]
          ["SubmissionDateTime"].strftime('%Y-%m-%d %H:%M:%S'))
    executions = resp["QueryExecutions"]
    return [{
        "QueryExecutionId": execution["QueryExecutionId"],
        "Query": execution.get("Query"),
        "StatementType": execution.get("StatementType"),
        "ResultConfiguration": str(execution.get("ResultConfiguration")),
        "QueryExecutionContext": str(execution.get("QueryExecutionContext")),
        "State":   execution["Status"].get("State"),
        "StateChangeReason":   execution["Status"].get("StateChangeReason"),
        "SubmissionDateTime": execution["Status"]["SubmissionDateTime"],
        "SubmissionDateTimeString": execution["Status"]["SubmissionDateTime"].strftime('%Y-%m-%d %H:%M:%S'),
        "CompletionDateTime":   execution["Status"].get("CompletionDateTime").strftime('%Y-%m-%d %H:%M:%S'),
        "EngineExecutionTimeInMillis": execution.get("Statistics").get("EngineExecutionTimeInMillis"),
        "DataScannedInBytes": execution.get("Statistics").get("DataScannedInBytes"),
        "WorkGroup": execution.get("WorkGroup")
    } for execution in executions]


@AWSRetry.backoff(tries=3, delay=3, added_exceptions=["ThrottlingException"])
def get_each_execution(client, id):
    resp = client.get_query_execution(QueryExecutionId=id)
    print(resp["QueryExecution"]["Status"]
          ["SubmissionDateTime"].strftime('%Y-%m-%d %H:%M:%S'))
    return {
        "QueryExecutionId": resp["QueryExecution"]["QueryExecutionId"],
        "Query": resp["QueryExecution"].get("Query"),
        "StatementType": resp["QueryExecution"].get("StatementType"),
        "ResultConfiguration": str(resp["QueryExecution"].get("ResultConfiguration")),
        "QueryExecutionContext": str(resp["QueryExecution"].get("QueryExecutionContext")),
        "State":   resp["QueryExecution"]["Status"].get("State"),
        "StateChangeReason":   resp["QueryExecution"]["Status"].get("StateChangeReason"),
        "SubmissionDateTime": resp["QueryExecution"]["Status"]["SubmissionDateTime"],
        "SubmissionDateTimeString": resp["QueryExecution"]["Status"]["SubmissionDateTime"].strftime('%Y-%m-%d %H:%M:%S'),
        "CompletionDateTime":   resp["QueryExecution"]["Status"].get("CompletionDateTime").strftime('%Y-%m-%d %H:%M:%S'),
        "EngineExecutionTimeInMillis": resp["QueryExecution"].get("Statistics").get("EngineExecutionTimeInMillis"),
        "DataScannedInBytes": resp["QueryExecution"].get("Statistics").get("DataScannedInBytes"),
        "WorkGroup": resp["QueryExecution"].get("WorkGroup")
    }


def binary_search_index_of_query_submission_date(get_each_execution_with_client, query_ids, submission_date):
    left = 0
    right = len(query_ids)-1

    submission_date_parsed = datetime.datetime.strptime(
        submission_date, "%Y-%m-%d").replace(tzinfo=None).replace(hour=0, minute=0, second=0, microsecond=0)

    while left <= right:
        midpoint = left + (right - left)//2
        midpoint_result_dict = get_each_execution_with_client(
            query_ids[midpoint])
        midpoint_date_parsed = midpoint_result_dict["SubmissionDateTime"].replace(
            tzinfo=None).replace(hour=0, minute=0, second=0, microsecond=0)
        if midpoint_date_parsed == submission_date_parsed:
            return midpoint
        else:
            # note the query id list is sorted in reversed order so if search date is less than midpoint date, it should be on right hand side
            if submission_date_parsed < midpoint_date_parsed:
                left = midpoint+1
            else:
                # if search date > midpoint date, it should be on left hand side of the list
                right = midpoint-1

    print("date you specified not found. Returning the nearest date's index")
    return left-1


def get_all_execution_ids():
    next_token = None
    no_of_page = 0
    query_execution_ids = []

    while True:
        paginator = client.get_paginator('list_query_executions')
        response_iterator = paginator.paginate(PaginationConfig={
            'MaxItems': 5000,
            'PageSize': 50,
            'StartingToken': next_token})

        for page in response_iterator:
            # print(page["QueryExecutionIds"])
            query_execution_ids.extend(page["QueryExecutionIds"])
            no_of_page = no_of_page + 1

        try:
            next_token = page["NextToken"]
        except KeyError:
            break

        print(no_of_page)
        return query_execution_ids


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.exception("main() fail: " + str(e))
        slackBot.warn("main() fail: " + str(e))
