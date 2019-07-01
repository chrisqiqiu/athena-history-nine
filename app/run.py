# from dotenv import load_dotenv
import os
from s3 import S3
from lib.log import setup_logger
from lib.notification import SlackNotification
from config import Config
from functools import partial
from itertools import chain
import datetime
from awsretry import AWSRetry
import boto3
import csv
from multiprocessing.dummy import Pool as ThreadPool
import json

# load_dotenv()
logger = setup_logger(__name__)
slackBot = SlackNotification(__name__)

# cutoff_date is the "from date" exclusive. up_to_date is the "to date" by default i.e. empty string in config is today exclusive.


def main():
    client = boto3.client('athena', region_name='ap-southeast-2')
    logger.info("Read config file for cut-off date... ")
    # os.environ['CONTROLCONFIGPATH'] export "../configs/prod.json" to os.environ['CONTROLCONFIGPATH']
    data = Config(
        os.environ['CONTROLCONFIGPATH']).data

    cutoff_date = data.get("cutoff_date")
    now = datetime.datetime.now()

    # cutoff date can be in formats e.g. "2 months ago" or "2019-05-20" or "" or "yesterday". empty string means taking all the history
    if cutoff_date and " " in cutoff_date:
        cutoff_date = (datetime.date.today(
        ) - datetime.timedelta(int(cutoff_date.split(" ")[0])*365/12)).strftime('%Y-%m-%d')
        logger.info(f"cutoff_date is {cutoff_date}")
    elif cutoff_date and cutoff_date == "yesterday":
        cutoff_date = (now - datetime.timedelta(2)).strftime('%Y-%m-%d')

    get_each_execution_with_client = partial(get_each_execution, client)

    get_each_batch_execution_with_client = partial(
        get_each_batch_execution, client)

    query_execution_ids = get_all_execution_ids(client)

    logger.info(f"Total executions id are {len(query_execution_ids)} ")
    logger.info(
        f"Executions id ranging from {get_each_execution_with_client(query_execution_ids[-1])['SubmissionDateTime']} to {get_each_execution_with_client(query_execution_ids[0])['SubmissionDateTime']}")

    if cutoff_date:
        max_query_ids_index = binary_search_index_of_query_submission_date(get_each_execution_with_client,
                                                                           query_execution_ids, cutoff_date)

        query_execution_ids = query_execution_ids[:max_query_ids_index+1]

    query_ids_chunks = get_query_ids_in_chunks(query_execution_ids, 50)

    loop_start = datetime.datetime.now()
    pool = ThreadPool(4)
    final_list_of_dict_in_chunks = pool.map(
        get_each_batch_execution_with_client, query_ids_chunks)
    pool.close()
    pool.join()
    loop_end = datetime.datetime.now()

    def hours_minutes(td):
        return td.seconds//3600, (td.seconds//60) % 60

    hours, minutes = hours_minutes(loop_end-loop_start)

    logger.info(
        f"Time used to get all data is {str(hours) } hours, {str(minutes)} minutes")

    final_list_of_dict = list(
        chain.from_iterable(final_list_of_dict_in_chunks))

    # exclude cut-off date and today's date
    if data.get("up_to_date"):
        up_to_date = datetime.datetime.strptime(
            data.get("up_to_date"), '%Y-%m-%d')
    else:
        up_to_date = now

    final_list_of_dict = list(filter(lambda d: datetime.datetime.strptime(
        d["SubmissionDateTime"], '%Y-%m-%d %H:%M:%S') >= datetime.datetime.strptime(cutoff_date, '%Y-%m-%d').replace(hour=0, minute=0, second=0, microsecond=0)+datetime.timedelta(1) and datetime.datetime.strptime(
        d["SubmissionDateTime"], '%Y-%m-%d %H:%M:%S') < up_to_date.replace(hour=0, minute=0, second=0, microsecond=0), final_list_of_dict))

    logger.info(
        f"Final queries dates ranging from {final_list_of_dict[-1]['SubmissionDateTime']} to {final_list_of_dict[0]['SubmissionDateTime']}")

    s3 = S3(data.get("dest_s3_bucket"), prefix=data.get("dest_s3_prefix"))

    date_folder_on_s3 = now-datetime.timedelta(1)

    if data.get("output_type") == "json":
        out_file = write_to_json(final_list_of_dict, "athena_history")
        s3.put(out_file, f"{str(date_folder_on_s3.year)}/{str(date_folder_on_s3.month).zfill(2)}/{str(date_folder_on_s3.day)}/athena_history_{str(date_folder_on_s3.year)}_{str(date_folder_on_s3.month).zfill(2)}_{str(date_folder_on_s3.day)}.json")
    else:
        out_file = write_to_csv(final_list_of_dict, "athena_history")
        s3.put(out_file, f"{str(date_folder_on_s3.year)}/{str(date_folder_on_s3.month).zfill(2)}/{str(date_folder_on_s3.day)}/athena_history_{str(date_folder_on_s3.year)}_{str(date_folder_on_s3.month).zfill(2)}_{str(date_folder_on_s3.day)}.csv")


def write_to_csv(final_list_of_dict, outfile):
    with open(outfile, 'w',   newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(final_list_of_dict[0].keys()))
        w.writeheader()
        w.writerows(final_list_of_dict)
    return outfile


def write_to_json(final_list_of_dict, outfile):
    with open(outfile, 'a', newline='') as f:
        for idx, dic in enumerate(final_list_of_dict):
            json.dump(dic, f)
            if idx != len(final_list_of_dict)-1:
                f.write("\n")
    return outfile


def get_query_ids_in_chunks(query_ids, chunk_size):
    return [query_ids[i:i+chunk_size]
            for i in range(0, len(query_ids), chunk_size)]


@AWSRetry.backoff(tries=3, delay=3, added_exceptions=["ThrottlingException"])
def get_each_batch_execution(client, ids_chunk):
    resp = client.batch_get_query_execution(QueryExecutionIds=ids_chunk)
    # print(resp["QueryExecutions"][0]["Status"]["SubmissionDateTime"].strftime('%Y-%m-%d %H:%M:%S'))

    executions = resp["QueryExecutions"]
    return [{
        "QueryExecutionId": execution["QueryExecutionId"],
        "Query": execution.get("Query"),
        "StatementType": execution.get("StatementType"),
        "ResultConfiguration": str(execution.get("ResultConfiguration")),
        "QueryExecutionContext": str(execution.get("QueryExecutionContext")),
        "State":   execution["Status"].get("State"),
        "StateChangeReason":   execution["Status"].get("StateChangeReason"),
        # "SubmissionDateTime": execution["Status"]["SubmissionDateTime"],
        "SubmissionDateTime": execution["Status"]["SubmissionDateTime"].strftime('%Y-%m-%d %H:%M:%S'),
        "CompletionDateTime":   execution["Status"].get("CompletionDateTime").strftime('%Y-%m-%d %H:%M:%S'),
        "EngineExecutionTimeInMillis": execution.get("Statistics").get("EngineExecutionTimeInMillis"),
        "DataScannedInBytes": execution.get("Statistics").get("DataScannedInBytes"),
        "WorkGroup": execution.get("WorkGroup")
    } for execution in executions]


@AWSRetry.backoff(tries=3, delay=3, added_exceptions=["ThrottlingException"])
def get_each_execution(client, id):
    resp = client.get_query_execution(QueryExecutionId=id)
    # print(resp["QueryExecution"]["Status"]["SubmissionDateTime"].strftime('%Y-%m-%d %H:%M:%S'))
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

    logger.info(f"Searching {submission_date}")

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

    logger.info(
        f"Date you specified not found. Returning the index of nearest date {get_each_execution_with_client(query_ids[left-1])['SubmissionDateTime']}")
    return left-1


def get_all_execution_ids(client):
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

        logger.info(f"Processed pages {str(no_of_page)} to get execution ids")

    return query_execution_ids


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.exception("main() fail: " + str(e))
        slackBot.warn("main() fail: " + str(e))
