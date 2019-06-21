import threading
import boto3
import sys
import os

from boto3.s3.transfer import S3Transfer, TransferConfig
import re
from lib.log import setup_logger
from lib.notification import SlackNotification


class S3(object):

    def __init__(self, bucket, prefix=None, access_key=None, secret_access=None):
        self._s3 = None
        self._s3_wormhole = None
        self.bucket = bucket
        self._logger = setup_logger(__name__)
        self.slackBot = SlackNotification(__name__)

        # Set Prefix if not None
        if prefix is not None:
            self.prefix = prefix
        else:
            self.prefix = None

        self._region = 'ap-southeast-2'

        # Set Access Key pair for external transfer if not None
        if all([access_key, secret_access]):
            self.access_key = access_key
            self.secret_access = secret_access
        else:
            self.access_key = None
            self.secret_access = None

        if all([self.access_key, self.secret_access]):
            self._s3 = boto3.client('s3', region_name=self._region, aws_access_key_id=self.access_key,
                                    aws_secret_access_key=self.secret_access)
        else:
            self._s3 = boto3.client('s3', region_name=self._region)

        # Warmhole to support multipart transfers with following config

        self._s3_wormhole = S3Transfer(self._s3,  config=TransferConfig(
            multipart_threshold=8 * 1024 * 1024,
            max_concurrency=10,
            num_download_attempts=10,
        ))

    def get(self, key, local_path):
        """Download s3://bucket/key to local_path'."""
        try:
            self._s3_wormhole.download_file(
                bucket=self.bucket, key="{}/{}".format(self.prefix, key) if self.prefix else key, filename=local_path)
        except Exception as e:
            self._logger.info(
                "Could not get {} from S3 due to {}".format(key, e))
            return None
        else:
            self._logger.info("Successfully get {} from S3".format(key))
        return key

    def put(self, local_path, key):
        """Upload local_path to s3: // bucket/key and print upload progress."""
        try:

            self._s3_wormhole.upload_file(filename=local_path,
                                          bucket=self.bucket,
                                          key="{}/{}".format(self.prefix,
                                                             key) if self.prefix else key,
                                          callback=ProgressPercentage(local_path))
        except Exception as e:
            self._logger.info(
                "Could not upload {} to S3 due to {}".format(key, e))
        else:
            self._logger.info("Successfully uploaded {} to S3".format(key))

    def list_objects(self, prefix):
        """Get a list of all keys in an S3 bucket."""
        keys = set()
        kwargs = {'Bucket': self.bucket, 'Prefix': prefix}
        while True:
            resp = self._s3.list_objects_v2(**kwargs)

            try:
                for obj in resp['Contents']:
                    if obj['Key'][-1] == "/":
                        continue
                    keys.add(obj['Key'])
            except KeyError:
                return keys

            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break
        return keys

    def delete(self,  key):
        self._logger.info(f"Deleting s3://{self.bucket}/{key}")
        self._s3.delete_object(
            Bucket=self.bucket, Key=key)
        self._logger.info(f"Successfully deleted s3://{self.bucket}/{key}")


class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (self._filename, self._seen_so_far,
                                             self._size, percentage))
            sys.stdout.flush()
