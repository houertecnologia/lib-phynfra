import asyncio
import json
import random

from boto3 import Session
from botocore.client import BaseClient
from botocore.exceptions import BotoCoreError
from botocore.exceptions import ClientError
from loguru import logger
from phynfra.aws.async_utils import AsyncResponseS3


class AWSBucket:
    def get_boto3_session(
        self,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
        region_name: str = None,
        aws_profile: str = None,
    ) -> Session:
        """
        Get access to AWS credentials

        Args:
            aws_access_key_id: AWS access key id
            aws_secret_access_key: AWS secret access key
            region_name: AWS region
            aws_profile: AWS profile
        """
        return Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
            profile_name=aws_profile,
        )

    def write_json_data_to_s3(
        self, s3_client: BaseClient, data: dict, bucket_name: str, key: str, content_type: str = "application/json"
    ):
        """
        Write json to aws s3 bucket

        Args:
            s3_client: Client S3 for Boto3
            data: Object data
            bucket_name: Destination bucket to save json file
            key: Path to save the object.
            content_type: A standard MIME type describing the format of the contents
        """

        logger.info(f"Writing data to s3://{bucket_name}/{key}")

        try:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=json.dumps(data, ensure_ascii=False).encode("utf-8"),
                ContentType=content_type,
            )
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Error trying to write data to s3://{bucket_name}/{key}")
            logger.error(e)
            raise e

    async def write_json_data_to_s3_async(
        self,
        s3_client,
        data: dict,
        bucket_name: str,
        key: str,
        task_name: str,
        content_type: str = "application/json",
        max_retries: int = 3,
    ) -> AsyncResponseS3:
        """
        Write json in asynchronous mode to aws s3 bucket

        Args:
            s3_client: Client S3 for Boto3
            data: Object data
            bucket_name: Destination bucket to save json file
            key: Path to save the object.
            task_name:
            content_type: A standard MIME type describing the format of the contents
            max_retries: Maximum retries that the function will execute
        """

        s3_full_path = f"s3://{bucket_name}/{key}"

        for retry in range(1, max_retries + 1):
            try:
                await s3_client.put_object(
                    Bucket=bucket_name,
                    Key=key,
                    Body=json.dumps(data, ensure_ascii=False).encode("utf-8"),
                    ContentType=content_type,
                )

                logger.info(f"Uploaded to {s3_full_path}")
                return AsyncResponseS3(task_name, True, s3_full_path)
            except Exception as e:
                logger.error(f"Error trying to upload to {s3_full_path}")
                logger.error(f"The task {task_name} failed on attempt {retry} with error:")
                logger.error(e)
                # Calculate the wait time using exponential backoff
                wait_time = random.uniform(2 ** (retry - 1), 2**retry)
                logger.info(f"Retrying task {task_name} in {wait_time:.2f} seconds")
                await asyncio.sleep(wait_time)

        return AsyncResponseS3(task_name, False, s3_full_path)

    def check_s3_folder(self, s3_client: BaseClient, bucket_name: str, folder_prefix: str):
        """
        Check if directory exists in s3 bucket

        Args:
            bucket_name: Destination bucket to check directory
            folder_prefix: Directory in bucket
        """
        try:
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix, MaxKeys=2)
            objects = response.get("Contents", [])

            if len(objects) > 0:
                logger.debug(
                    f"The folder exists or there are files on the folder '{folder_prefix}' in S3 bucket '{bucket_name}'"
                )
                return True
            else:
                logger.debug(
                    f"The folder does not exist or there are no files on the folder '{folder_prefix}' in S3 bucket '{bucket_name}'"
                )
                return False
        except Exception as e:
            logger.error("An error occurred:")
            logger.error(e)
            raise e
