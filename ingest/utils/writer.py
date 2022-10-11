"""A class that handles writing data to storage"""
import boto3
import json
from io import BytesIO
from loguru import logger
from datetime import datetime
from botocore.exceptions import ClientError


def access_storage(config: dict):
    """Obtains and returns a client for access to storage"""
    client = boto3.client(
        "s3",
        aws_access_key_id=config["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=config["AWS_SECRET_ACCESS_KEY"],
        endpoint_url=config["DATALAKE_ENPOINT"],
    )

    return client


def write_to_storage(
    client: boto3.client,
    obj: dict,
    partition: str,
    filename: str,
    bucketname: str = "myanimelist",
):
    """
    Writes the given object to storage using the provided configuration.

    :param client: A boto3 client configured to write to s3
    :param obj: The raw dict object to write to storage
    :param partition: The primary storage partition for the file
    :param filename: The filename of the object for use in the bucket
    :param bucketname: The name of the storage bucket
    """
    dt = datetime.now()
    partition_date = f"year={dt.year}/month={dt.month}/day={dt.day}"
    key = f"{partition}/{partition_date}/{filename}"
    logger.info("Writing to storage...")
    try:
        client.put_object(Body=json.dumps(obj), Bucket=bucketname, Key=key)
        logger.info(f"{filename} has been successfully written to storage at {key}")
    except (ClientError, KeyError):
        logger.exception("Error occurred while writing to storage...")
        raise


def read_from_storage(client: boto3.client, bucketname: str, filepath: str) -> list:
    """
    Reads from s3 storage and returns the file found at the given path

    :param client: An s3 client configured to connect to storage
    :param bucketname: The name of the storage bucket to read from
    :param filepath: The path to the file that you want to read from inside the bucket
    """
    file_holder = BytesIO()
    try:
        client.download_fileobj(bucketname, filepath, file_holder)
    except (ClientError, KeyError):
        logger.exception(
            f"Error occurred while reading from storage at filepath: {filepath}"
        )
        raise

    file_holder.seek(0)
    result = json.load(file_holder)

    return result
