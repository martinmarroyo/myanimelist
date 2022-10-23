"""A class that handles writing data to storage"""
import boto3
import json
from io import BytesIO
from loguru import logger
from datetime import datetime
from botocore.exceptions import ClientError


def access_storage(config: dict):
    """
    Obtains and returns a client for access to storage
    
    :param config: The configuration file that has credentials needed for storage access
    """
    endpoint = config.get("ENDPOINT_URL")
    try:
        client = boto3.client(
            "s3",
            aws_access_key_id=config["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=config["AWS_SECRET_ACCESS_KEY"],
            endpoint_url=endpoint,
        )
        
    except ClientError:
        logger.exception(
            "There is an issue with accessing the client. Please check your connection and credentials and try again"
        )
        raise
    except KeyError:
        logger.exception("There was an issue with the credentials in your config file. Please check your config and try again.")
    
    return client

def write_to_storage(
    client: boto3.client,
    obj: dict,
    prefix: str,
    filename: str,
    bucketname: str = "myanimelist",
    partition_date: datetime = datetime.now()
):
    """
    Writes the given object to storage using the provided configuration.
    Returns the path that the obj was written to.

    :param client: A boto3 client configured to write to s3
    :param obj: The raw dict object to write to storage
    :param prefix: The root prefix to write in
    :param filename: The filename of the object for use in the bucket
    :param bucketname: The name of the storage bucket
    :param partition_date: The partition date to write 
    """
    dt = partition_date
    partition_path_date = f"year={dt.year}/month={dt.month}/day={dt.day}"
    key = f"{prefix}/{partition_path_date}/{filename}"
    logger.info("Writing to storage...")
    try:
        client.put_object(Body=json.dumps(obj), Bucket=bucketname, Key=key)
        logger.info(f"{filename} has been successfully written to storage at {key}")
    except (ClientError, KeyError):
        logger.exception("Error occurred while writing to storage...")
        raise

    return key


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


class Writer():
    """
    A class used to read from and write to s3 storage. It is
    initialized from a configuration file that expects the following
    values:

    :param AWS_ACCESS_KEY_ID: The AWS Access Key ID for the client
    :param AWS_SECRET_ACCESS_KEY: The AWS Secret Access Key for the client
    :param ENDPOINT_URL (optional): The endpoint URL for the storage location 
    """
    def __init__(self, config: dict):
        
        def _init_client(config: dict):
            endpoint = config.get("ENDPOINT_URL")
            try:
                client = boto3.client(
                    "s3",
                    aws_access_key_id=config["AWS_ACCESS_KEY_ID"],
                    aws_secret_access_key=config["AWS_SECRET_ACCESS_KEY"],
                    endpoint_url=endpoint,
                )
            except ClientError:
                logger.exception("Client received invalid credentials")
                raise

            return client

        
        self.config = config
        self.client = _init_client(self.config)


    def write_to_storage(
        self,
        obj: dict,
        partition: str,
        filename: str,
        date: datetime = datetime.now(),
        bucket: str = "myanimelist",
    ):
        """
        Writes the given object to storage using the provided configuration.

        :param obj: The raw dict object to write to storage
        :param partition: The primary storage partition for the file
        :param filename: The filename of the object for use in the bucket
        :param date: A datetime used for creating the partition path
        :param bucket: The name of the storage bucket
        """
        dt = date
        partition_date = f"year={dt.year}/month={dt.month}/day={dt.day}"
        key = f"{partition}/{partition_date}/{filename}"
        logger.info("Writing to storage...")
        try:
            self.client.put_object(Body=json.dumps(obj), Bucket=bucket, Key=key)
            logger.info(f"{filename} has been successfully written to storage at {key}")
        except (ClientError, KeyError):
            logger.exception("Error occurred while writing to storage...")
            raise
    

    def read_from_storage(self, bucketname: str, filepath: str) -> list:
        """
        Reads from s3 storage and returns the file found at the given path

        :param bucketname: The name of the storage bucket to read from
        :param filepath: The path to the file that you want to read from inside the bucket
        """
        file_holder = BytesIO()

        try:
            self.client.download_fileobj(bucketname, filepath, file_holder)
        except (ClientError, KeyError):
            logger.exception(
                f"Error occurred while reading from storage at filepath: {filepath}"
            )
            raise

        file_holder.seek(0)
        result = json.load(file_holder)

        return result

    @property
    def get_client(self):
        return self.client

    