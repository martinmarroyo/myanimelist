"""
A script to transform the raw anime data into Hudi data sets
for storage in the data lake
"""
from pyspark.sql import SparkSession
import boto3
import json
import time
import os
from io import BytesIO
from datetime import datetime
from pyspark.sql import DataFrame as SparkDataFrame
from botocore.exceptions import ClientError
import pyspark.sql.functions as F
from flatdict import FlatDict
from loguru import logger

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


def flatten_stats(anime_stats: dict) -> dict:
    """
    Flattens the anime statistics section of the raw
    anime_stats data

    :param anime_stats: A collection of stats and scores for an anime
    """
    flat = dict(FlatDict(anime_stats['data'], delimiter='_'))
    flat['mal_id'] = anime_stats['mal_id']
    flat['update_time'] = int(time.time() * 1000)
    return json.dumps(flat)


def extract_and_flatten_scores(anime_stats: dict) -> dict:
    """
    Extracts and flattens the anime scores section of the raw
    anime_stats data

    :param anime_stats: A collection of stats and scores for an anime
    """
    scores = []
    for score in anime_stats['data']['scores']:
        score['mal_id'] = anime_stats['mal_id']
        score['update_time'] = int(time.time() * 1000)
        scores.append(score)
    return scores


def write_to_hudi(
    dataframe: SparkDataFrame,
    tablename: str, 
    output_path: str,
    recordkey: str = 'mal_id', 
    partition_path: str = 'partition_path',
    precombine_field: str = 'update_time',
    operation: str = 'upsert',
    mode: str = 'overwrite'
) -> tuple:
    """
    Writes the given dataframe to storage as a Hudi dataset.

    :param dataframe: The Spark DataFrame to write to storage
    :param tablename: The name given to the table written to storage
    :param output_path: The intended full path to the object in storage
    :param recordkey: The primary key for the table
    :param partition_path: The column used to partition the data by in storage
    :param precombine_field: The column used to distinguish two identical records by timestamp
    :param operation: The write operation to perform on the Hudi dataset. Default is `upsert`.
    :param mode: The mode to use when writing data to storage. Default is `overwrite`. 
    """
    opts = {
        'hoodie.table.name': tablename,
        'hoodie.datasource.write.recordkey.field': recordkey,
        'hoodie.datasource.write.partitionpath.field': partition_path,
        'hoodie.datasource.write.table.name': tablename,
        'hoodie.datasource.write.operation': operation,
        'hoodie.datasource.write.precombine.field': precombine_field,
    }
    try:
        (
            dataframe.write.format('org.apache.hudi')
            .options(**opts).mode(mode).save(output_path)
        )
        return (True, dataframe.count())
    except Exception as ex:
        return (False, ex)


def get_spark_session_and_context(appname: str):
    spark =( 
        SparkSession.builder
        .appName(appname)
        .config('spark.jars', '/opt/bitnami/spark/jars/hudi-spark3.2-bundle_2.12-0.12.0.jar')
        .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog')
        .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension')
        .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')
        .config('spark.hadoop.fs.s3a.endpoint', os.environ['ENDPOINT_URL'])
        .config('spark.hadoop.fs.s3a.access.key', os.environ['AWS_ACCESS_KEY_ID'])
        .config('spark.hadoop.fs.s3a.secret.key', os.environ['AWS_SECRET_ACCESS_KEY'])
        .config('spark.hadoop.fs.s3a.path.style.access', 'true')
        .getOrCreate()
    )
    sc = spark.sparkContext
    sc.setLogLevel("OFF")
    return (spark, sc)


def main():
    # Get Spark
    logger.info('Beginning Spark Job...')
    spark, sc = get_spark_session_and_context('anime_transform')
    today = datetime.now()
    date_partition = f'{today.year}/{today.month}/{today.day}'
    # Get client
    client = boto3.client('s3', endpoint_url=os.environ['ENDPOINT_URL'])
    # Get anime data
    logger.info('Retrieving data from storage...')
    raw_anime_data = spark.read.json(
        f's3a://myanimelist/all_anime/raw/year={today.year}/month={today.month}/day={today.day}/all_anime.json'
    )
    # Get anime stats
    raw_anime_stats = read_from_storage(
        client, 
        bucketname='myanimelist', 
        filepath=f'anime_stats/raw/year={today.year}/month={today.month}/day={today.day}/anime_stats.json')
    logger.info('Creating Dataframes...')
    # Create all anime
    all_anime = raw_anime_data.withColumn('update_time', F.lit(time.time() * 1000))
    all_anime = all_anime.withColumn('partition_path', F.lit(date_partition))
    # Create anime stats
    anime_stats = sc.parallelize(raw_anime_stats).map(flatten_stats)
    anime_stats = spark.read.json(anime_stats)
    anime_stats = anime_stats.drop('scores')
    anime_stats = anime_stats.withColumn('partition_path', F.lit(date_partition))
    # Create anime scores
    anime_scores = sc.parallelize(raw_anime_stats).map(extract_and_flatten_scores).flatMap(lambda x: x)
    anime_scores = spark.read.option('multiline','true').json(anime_scores)
    anime_scores = anime_scores.withColumn('partition_path', F.lit(date_partition))
    logger.info('Writing to storage...')
    # Write to storage
    anime_success, anime_result = write_to_hudi(
        dataframe=all_anime, tablename='all_anime', output_path='s3a://myanimelist/all_anime/processed'
    )
    stats_success, stats_result = write_to_hudi(
        dataframe=anime_stats, tablename='anime_stats', output_path='s3a://myanimelist/anime_stats/processed/stats', operation='insert'
    )
    scores_success, scores_result = write_to_hudi(
        dataframe=anime_scores, tablename='anime_scores', output_path='s3a://myanimelist/anime_stats/processed/scores', operation='insert'
    )

    if all([anime_success, stats_success, scores_success]):
        logger.info(
            'All tables were successfully written to storage\n'
            'Table stats:\n'
            f'all_anime: {anime_result} total records written\n'
            f'anime_stats: {stats_result} total records written\n'
            f'anime_scores: {scores_result} total records written\n'
        )
        
    else:
        logger.info('There was an error during the upload. Check storage and logs.')


if __name__ == '__main__':
    main()