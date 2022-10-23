"""
@Author: Martin Arroyo
@Email: martinm.arroyo7@gmail.com
@Description:
A collection of functions used to extract and upload anime stats
from MyAnimeList using the Jikkan API. 

The Jikkan API documentation can be found here:
https://docs.api.jikan.moe/
"""
import os
import sys
import time
import boto3
import asyncio
import aiohttp
import utils.storage as storage
from botocore.exceptions import ClientError, EndpointConnectionError
from dotenv import dotenv_values
from loguru import logger
from pathlib import Path
from datetime import datetime


async def get_stats(session: aiohttp.ClientSession, url: str, anime_id: int):
    """
    A worker function that obtains the anime stats from the endpoint
    and returns the JSON response if found. Otherwise, it returns the
    status code from the response.

    :param session: An asynchronous client session to connect to the endpoint
    :param url: The API endpoint to call
    :param anime_id: The `mal_id` used for the API request
    """
    async with session.get(url) as response:
        status = response.status
        if status == 200:
            stats = await response.json()
            # Append the anime_id to result
            stats["mal_id"] = anime_id
            return (status, stats)
        # We didn't get data - return the status
        return (status, anime_id)


async def get_anime_stats(anime_ids: list):
    """
    Gets the anime stats from the Jikkan API statistics endpoint.
    Statistics are gathered on a per-anime basis. To access statistics,
    the endpoint requires a valid `mal_id` from the client.

    :param anime_id: A list of valid anime IDs (mal_id)
    """
    anime_stats = []
    to_process = anime_ids
    total_anime = len(anime_ids)
    while len(anime_stats) < total_anime:
        async with aiohttp.ClientSession() as session:
            tasks = []
            # Process anime id's asynchronously, continuously decreasing
            # batch size (based on sleep time) to achieve ~20-33% boost in 
            # total processing speed compared to waiting 1 second between
            # calls to account for rate limiting.
            for id in to_process:
                url = f"https://api.jikan.moe/v4/anime/{id}/statistics"
                task = asyncio.ensure_future(get_stats(session, url, id))
                tasks.append(task)
                await asyncio.sleep(1/2)
            results = await asyncio.gather(*tasks)
            # Extract good data from bad in response
            success = [res[1] for res in results if res[0] == 200]
            retries = [res[1] for res in results if res[0] == 429]
            anime_stats += success
            total_anime = len(anime_stats) + len(retries)
            to_process = retries

    return anime_stats


def extract_anime_ids(anime_data: list) -> list:
    """
    Extracts the anime ids from the raw all_anime data

    :param anime_data: The raw all_anime data in a list
    """
    try:
        anime_ids = (anime["mal_id"] for anime in anime_data)
        return anime_ids
    except KeyError:
        logger.exception("There was an error extracting the anime IDs. Check code.")
        raise


def get_anime_data(client: boto3.client, bucketname: str, filename: str) -> tuple:
    """
    Gets the anime data from storage and returns a tuple with the data and a success status

    :param client: A boto3 client configured to access storage
    :param bucketname: The name of the bucket where MyAnimeList data is stored
    :param filename: The filename and partition path of the all_anime.json file
    """
    try:
        anime_data = storage.read_from_storage(client, bucketname, filename)
        return (None, anime_data)
    except (ClientError, EndpointConnectionError) as ex:
        return (ex, None)
    

def upload_anime_stats(
    client: boto3.client,
    bucketname: str,
    input_file: str,
    output_file: str = "anime_stats.json",
    prefix: str = "anime_stats/raw",
    partition_date: datetime = datetime.now(),
    testing: bool = False,
    sample: int = 0,
) -> tuple:
    """
    Extracts anime stats from the Jikkan API statistics endpoint
    and uploads them to cloud storage. Returns an error status and
    the total number of anime stats uploaded.

    :param client:  A boto3 client configured to access storage
    :param bucketname: The name of the bucket where MyAnimeList data is stored
    :param input_file: The location of the `all_anime.json` data for `mal_id` extraction
    :param output_file: The path that the `anime_stats.json` data will be written to
    :param prefix: The prefix to use for the data set. Default is `anime_stats/raw`
    :param testing: A flag used to determine if this is a test run or not. If it is a test
                    run, then the `anime_id` list will be shortened up to the @sample size
    :param sample: An integer used to determine the number of anime_id's to use for testing
    """
    # Get the anime ids
    logger.info("Getting Anime IDs...")
    try:
        err, anime_data = get_anime_data(client, bucketname, input_file)
        
        if err is not None:
            logger.info(f"There was an error with downloading the all_anime data\n{err}")
            raise ValueError("Unable to continue without anime data")
        
        anime_ids = extract_anime_ids(anime_data)

        if testing:
            logger.info(f"Testing with a sample size of {sample} anime IDs")
            # Collect the sample size from the anime_ids generator
            anime_ids = [id for n, id in enumerate(anime_ids) if n < sample]
        # Get the anime stats
        logger.info("Extracting anime stats from Jikkan API...")
        anime_stats = asyncio.run(get_anime_stats(anime_ids))
        logger.info(f"Extraction complete! Extracted stats for {len(anime_stats)} anime")
        # Upload the stats
        logger.info("Uploading to storage...")
        storage.write_to_storage(
            client=client, 
            obj=anime_stats, 
            prefix=prefix, 
            filename=output_file, 
            partition_date=partition_date
        )

        return (None, len(anime_stats))

    except Exception as ex:
        return (ex, 0)


def main() -> None:
    """
    Runs the animestats extraction and uploading from end to end
    The user must supply a file path to the input file (excluding the bucket name)

    Command Example: 

    File endpoint (w/ bucket name): myanimelist/all_anime/raw/year=2022/month=10/day=11/all_anime.json
    
    Command:
    ```
    python animestats.py all_anime/raw/year=2022/month=10/day=11/all_anime.json
    ```

    """
    # Initialize configuration
    logger.info("Initializing...")
    start = time.time()
    config_path = Path.cwd() / ".env"
    config = dotenv_values(config_path.resolve())
    commands = sys.argv[1:]
    input_file = commands[0]
    command_str = " ".join(commands)
    testing = False
    sample = 0
    if len(commands) == 4:
        tests = [
            commands[1] == "sample",
            commands[2] == "-n",
            commands[3].isnumeric(),
        ]
        valid_cmd = all(tests)
        if not valid_cmd:
            logger.info(
                f"{command_str} was an invalid command."
                "Continuing with default settings..."
            )
        else:
            testing = True
            sample = int(commands[3])
            logger.info(f"Test run starting... collecting stats for {sample} anime")

    logger.info("Starting process...")
    # Connect to client
    client = storage.access_storage(config)
    # Run the process
    err, status = upload_anime_stats(
        client=client,
        bucketname="myanimelist",
        input_file=input_file,
        testing=testing,
        sample=sample,
    )
    end = time.time()
    duration = round(end - start, 2)
    if err is not None:
        logger.info(f"An error occurred while getting anime stats.\n{repr(err)}")
        os._exit(1)
    logger.info(f"Uploaded {status} anime! Elapsed time: {duration} second(s)")


if __name__ == "__main__":
    main()
