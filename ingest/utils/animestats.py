import os
import sys
import time
import boto3
import asyncio
import aiohttp
from dotenv import dotenv_values
from loguru import logger
from utils.writer import access_storage, read_from_storage, write_to_storage


async def get_stats(session: aiohttp.ClientSession, url):
    """
    A worker function that obtains the anime stats from the endpoint
    and returns the JSON response if found. Otherwise, it returns the
    status code from the response.

    :param session: An asynchronous client session to connect to the endpoint
    """
    async with session.get(url) as response:
        if response.status == 200:
            stats = await response.json()
            return stats
        # We didn't get data - check the status
        return response.status


async def get_anime_stats(anime_id: list):
    """
    Gets the anime stats from the Jikkan API statistics endpoint.
    Statistics are gathered on a per-anime basis. To access statistics,
    the endpoint requires a valid `mal_id` from the client.

    :param anime_id: A list of valid anime IDs (mal_id)
    """
    async with aiohttp.ClientSession() as session:
        tasks = []
        for id in anime_id:
            url = f"https://api.jikan.moe/v4/anime/{id}/statistics"
            task = asyncio.ensure_future(get_stats(session, url))
            tasks.append(task)
            await asyncio.sleep(1)
        results = await asyncio.gather(*tasks)
        return results


def get_anime_ids(client: boto3.client, bucketname: str, filename: str) -> list:
    """
    Gets the anime ids from the raw all_anime data

    :param client: A boto3 client configured to access storage
    :param bucketname: The name of the bucket where MyAnimeList data is stored
    :param filename: The filename and partition path of the all_anime.json file
    """
    anime_data = read_from_storage(client, bucketname, filename)
    try:
        anime_ids = [anime["mal_id"] for anime in anime_data]
    except KeyError:
        logger.exception("There was an error extracting the anime IDs. Check code.")
        raise

    return anime_ids


def upload_anime_stats(
    client: boto3.client,
    bucketname: str,
    input_file: str,
    output_file: str = "anime_stats.json",
    partition: str = "anime_stats/raw",
    testing: bool = False,
    sample: int = 10,
):
    """
    Extracts anime stats from the Jikkan API statistics endpoint
    and uploads them to cloud storage.

    :param client:  A boto3 client configured to access storage
    :param bucketname: The name of the bucket where MyAnimeList data is stored
    :param input_file: The location of the `all_anime.json` data for `mal_id` extraction
    :param output_file: The path that the `anime_stats.json` data will be written to
    :param partition: The partition name to use for the data set. Default is `anime_stats/raw`
    :param testing: A flag used to determine if this is a test run or not. If it is a test
                    run, then the `anime_id` list will be shortened up to the @sample size
    :param sample: An integer used to determine the number of anime_id's to use for testing
    """
    # Get the anime ids
    logger.info("Getting Anime IDs...")
    anime_ids = get_anime_ids(client, bucketname, input_file)
    if testing:
        logger.info(f"Testing with a sample size of {sample} anime IDs")
        anime_ids = anime_ids[:sample]
    # Get the anime stats
    logger.info("Extracting anime stats from Jikkan API...")
    anime_stats = asyncio.run(get_anime_stats(anime_ids))
    logger.info(f"Extraction complete! Extracted stats for {len(anime_stats)} anime")
    # Upload the stats
    logger.info("Uploading to storage...")
    write_to_storage(client, anime_stats, partition, output_file)
    logger.info("Upload complete!")


if __name__ == "__main__":
    # Initialize configuration
    logger.info("Initializing...")
    start = time.time()
    config = dotenv_values(".env")
    commands = sys.argv[1:]
    command_str = " ".join(commands)
    testing = False
    sample = 0
    if len(commands) == 3:
        tests = [
            commands[0] == "sample",
            commands[1] == "-n",
            commands[2].isnumeric(),
        ]
        valid_cmd = all(tests)
        if not valid_cmd:
            logger.info(
                f"{command_str} was an invalid command."
                "Continuing with default settings..."
            )
        else:
            testing = True
            sample = int(commands[2])
            logger.info(f"Test run starting... collecting stats for {sample} anime")
    logger.info("Starting process...")
    # Connect to client
    client = access_storage(config)
    # Run the process
    upload_anime_stats(
        client=client,
        bucketname="myanimelist",
        input_file="all_anime/raw/year=2022/month=10/day=10/all_anime.json",
        testing=testing,
        sample=sample,
    )
    logger.info("Process complete!")
    end = time.time()
    duration = round(end - start, 2)
    logger.info(f"Elapsed time: {duration} second(s)")
    os._exit(0)
