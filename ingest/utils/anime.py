"""
@Author: Martin Arroyo
@Email: martinm.arroyo7@gmail.com
@Description:
A collection of functions used to extract and upload anime facts
from MyAnimeList using the Jikkan API. 

The Jikkan API documentation can be found here:
https://docs.api.jikan.moe/
"""
import os
import sys
import boto3
import json
import time
import yaml
import asyncio
import aiohttp
import requests
import utils.storage as storage
from loguru import logger
from datetime import datetime
from dotenv import dotenv_values
from yaml.loader import SafeLoader


def get_page_count() -> tuple:
    """
    Gets the total page count for the `all anime` endpoint.
    Returns a tuple with the structure (status code, value).
    """
    URL = "https://api.jikan.moe/v4/anime?sfw=true"
    response = requests.get(URL)
    status = response.status_code
    if status == 200:
        total_pages = json.loads(response.content)
        return (status, int(total_pages["pagination"]["last_visible_page"]))
    return (status, 0)


async def get_anime_page(session, url) -> tuple:
    """
    Gets the anime page specified in the given `url`.
    Returns a tuple of (status code, response). In the
    case of an error status, the response returned is the URL
    that returned the error.

    :param session: A session used to call the API
    :param url: The URL for the API call
    """
    async with session.get(url) as response:
        status = response.status
        if status == 200:
            page = await response.json()
            return (status, page)
        return (status, url)


async def generate_anime_list(session: aiohttp.ClientSession, page_count: int = 0):
    """
    Returns a list containing pages from the all anime list

    :param session: A session used to call the API
    :param page_count: The number of pages to extract from the endpoint.
                       If this is not passed, it will extract all current
                       pages. Each page contains information for 25 anime
                       titles.
    """
    urls = [
        f"https://api.jikan.moe/v4/anime?page={page}&sfw=true" 
        for page in range(1, page_count+1)
    ]
    pages = []
    while len(urls) > 0:
        tasks = []
        # Process anime's asynchronously, continuously decreasing
        # batch size (based on sleep time) to achieve ~20-33% boost in 
        # total processing speed compared to waiting 1 second between
        # calls to account for rate limiting.
        for url in urls:
            task = asyncio.ensure_future(get_anime_page(session, url))
            tasks.append(task)
            await asyncio.sleep(1/2)
        results = await asyncio.gather(*tasks)
        # Check status codes and assign results to appropriate list
        pages += [res[1] for res in results if res[0] == 200]
        retries = [res[1] for res in results if res[0] == 429]
        # Retry URLs returned with timeout status code (429)
        urls = retries
        results = None
        
    return pages


async def get_anime(page_count: int = None):
    """
    Gets all the raw anime data from the Jikan API /anime endpoint

    :param page_count: The number of pages to extract from the endpoint.
                       If this is not passed, it will extract all current
                       pages. Each page contains information for 25 anime
                       titles.
    """
    async with aiohttp.ClientSession() as session:
        if page_count is None:
            page_count = get_page_count()
        pages = await generate_anime_list(session, page_count)
    return pages


def extract_anime_data(raw_data: list, schema: list) -> dict:
    """
    Extracts the anime data from the raw anime list based on a pre-selected
    schema.

    :param raw_data: A list of raw JSON data from the `anime` endpoint
    :param schema: The schema to use to extract anime data from the raw data.
    """
    anime_data = []
    try:
        for page in raw_data:
            for anime in page["data"]:
                cols = [field for field in anime.keys() if field in schema]
                data = {col: anime[col] for col in cols}
                data["aired_from"] = data["aired"]["from"]
                data["aired_to"] = data["aired"]["to"]
                data.pop("aired")
                anime_data.append(data)
    except KeyError:
        logger.exception(
            "Issue occurred with anime page keys. Please check code and try again."
        )
        raise

    return anime_data


def upload_all_anime(
    client: boto3.client, 
    schema: list, 
    page_count: int = None, 
    partition_date: datetime = datetime.now()
    ) -> tuple:
    """
    Extracts anime data from the Jikkan all anime enpoint
    and writes it to cloud storage.

    :param client: A boto3 client configured to access s3 storage
    :param schema: The schema to use to extract anime data from the raw data.
    :param page_count: The number of pages to extract from the endpoint.
                       If this is not passed, it will extract all current
                       pages. Each page contains information for 25 anime
                       titles.
    """
    try:
        if page_count is None:
            status, page_count = get_page_count()
            if status != 200:
                raise ValueError("Error retrieving page count from API")
        logger.info("Connecting to Jikkan API and extracting raw data...")
        # Get the raw data
        raw_data = asyncio.run(get_anime(page_count))
        # Extract the data we want
        logger.info("Raw extraction complete! Extracting configured fields...")
        data = extract_anime_data(raw_data, schema)
        logger.info("Extraction complete! Writing to storage...")
        # Write to storage
        prefix = "all_anime/raw"
        filename = "all_anime.json"
        file_partition = storage.write_to_storage(
            client=client, 
            obj=data, 
            prefix=prefix, 
            filename=filename,
            partition_date=partition_date
        )
        logger.info("Writing to storage is complete!")
        return (None, file_partition)
    except Exception as ex:
        return (ex, None)


def main():
    # Initialize configuration
    with open("utils/config.yaml", "r", encoding="utf-8") as f:
        config = yaml.load(f, Loader=SafeLoader)
    dbconfig = dotenv_values(".env")
    # Check for commandline arguments
    page_count = None
    commands = sys.argv[1:]
    command_str = " ".join(commands)
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
            page_count = int(commands[2])
    logger.info("Starting process...")
    start = time.time()
    client = storage.access_storage(dbconfig)
    err, filename = upload_all_anime(client, config["schema"], page_count)
    if err is not None:
        logger.info(f"An error occurred while getting anime info:\n{repr(err)}")
        os._exit(1)
    logger.info(f"Process complete! Anime data has been written to {filename}")
    end = time.time()
    duration = round(end - start, 2)
    logger.info(f"Elapsed time: {duration} second(s)")


if __name__ == "__main__":
    main()