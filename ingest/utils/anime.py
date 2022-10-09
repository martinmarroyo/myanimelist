"""
A collection of functions used to gather anime 
statistics from My Anime List using the 
Jikkan API
"""
import os
import sys
import json
import time
import yaml
import asyncio
import aiohttp
import requests
from db import ingest
from loguru import logger
from dotenv import dotenv_values
from yaml.loader import SafeLoader
from sqlalchemy.ext.asyncio import create_async_engine

def get_page_count():
    """
    Gets the total page coun for the `all anime` endpoint
    """
    URL = "https://api.jikan.moe/v4/anime?sfw=true"
    initial_response = requests.get(URL)
    if initial_response.status_code == 200:
        total_pages = json.loads(initial_response.content)
        return int(total_pages["pagination"]["last_visible_page"])
    return -1


async def get_anime_page(session, url):
    """
    Gets the anime page specified in the given `url` and
    returns the page if successful. Otherwise, it will 
    return the status code number from the API call.
    """
    async with session.get(url) as response:
        if response.status == 200:
            page = await response.json()
            return page
        return response.status


async def generate_anime_list(session: aiohttp.ClientSession, page_count: int = 0):
    """
    Returns a list containing pages from the all anime list
    """
    tasks = []
    for page_num in range(1, page_count + 1):
        url = f"https://api.jikan.moe/v4/anime?page={page_num}&sfw=true"
        task = asyncio.ensure_future(get_anime_page(session, url))
        tasks.append(task)
        await asyncio.sleep(1)
    pages = await asyncio.gather(*tasks)
    return pages


async def get_anime(page_count: int = None):
    """
    Gets all the raw anime data from the Jikan API /anime endpoint
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
    """
    anime_data = []
    try:
        for page in raw_data:
            for anime in page["data"]:
                cols = [field for field in anime.keys() if field in schema]
                data = {col:anime[col] for col in cols}
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


if __name__ == "__main__":
    # Initialize configuration
    with open('utils/config.yaml', 'r', encoding='utf-8') as f:
        config = yaml.load(f, Loader=SafeLoader)
    dbconfig = dotenv_values('.env')
    engine = create_async_engine(dbconfig["DATABASE_URL"])
    # Check for commandline arguments
    page_count = None
    commands = sys.argv[1:]
    command_str = ' '.join(commands)
    if len(commands) == 3:
        tests = [
            commands[0] == 'sample',
            commands[1] == '-n',
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
    logger.info("Getting raw data from API")
    raw_data = asyncio.run(get_anime(page_count))
    logger.info("Raw data download complete! Extracting...")
    data = extract_anime_data(raw_data, config["schema"])
    logger.info("Data extracted! Writing to database...")
    asyncio.run(ingest(data, engine))
    logger.info("Process complete!")
    end = time.time()
    duration = round(end-start, 2)
    logger.info(f"Elapsed time: {duration} second(s)")
    os._exit(0)