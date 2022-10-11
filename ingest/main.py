""""
A script for extracting data from the Jikkan anime
API and loading it to cloud storage. This is a long-running
batch process that updates all of the anime data along
with the newest statistics for each anime.
"""
import os
import sys
import time
import yaml
from yaml.loader import SafeLoader
from dotenv import dotenv_values
from loguru import logger
from tqdm import tqdm
from utils import anime, animestats, writer

def command_validator(commands: list) -> int:
    """
    Takes in a list of commands from stdin and
    determines whether or not to perform a test
    run

    :param commands: A list of commands from stdinput
    """
    page_count = None
    command_str = " ".join(commands)
    # Handle trial runs
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
            logger.info(
                f"Performing test run using a sample size of {page_count * 25} anime"
            )

    return page_count


def main():
    logger.info("Initializing")
    start = time.time()
    with open('utils/config.yaml', 'r', encoding='utf-8') as f:
        data_config = yaml.load(f, Loader=SafeLoader)
    config = dotenv_values('.env')
    commands = sys.argv[1:]
    # Determine the number of pages to collect from the anime endpoint
    page_count = command_validator(commands)
    testing = True if page_count is not None else False
    sample = page_count * 25 if page_count is not None else 0
    # Start a connection to the storage client
    client = writer.access_storage(config)
    bucket = "myanimelist"
    # TODO: Write function to handle getting latest data from buckets
    input_file = "all_anime/raw/year=2022/month=10/day=10/all_anime.json"
    # Run each process in order and display a progress bar
    logger.info("Starting process...")
    for process in tqdm(range(2)):
        if process == 0:
            # Upload anime info 
            logger.info("Uploading anime info...")
            anime.upload_all_anime(client, data_config['schema'], page_count)
            logger.info("Anime info has been uploaded to storage!")
        elif process == 1:
            # Upload anime stats
            logger.info("Uploading anime stats...")
            animestats.upload_anime_stats(client, bucket, input_file, testing=testing, sample=sample)
            logger.info("Anime stats uploaded!")
    
    logger.info("Process complete!")
    end = time.time()
    duration = round(end - start, 2)
    logger.info(f"Elapsed time: {duration} second(s)")
    os._exit(0)
    

if __name__ == '__main__':
    main()