import os
import sys
import json
import time
import asyncio
import aiohttp
import psycopg2
import requests
import pandas as pd

async def get_stats(session: aiohttp.ClientSession, url):
    async with session.get(url) as response:
        if response.status == 200:
            stats = await response.json()
            return stats
        # We didn't get data - check the status
        return response.status
        

async def get_anime_stats(anime_id: list):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for id in anime_id:
            url = f"https://api.jikan.moe/v4/anime/{id}/statistics"
            task = asyncio.ensure_future(get_stats(session, url))
            tasks.append(task)
            await asyncio.sleep(1)
        results = await asyncio.gather(*tasks)
        return results


def get_anime_ids(connection: psycopg2.connect) -> list:
    """
    Takes in a db connection and returns
    a list of anime ids
    """
    query = """
        SELECT DISTINCT id
        FROM anime.all_anime
    """
    with connection.cursor() as cur:
        cur.execute(query)
        results = cur.fetchall()
        # Flatten results
        results = [i for sub in results for i in sub]
        return results


def test():
    """
    This is used for testing. This expects to be called using a `test` 
    command followed by a list of 1 to many space-separated ids, specified 
    using the `-id` argument.
    
    Example:
    
    python anime.py test -id 10 # Runs a test of `get_anime_stats` for 
    the anime with an id of 10
    """
    commands = sys.argv[1:]
    command_str = ' '.join(commands)
    if len(commands) >= 3:
        tests = [
            commands[0] == 'test',
            commands[1] == '-id',
            all([i.isnumeric() for i in commands[2:]]),
        ]
        valid_cmd = all(tests)
        # There should be exactly three commands. If there isn't, then break
        if not valid_cmd:
            print(f"Unexpected command: {command_str}")
            os._exit(1)
        anime_ids = [int(i) for i in commands[2:]]
        # Run get_anime with the given sample size
        data = asyncio.run(get_anime_stats(anime_ids))
        missing_data = sum([1 for res in data if res == 429])
        print(
            f"Total data collected: {len(data)} records, with {missing_data} records dropped."
        )
        print(data[0])
        os._exit(0)
    print(f"Unexpected input: {command_str}")
    os._exit(1)


if __name__ == '__main__':
    test()