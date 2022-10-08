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


def get_anime_ids(connection: psycopg2.connect):
    """
    Takes in a db connection and returns
    a DataFrame of anime ids
    """
    query = """
        SELECT id
        FROM anime.all_anime
        GROUP BY id
    """
    ids = pd.read_sql(query, connection)
    return ids
