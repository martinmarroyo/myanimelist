"""
A collection of functions used to gather anime 
statistics from My Anime List using the 
Jikkan API
"""
import json
import time
import asyncio
import aiohttp
import requests

def get_page_count():
    """
    Gets the total page count
    for the all anime endpoint
    """
    URL = "https://api.jikan.moe/v4/anime?sfw=true"
    initial_response = requests.get(URL)
    if initial_response.status_code == 200:
        total_pages = int(json.loads(initial_response.text)["pagination"]["last_visible_page"])
        return total_pages
    return -1


async def get_anime_page(session, url):
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
            page_count = get_page_count(session)
        pages = await generate_anime_list(session, page_count)
    return pages


def main():
    # Get anime list
    animelist = asyncio.run(get_anime())
    # Write to storage
    # TODO: write code for database insert
    pass


if __name__ == "__main__":
    main()