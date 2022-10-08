import json
import time
import requests
import pandas as pd

def get_anime_stats(session: requests.Session, anime_id: int):
    """
    Returns the stats for the anime
    associated with the given anime_id
    """
    url = f"https://api.jikan.moe/v4/anime/{anime_id}/statistics"
    resp = session.get(url)
    if resp.status_code == 200:
        return json.loads(resp.text)


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



