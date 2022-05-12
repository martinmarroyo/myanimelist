"""
A collection of functions used to gather anime 
statistics from My Anime List using the 
Jikkan API
"""
import json
import time
from datetime import datetime
from itertools import count
import requests
import psycopg2
import pandas as pd


def get_page_count(url: str, session: requests.Session):
    """
    Gets the total page count
    for the all anime endpoint
    """
    initial_response = session.get(url)
    total_pages = json.loads(initial_response.text)["pagination"]["last_visible_page"]
    return total_pages


def generate_anime_list(session: requests.Session, page_count: int = 0):
    """
    Returns a generator containing pages from the all anime list
    """
    for page_num in range(1, page_count + 1):
        url = f"https://api.jikan.moe/v4/anime?page={page_num}&sfw=true"
        resp = session.get(url)
        if resp.status_code == 200:
            yield json.loads(resp.text)
            time.sleep(1)  # Space out requests to avoid errors
        else:
            print(f"Error received: {resp.status_code}")


def add_anime(anime_list, connection):
    """
    Takes in a generator of pages from the
    generate_anime_list function and a connection,
    and adds any new titles to the database
    """
    try:
        with connection.cursor() as cur:
            for page in anime_list:
                for anime in page["data"]:
                    try:
                        # Insert into database
                        query = """
                            INSERT INTO anime_stage.all_anime
                            (id,title,status,rating,score
                            ,favorites,load_date,airing
                            ,aired_from,aired_to)
                            VALUES
                            (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """
                        cur.execute(
                            query,
                            (
                                anime["mal_id"],
                                anime["title"],
                                anime["status"],
                                anime["rating"],
                                anime["score"],
                                anime["favorites"],
                                datetime.now(),
                                anime["airing"],
                                anime["aired"]["from"],
                                anime["aired"]["to"],
                            ),
                        )
                    except Exception as err:
                        print(f"Error with database connection: {err}")
                        raise
    except Exception as err:
        print(f"Exception occurred while connecting to the database: {err}")
        raise


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


def upload_anime_stats(
    anime_id_list: pd.DataFrame, connection: psycopg2.connect, session: requests.Session
):
    """
    Adds the anime stats for each id
    in the given anime_id_list
    """
    # Setup row counter to perform a commit at every 1000 rows
    row = count(1)
    try:
        with connection.cursor() as cur:
            for id_ in anime_id_list["id"]:
                # Get the stats
                stats = get_anime_stats(session, id_)
                rows = next(row)
                if stats is not None and rows in range(1, 1001):
                    try:
                        insert_stats_scores = """
                            INSERT INTO 
                                anime_stage.anime_stats_scores
                                (anime_id,scores,load_date) 
                            VALUES 
                                (%s,%s,%s)
                        """
                        cur.execute(
                            insert_stats_scores,
                            (id_, json.dumps(stats["data"]), datetime.now()),
                        )
                    except:
                        print("Something happened with the connection.")
                        print("Please check and try again")
                        raise
                else:
                    # Perform commit and reset counter
                    connection.commit()
                    row = count(1)
    except Exception as err:
        print(f"Exception occurred while connecting to the database: {err}")
        raise


def clear_staging(connection: psycopg2.connect):
    """
    Clears the anime_stage tables.
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                TRUNCATE TABLE anime_stage.all_anime;
            """
            )
    except Exception as err:
        print(f"Exception occurred while connecting to the database: {err}")
        raise


def refresh_views(connection: psycopg2.connect):
    """
    Refreshes materialized views
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                REFRESH MATERIALIZED VIEW anime.anime_stats_and_scores;
            """
            )
    except Exception as err:
        print(f"Exception occurred while connecting to the database: {err}")
        raise


def insert_anime_scores_and_stats(connection: psycopg2.connect):
    """
    Inserts anime scores and stats into production
    from staging
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT anime_stage.insert_anime_stats();
                SELECT anime_stage.insert_anime_scores();
            """
            )
    except Exception as err:
        print(f"Exception occurred while connecting to the database: {err}")
        raise
