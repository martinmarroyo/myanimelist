"""A collection of functions for writing data to the database"""
import psycopg2
from loguru import logger
from sqlalchemy import text
from psycopg2.errors import (
    OperationalError,
    InterfaceError,
    DatabaseError,
    ProgrammingError,
)


async def ingest(data, engine) -> None:
    """ "
    Ingests anime data from the Jikkan MyAnimeList API & writes it
    to a Postgres Database
    """
    logger.info("Starting ingestion...")
    try:
        async with engine.begin() as conn:

            try:
                query = """
                    INSERT INTO anime_stage.all_anime
                    (id,title,status,airing,rating,score,
                    favorites,aired_from,aired_to,load_date)
                    VALUES
                    (:mal_id,:title,:status,:airing,:rating,:score,
                    :favorites,:aired_from,:aired_to,NOW())
                """
                await conn.execute(text(query), parameters=data)
            except (TypeError, OperationalError, InterfaceError):
                logger.exception("Error occurred during query...")

    except (DatabaseError, OperationalError, ProgrammingError):
        logger.exception("Error occurred while connecting to the database")
    logger.info("Written to database!")


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
