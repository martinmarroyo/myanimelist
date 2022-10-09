"""A script to initialize the MyAnimeList database"""
import os
import time
import psycopg2
from glob import glob
from loguru import logger
from dotenv import dotenv_values
from psycopg2.errors import DatabaseError, OperationalError, ProgrammingError


def create_schemas(connection: psycopg2.extensions.connection) -> None:
    """
    Runs the script to create the `anime` and  `anime_stage` schemas.
    The script also enables the use of the `crosstab` function.
    """
    logger.info("Creating schemas...")
    try:
        with open("create_schemas.sql", "r", encoding="utf-8") as query:
            query = query.read()
            try:
                with connection.cursor() as cursor:
                    cursor.execute(query)
                logger.info("Schemas created successfully!")
            except (DatabaseError, OperationalError):
                logger.exception("Issue occurred while creating schemas:\n")
                raise
    except FileNotFoundError:
        logger.exception(
            "Cannot find the `create_schemas.sql` file. Is it in this directory?"
        )
        raise


def create_staging(connection: psycopg2.extensions.connection) -> None:
    """
    Runs the script to create the `anime_stage` schema tables.
    """
    stage_scripts = glob(os.path.normpath("table/stage/*.sql"))
    # Open a connection, loop through each script, and execute it
    logger.info("Creating staging...")
    try:
        with connection.cursor() as cursor:
            for script in stage_scripts:
                # Open the script
                with open(script, "r", encoding="utf-8") as query:
                    query = query.read()
                    try:
                        cursor.execute(query)
                    except (DatabaseError, OperationalError):
                        logger.exception(
                            f"Error occurred while running script:\n{query}\n"
                        )
                        raise
        logger.info("Staging created successfully!")
    except (DatabaseError, OperationalError):
        logger.exception("Error connecting to database")
        raise


def create_production(connection: psycopg2.extensions.connection) -> None:
    """
    Runs the script to create the `anime` schema tables.
    """
    prod_scripts = glob(os.path.normpath("table/prod/*.sql"))
    # Open a connection, loop through each script, and execute it
    logger.info("Creating production...")
    try:
        with connection.cursor() as cursor:
            for script in prod_scripts:
                # Open the script
                with open(script, "r", encoding="utf-8") as query:
                    query = query.read()
                    try:
                        cursor.execute(query)
                    except (DatabaseError, OperationalError):
                        logger.exception(
                            f"Error occurred while running script:\n{query}"
                        )
                        raise
            logger.info("Production created successfully!")
    except (DatabaseError, OperationalError):
        logger.exception("Error connecting to database")
        raise


def initialize_views(connection: psycopg2.extensions.connection) -> None:
    """
    Runs the script to create predefined materialized views.
    """
    view_scripts = glob(os.path.normpath("view/*.sql"))
    logger.info("Initializing views...")
    try:
        with connection.cursor() as cursor:
            for script in view_scripts:
                # Open the script
                with open(script, "r", encoding="utf-8") as query:
                    query = query.read()
                    try:
                        cursor.execute(query)
                    except (DatabaseError, OperationalError):
                        logger.exception(
                            f"Error occurred while running script:\n{query}"
                        )
                        raise
            logger.info("Views created successfully!")
    except (DatabaseError, OperationalError):
        logger.exception("Error connecting to database")
        raise


def initdb(config: dict) -> None:
    """
    A wrapper function that runs the database initialization process.
    This is a destructive process, as each time it runs it will delete
    everything existing inside the `anime` and `anime_stage` schemas before
    recreating the schema structure in the database.
    """
    logger.info("Initializing database...")
    try:
        with psycopg2.connect(**config) as connection:
            create_schemas(connection)
            create_staging(connection)
            create_production(connection)
            initialize_views(connection)
        logger.info("Database initialization complete!")
    except (ProgrammingError, DatabaseError, OperationalError):
        logger.exception("Error connecting to the database:\n")
        raise


if __name__ == "__main__":
    # Initialize configuration
    dbconfig = dotenv_values("dbenv")
    start = time.time()
    initdb(dbconfig)
    end = time.time()
    duration = round(end - start, 2)
    logger.info(f"Total duration: {duration} second(s)")
