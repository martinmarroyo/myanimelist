"""A script to initialize the MyAnimeList database"""
import os
import time
import psycopg2
import logging
from glob import glob
from dotenv import dotenv_values

# Initialize configuration & logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s:%(module)s: %(message)s",
) 
config = dotenv_values('.env')

def create_schemas(connection: psycopg2.extensions.connection) -> None:
    logging.info("Creating schemas...")
    with open('create_schemas.sql', 'r', encoding='utf-8') as query:
        query = query.read()
        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
            logging.info("Schemas created successfully!")
        except:
            logging.exception("Issue occurred while creating schemas")
            raise


def create_staging(connection: psycopg2.extensions.connection) -> None:
    stage_scripts = glob(os.path.normpath("table/stage/*.sql"))
    # Open a connection, loop through each script, and execute it
    logging.info("Creating staging...")
    try:
        with connection.cursor() as cursor:
            for script in stage_scripts:
                # Open the script
                with open(script, 'r', encoding='utf-8') as query:
                    query = query.read()
                    try:
                        cursor.execute(query)
                    except:
                        logging.exception(f"Error occurred while running script:\n{query}")
                        raise
        logging.info("Staging created successfully!")
    except:
        logging.exception("Error connecting to database")
        raise


def create_production(connection: psycopg2.extensions.connection) -> None:
    prod_scripts = glob(os.path.normpath("table/prod/*.sql"))
    # Open a connection, loop through each script, and execute it
    logging.info("Creating production...")
    try:
        with connection.cursor() as cursor:
            for script in prod_scripts:
                # Open the script
                with open(script, 'r', encoding='utf-8') as query:
                    query = query.read()
                    try:
                        cursor.execute(query)
                    except:
                        logging.exception(f"Error occurred while running script:\n{query}")
                        raise
            logging.info("Production created successfully!")
    except:
        logging.exception("Error connecting to database")
        raise


def initialize_views(connection: psycopg2.extensions.connection) -> None:
    view_scripts = glob(os.path.normpath("view/*.sql"))
    logging.info("Initializing views...")
    try:
        with connection.cursor() as cursor:
            for script in view_scripts:
                # Open the script
                with open(script, 'r', encoding='utf-8') as query:
                    query = query.read()
                    try:
                        cursor.execute(query)
                    except:
                        logging.exception(f"Error occurred while running script:\n{query}")
                        raise
            logging.info("Views created successfully!")
    except:
        logging.exception("Error connecting to database")
        raise


def initdb(config):
    logging.info("Initializing database...")
    with psycopg2.connect(**config) as connection:
        create_schemas(connection)
        create_staging(connection)
        create_production(connection)
        initialize_views(connection)
    logging.info("Database initialization complete!")


if __name__ == '__main__':
    start = time.time()
    initdb(config)
    end = time.time()
    duration = round(end-start, 2)
    logging.info(f"Total duration: {duration} second(s)")