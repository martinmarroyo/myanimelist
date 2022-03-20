import requests
import json
import time
import timeit
import psycopg2
import logging
import myanimelist_api_functions
import pandas as pd
from itertools import count
from datetime import datetime
from dotenv import dotenv_values
from psycopg2 import pool

if __name__ == '__main__':


    conf = dotenv_values('.env')
    logging.basicConfig(level=logging.INFO,
                        filename="animelistapi.log",
                        encoding='utf-8',
                        format='%(asctime)s:%(levelname)s:%(message)s')


    logging.info("Starting...")
    start = time.time()

    # Set up connections to database and API
    connection_pool = psycopg2.pool.SimpleConnectionPool(1,5,
                                                    user=conf['USER'],
                                                    password=conf['PASS'],
                                                    host=conf['HOST'],
                                                    port=conf['PORT'],
                                                    database=conf['DB_NAME']
                                                )
    try:
        
        # Get connection
        connection = connection_pool.getconn()

        # Get data from API requests and insert into DB
        with requests.Session() as session:
            
            # Get all the anime updates
            start_anime_list = time.time()
            url = "https://api.jikan.moe/v4/anime?sfw=true"
            total_pages = get_page_count(url,session)
            anime_list = generate_anime_list(session,total_pages)
            add_anime(anime_list,connection)
            connection.commit()
            end_anime_list = time.time()
            logging.info(f"Anime list ETL complete!")
            logging.info(f"Elapsed time was {end_anime_list-start_anime_list} seconds")
            
            # Get all the anime stat updates
            start_anime_stats = time.time()
            anime_ids = get_anime_ids(connection)
            upload_anime_stats(anime_ids,connection,session)
            connection.commit()
            end_anime_stats = time.time()
            logging.info(f"Anime stats upload complete!")
            logging.info(f"Elapsed time was {end_anime_stats-start_anime_stats} seconds")
            
        # Commit all changes and clean up staging 
        insert_anime_scores_and_stats(connection)
        refresh_views(connection)
        clear_staging(connection)
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as err:
        logging.info("An error occurred: ",err)

    finally:
        connection.close()
        
    end = time.time()
    logging.info(f"Done! Elapsed time was {end-start} seconds")