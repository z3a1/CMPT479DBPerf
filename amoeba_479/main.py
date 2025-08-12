import asyncio
import os
import asyncpg
from dotenv import load_dotenv
from db_config import DB_CONFIG
from generator import *
import csv


# Should be driver code but temporarily is set as test bench for debugging
load_dotenv()

async def main():
    table_meta_data = await retrieve_metadata()
    gen = QueryGenerator(table_meta_data)
    base_queries = await gen.generate_queries(n=10)
    with open(GENERATOR_FILE, "w", newline = '') as csvfile:
        logger = csv.writer(csvfile)
        logger.writerow(["basequery"])
        for query in base_queries:
            logger.writerow(query)

asyncio.run(main())