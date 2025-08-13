# validator.py

import asyncpg
import asyncio
import csv
import os
from datetime import datetime, UTC
import time
from db_config import DB_CONFIG  # assuming this has host, user, password, etc.
from dotenv import load_dotenv
load_dotenv()

LOG_FILE = "log/reports.csv"

def getTableRow():
    return ["timestamp","base","mutator","valid","error"]

async def validate_query(base_query, mutator_query, pool):
    try:
        baseRes = []
        mutatorRes = []
        async with pool.acquire() as conn:
            baseRes = await conn.fetch(base_query)
        async with pool.acquire() as conn:
            mutatorRes = await conn.fetch(mutator_query)
        
        return baseRes == mutatorRes, None

    except Exception as e:
        return False, str(e)  # invalid query

async def validate_queries(queries):
    os.makedirs("log", exist_ok=True)
    pool = await asyncpg.create_pool(os.getenv("DATABASE_URL"))
    results = []

    for query in queries:
        is_valid, error= await validate_query(query["base"],query["mutator"] ,pool)
        # print(baseResSize,mutatorResSize,time.time())
        results.append((datetime.now(UTC),query["base"],query["mutator"] ,is_valid, error))

    await pool.close()

    # Return only valid queries
    return [[t,q,mq,v,e] for t,q, mq, v, e in results if v]
