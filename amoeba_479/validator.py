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

async def validate_query(base_query, mutator_query, pool):
    try:
        start_time = time.time()
        baseRes = []
        mutatorRes = []
        async with pool.acquire() as conn:
            baseRes = await conn.fetch(base_query)
        
        mutator_start_time = time.time()

        async with pool.acquire() as conn:
            mutatorRes = await conn.fetch(mutator_query)
        
        return baseRes == mutatorRes, None, start_time, mutator_start_time

    except Exception as e:
        return False, str(e), 0, 0  # invalid query

async def validate_queries(queries):
    os.makedirs("log", exist_ok=True)
    pool = await asyncpg.create_pool(os.getenv("DATABASE_URL"))
    results = []

    for query in queries:
        is_valid, error, start_time, mutator_start_time = await validate_query(query["base"],query["mutator"] ,pool)
        baseLatency = time.time() - start_time
        mutatorLatency = time.time() - mutator_start_time
        # print(baseResSize,mutatorResSize,time.time())
        results.append((datetime.now(UTC),query["base"],query["mutator"] ,is_valid, error, baseLatency, mutatorLatency))

    await pool.close()

    # Return only valid queries
    return [[t,q,mq,v,e,bl,ml] for t,q, mq, v, e, bl, ml, in results if v] , ["timestamp","base","mutator","valid","error","baseLatency","mutatorLatency"]
