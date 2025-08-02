# validator.py

import asyncpg
import asyncio
import csv
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

LOG_FILE = "log/reports.csv"

async def validate_query(query, pool):
    try:
        async with pool.acquire() as conn:
            await conn.fetch(query)
        return True, None  # valid query
    except Exception as e:
        return False, str(e)  # invalid query

async def validate_queries(queries):
    os.makedirs("log", exist_ok=True)
    pool = await asyncpg.create_pool(os.getenv("DATABASE_URL"))
    results = []

    for query in queries:
        is_valid, error = await validate_query(query, pool)
        results.append((query, is_valid, error))

    await pool.close()

    # Save results
    with open(LOG_FILE, "w", newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["timestamp", "query", "valid", "error"])
        for query, valid, error in results:
            writer.writerow([
                datetime.utcnow().isoformat(),
                query,
                valid,
                error if error else ""
            ])

    # Return only valid queries
    return [q for q, v, _ in results if v]
