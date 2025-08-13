
import asyncpg
import asyncio
import csv
import os
from datetime import datetime, UTC
import time
from dotenv import load_dotenv
from csvWriter import logCSVFile
import statistics
import cpuinfo
import psutil

load_dotenv()

system_cpu_info = {
    "logical_cores": psutil.cpu_count(logical=True),
    "physical_cores": psutil.cpu_count(logical=False),
    "cpu_name": cpuinfo.get_cpu_info()['brand_raw']
}

# Run the query and log the time it took for the query to run
# On top of displaying the volume of data and the CPU usage
async def run_query(query,pool):
    start_time = time.time()
    try:
        async with pool.acquire() as conn:
            res = await conn.fetch(query)
            # return [{"latency": time.time() - start_time, "transaction_length": len(res), "cpu_usage": psutil.cpu_percent(interval=0.5)}]
            return [time.time() - start_time, len(res), psutil.cpu_percent(interval=0.5)]
    except:
        return None

# Run query n number of times
async def benchmark_query(query,thread_count=10,n=50):

    pool = await asyncpg.create_pool(os.getenv("DATABASE_URL"))
    initial_time = time.time()
    query_execution_count = 0
    query_iteration_array = []

    #While the time frame is less than the thresh hold, we run the concurrency function
    while time.time() - initial_time < n:
        # Create separate threads for the given query and pass the pool connection to it
        # Thread count can also be seen as the how many are we running concurrently at the same time
        current_tasks = [currQuery for currQuery in ((run_query(query=query,pool=pool)) for _ in range(thread_count)) if currQuery is not None]
        results = await asyncio.gather(*current_tasks)
        query_iteration_array.extend(results) 
        query_execution_count += thread_count

    await pool.close()

    throughput = query_execution_count / n

    return query_iteration_array , throughput


# Double check the query does not have any extraneous quotation marks or ; at the end
def sanitize_query(query):
    return query.replace("\"","").replace(";","")

        

# USAGE EXAMPLE, read either base queries or from the mutant_base_queries.csv file
BASE_QUERIES = [
    "SELECT t0.move_id, t0.priority FROM pokemon_moves t0 WHERE t0.level > 0 GROUP BY t0.move_id, t0.priority",
    "SELECT t0.slot, t1.damage_type_id FROM (pokemon_types t0 LEFT JOIN type_efficacy t1 ON TRUE) WHERE t0.type_id > 0 AND t1.damage_type_id > 0;",
    "SELECT t0.priority FROM (pokemon t0 LEFT JOIN moves t1 ON TRUE) WHERE t1.effect_id > 0 GROUP BY t0.priority;"
]

def parseBenchmarkRows(benchmark_array, query,throughput):
    parsed_array = []
    for row in benchmark_array:
        row.insert(1,query)
        row.extend(system_cpu_info.values())
        row.append(throughput)
        parsed_array.append(row)
    return parsed_array

async def main():
    query_arr = []
    with open('log/reports.csv', mode='r') as file:
        reader = csv.reader(file)
        for row in reader:
            query_arr.append(row)

    del query_arr[0]

    benchmark_row_data = []

    counter = 0
    for query_pair in query_arr:
        print(f"iteration: {counter}")
        baseq_bm = query_pair[1]
        mutantq_bm = query_pair[2]
        base_bm_res, bt = await benchmark_query(baseq_bm)
        mutant_bm_res, mt = await benchmark_query(mutantq_bm)
        print(f'Base Res Length: {len(base_bm_res)} Mutant Res Length: {len(mutant_bm_res)}')
        if base_bm_res and mutant_bm_res:
            benchmark_row_data.extend(parseBenchmarkRows(base_bm_res,baseq_bm,bt))
            benchmark_row_data.extend(parseBenchmarkRows(mutant_bm_res,mutantq_bm,mt))
            counter += 1
        else:
            print("ERROR")
            break

    logCSVFile("log/base_benchmarks.csv",["query","latency","transaction_rows","cpu_usage","cpu_logical_cores","cpu_physical_cores","cpu_name","query_throughput"],benchmark_row_data)

if __name__ == "__main__":
    asyncio.run(main())