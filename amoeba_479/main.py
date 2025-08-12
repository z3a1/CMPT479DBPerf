import asyncio
import os
import asyncpg
from dotenv import load_dotenv
from db_config import DB_CONFIG
from generator import *
from mutator import *
from validator import *
import csv
import jpype
import jpype.imports
from jpype.types import *


# Should be driver code but temporarily is set as test bench for debugging
load_dotenv()


def logCSVFIle(fileName,labelRow ,data):
    with open(fileName, "w", newline = '') as csvFile:
        logger = csv.writer(csvFile)
        logger.writerow(labelRow)
        for row in data:
            if isinstance(row,dict):
                logger.writerow([row['base'], row['mutator']])
            elif isinstance(row,list):
                logger.writerow(row)
            else:
                logger.writerow([row])
            



async def main():
    table_meta_data = await retrieve_metadata()
    gen = QueryGenerator(table_meta_data)
    generate_base_query = await gen.generate_queries(n=10)

    query_mutator_array = []

    for _,query in enumerate(generate_base_query):
        # if "order" not in query and idx 
        parsedQuery = query.replace(";","")
        base, mutant = mutate_query(parsedQuery)
        # res = "".join(x for x in mutant)
        parsedMutantQuery = "".join(str(x + " ") for x in str(mutant[0]).splitlines()).rstrip()
        query_mutator_array.append({"base": base, "mutator": parsedMutantQuery})

    validQueries, labelRow = await validate_queries(query_mutator_array)
    
    jpype.shutdownJVM()

    logCSVFIle(GENERATOR_FILE,["baseQuery"],generate_base_query)
    logCSVFIle("log/mutant_base_queries.csv",["base","mutant"],query_mutator_array)
    logCSVFIle(LOG_FILE,labelRow,validQueries)


asyncio.run(main())