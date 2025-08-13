from dotenv import load_dotenv
from db_config import DB_CONFIG
from generator import *
from mutator import *
from validator import *
import csv
import jpype
from jpype.types import *
from benchmark import *
from csvWriter import logCSVFile


# Should be driver code but temporarily is set as test bench for debugging
load_dotenv()
            



async def main():
    table_meta_data = await retrieve_metadata()
    gen = QueryGenerator(table_meta_data)
    generate_base_query = await gen.generate_queries(n=10)

    query_mutator_array = []

    for _,query in enumerate(generate_base_query):
        # if "order" not in query and idx 
        parsedQuery = query.replace(";","")
        base, mutant = mutate_query(parsedQuery)
        query_mutator_array.append({"base": base, "mutator": mutant})


    mbq_csv_arr = []

    for row in query_mutator_array:
        for mutator in row['mutator']:
            parsedMutator = " ".join(x for x in (str(mutator).splitlines()))
            mbq_csv_arr.append({"base": row['base'], "mutator": parsedMutator})

    valid_q = await validate_queries(mbq_csv_arr)


    logCSVFile(GENERATOR_FILE,["baseQuery"],generate_base_query)
    logCSVFile("log/mutant_base_queries.csv",["base","mutator"],mbq_csv_arr)
    logCSVFile(LOG_FILE,getTableRow(),valid_q)

    jpype.shutdownJVM()
    


if __name__ == "__main__":
    asyncio.run(main())