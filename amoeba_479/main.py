import asyncio
import os
import asyncpg
from dotenv import load_dotenv
from db_config import DB_CONFIG




# Should be driver code but temporarily is set as test bench for debugging
load_dotenv()

async def run():
    connStr = os.getenv("connBegstr") + os.getenv("USER") + ":" + os.getenv("KEY") + "@" + os.getenv("HOST") + "/" + os.getenv("DB") + "?" + os.getenv("connFinStr")
    dbConn = None
    try:
        dbConn = await asyncpg.create_pool(**DB_CONFIG)

        res = await dbConn.fetch("SELECT * FROM pokemon INNER JOIN pokemon_types ON pokemon.id = pokemon_types.pokemon_id")

        for row in res:
            print(row)

    except Exception as e:
        print("ERROR!")
        print(e)
    
    finally:
        await dbConn.close()

asyncio.run(run())