import asyncio
import os
import asyncpg
from dotenv import load_dotenv


load_dotenv()

connFinStr = "sslmode=require&channel_binding=require"
connBegStr = "postgresql://"

async def run():
    # print(os.getenv("TEST"))
    # connStr = connFinStr + os.getenv("USER") + ":" + os.getenv("KEY") + "@" + os.getenv("HOST") + "/" + os.getenv("DB") + connFinStr
    dbConn = None

    try:
        dbConn = await asyncpg.connect(os.getenv("DATABASE_URL"))

        res = await dbConn.fetch("SELECT * FROM pokemon")

        for row in res:
            print(row)

    except Exception as e:
        print("ERROR!")
        print(e)
    
    finally:
        await dbConn.close()

asyncio.run(run())