from dotenv import load_dotenv
import os

load_dotenv()

DB_CONFIG = {
    "user": os.getenv("USER"),
    "password": os.getenv("KEY"),
    "host": os.getenv("HOST"),
    "database": os.getenv("DB")
}