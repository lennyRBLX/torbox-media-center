import os
from dotenv import load_dotenv

load_dotenv()

TORBOX_API_KEY = os.getenv("TORBOX_API_KEY")
assert TORBOX_API_KEY, "TORBOX_API_KEY is not set in .env file"