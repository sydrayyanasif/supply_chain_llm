import os
from dotenv import load_dotenv

def load_env():
    load_dotenv()
    return {
        "GOOGLE_API_KEY": os.getenv("GOOGLE_API_KEY")
    }
