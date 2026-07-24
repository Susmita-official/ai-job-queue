import os
from dotenv import load_dotenv

# This tells Python to look for the hidden .env file and load the secrets inside it
load_dotenv()

# We grab the key and store it in a variable our app can use
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not OPENAI_API_KEY:
    print("WARNING: No OpenAI API key found in .env file!")