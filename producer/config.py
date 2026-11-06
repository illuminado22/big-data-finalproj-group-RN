"""Producer configuration (environment variables; a .env file is loaded if present)."""

import os

from dotenv import load_dotenv

load_dotenv()

API_URL = os.getenv("AQUANODE_API_URL", "https://server.aquanode.io/api/v1/marketplace")
API_KEY = os.getenv("AQUANODE_API_KEY", "")  # the public marketplace endpoint works without it
