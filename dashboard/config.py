"""Runtime configuration (environment variables) and dashboard constants."""

import os
import re

# --- CONFIGURATION (environment variables; the defaults work when running outside Docker) ---
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("MONGO_DB", "bigdata_db")
COLLECTION_NAME = os.getenv("MONGO_HISTORY_COLLECTION", "gpu_history")  # raw history written by Spark
AGG_COLLECTION = os.getenv("MONGO_AGG_COLLECTION", "gpu_aggregates")  # windowed aggregates written by Spark
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "gpu_stream")
AGG_WINDOW = os.getenv("AGG_WINDOW", "1 minute")

LIVE_MESSAGES = 120  # how many of the latest Kafka messages the live view shows
PAGE_SIZE = 100  # rows per page in the history table
HEALTHY_SECONDS = 120  # pipeline counts as healthy if Spark wrote something this recently

REALTIME_REFRESH_MS = 15_000  # dashboard refresh (15s minimum)
HISTORICAL_REFRESH_MS = 60_000  # historical data changes slowly
SLIDE_SECONDS = 12  # slideshow interval
SLIDE_HEIGHT = 260

# Slideshow images: MediaWiki API (public-apis > "Wikipedia", no API key) pointed at Wikimedia Commons.
COMMONS_API = "https://commons.wikimedia.org/w/api.php"
SLIDE_QUERIES = [
    "NVIDIA GeForce graphics card",
    "AMD Radeon graphics card",
    "GeForce RTX",
    "graphics processing unit",
    "GPU die",
    "video card",
]
SLIDES_PER_QUERY = 4
# A file title must look GPU-related to become a slide
GPU_TITLE = re.compile(
    r"\b(gpus?|geforce|radeon|nvidia|rtx|gtx|quadro|video ?cards?|"
    r"graphics (card|cards|processing|processor)|[ahv]100|rx ?\d{3,4})\b"
)

MAIN_H = 500  # height of the main chart row
BOTTOM_H = 340  # height of the lower card rows
