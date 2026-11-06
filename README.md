# RN · GPU Market Analytics

Real-time and historical dashboard for cloud GPU prices, built on a **MongoDB – Kafka – Spark** streaming pipeline.

```
Aquanode API ─► producer ─► Kafka (gpu_stream) ─┬─► Spark Structured Streaming ─► MongoDB ─► Streamlit (History)
                                                   │        ├─ gpu_history     (every record)        │
                                                   │        └─ gpu_aggregates  (1-minute windows)    │
                                                   └──────────────────────────────────────────────► Streamlit (Live, 15 s)
```

## Run it (Docker)

Requirements: Docker Desktop (or Docker Engine + Compose v2.24+) with at least **4 GB RAM** allotted to Docker
(Kafka, Spark and MongoDB all run at the same time).

```bash
cp .env.example .env        # optional: put your Aquanode API key in it
docker compose up --build
```

Open **http://localhost:8501**. The first start takes 1–2 minutes (Kafka and Spark start up); the Historical
tab fills after the first Spark batch (about 30 s after the producer's first poll).

| Service | What it does |
|---|---|
| `kafka` / `kafka-init` | Apache Kafka 3.7 (KRaft, no ZooKeeper) and creation of the `gpu_stream` topic |
| `producer` | Calls the Aquanode marketplace API every 15 s and publishes the best price per GPU model |
| `spark` | Structured Streaming job: Kafka → MongoDB (`gpu_history`, `gpu_aggregates`) |
| `mongo` | MongoDB 7 (data kept in a Docker volume) |
| `app` | Streamlit dashboard |

Useful commands:

```bash
docker compose logs -f spark      # watch Spark write batches
docker compose logs -f producer   # watch the API polls
docker compose down               # stop (data is kept)
docker compose down -v            # stop and delete all stored data
```

MongoDB is also published on `localhost:27017` (inspect with MongoDB Compass) and Kafka on `localhost:29092`.

## Dashboard

* **Real-time** – refreshes every 15 s from Kafka: market KPIs, per-GPU % change chart (or a single GPU's price with last-price tag), live feed, aggregation panel.
* **Historical** – everything Spark stored in MongoDB: pipeline health, all records (paginated), filters (GPU / provider / range), Spark 1-minute aggregates (avg with min–max band), data aggregation panel.
* **Export** – Excel (.xlsx, with native charts), PDF report, chart image (.jpg), CSV. Press *Prepare files*, then download.
* Light / dark mode switch (top right), GPU photo slideshow (Wikimedia Commons, 12 s, drag to slide).

## Configuration (environment variables)

| Variable | Default | Used by |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | producer, spark, app |
| `KAFKA_TOPIC` | `gpu_stream` | producer, spark, app |
| `MONGO_URI` / `MONGO_DB` | `mongodb://localhost:27017/` / `bigdata_db` | spark, app |
| `AQUANODE_API_KEY` | *(empty)* | producer (`.env`) |
| `PRODUCER_INTERVAL` / `PRODUCER_GPU_LIMIT` | `15` / `8` | producer |
| `AGG_WINDOW` | `1 minute` | spark, app |

## Run without Docker

Needs a local Kafka and MongoDB (and Java 17 for Spark).

```bash
pip install -r requirements/app.txt -r requirements/spark.txt
python -m producer                 # terminal 1
python -m streaming                # terminal 2
streamlit run app.py               # terminal 3  (not "python app.py")
```

## Project structure

```
app.py                     Streamlit entry point (streamlit run app.py)
dashboard/                 Web UI
  config.py                  environment variables and UI constants
  theme.py                   light / dark palettes and CSS
  data.py                    Kafka (live) and MongoDB (history) queries
  slideshow.py               GPU photo slideshow (Wikimedia Commons)
  components.py              cards, KPIs, tables, panels
  charts.py                  Plotly figures
  exports.py                 Excel / PDF / JPG export
  main.py                    page layout: top bar, slideshow, tab dispatch
  views/realtime.py          Real-time tab
  views/historical.py        Historical tab
producer/                  Aquanode API → Kafka   (python -m producer)
streaming/                 Spark job: Kafka → MongoDB   (python -m streaming)
  transform.py               parsing and 1-minute window aggregation
  mongo_sink.py              idempotent MongoDB upserts, indexes
  job.py                     wiring of the streaming queries
docker/                    app.Dockerfile (dashboard + producer), spark.Dockerfile
requirements/              app.txt, spark.txt
docker-compose.yml         the whole stack
docs/DISCUSSION.md         draft text: API, pipeline and significance (for the Activity Report)
```
