import html as html_lib
import json
import math
import os
import re
from datetime import datetime, timedelta, timezone

import pandas as pd
import plotly.graph_objects as go
import requests
import streamlit as st
import streamlit.components.v1 as components
from kafka import KafkaConsumer, TopicPartition
from pymongo import MongoClient
from streamlit_autorefresh import st_autorefresh

from exports import build_pdf, build_xlsx, chart_image

# --- CONFIGURATION (environment variables; the defaults work when running outside Docker) ---
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("MONGO_DB", "bigdata_db")
COLLECTION_NAME = os.getenv("MONGO_HISTORY_COLLECTION", "gpu_history")        # raw history written by Spark
AGG_COLLECTION = os.getenv("MONGO_AGG_COLLECTION", "gpu_aggregates")          # windowed aggregates written by Spark
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "gpu_stream")
AGG_WINDOW = os.getenv("AGG_WINDOW", "1 minute")

LIVE_MESSAGES = 120               # how many of the latest Kafka messages the live view shows
PAGE_SIZE = 100                   # rows per page in the history table
HEALTHY_SECONDS = 120             # pipeline counts as healthy if Spark wrote something this recently

REALTIME_REFRESH_MS = 10_000    # dashboard refresh (15s minimum)
HISTORICAL_REFRESH_MS = 60_000    # historical data changes slowly
SLIDE_SECONDS = 12                # slideshow interval
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

# --- THEMES (deep-space navy + violet/cyan, matching the GPU / cloud-compute feel) ---
PALETTES = {
    "Dark": dict(
        bg="#050816", text="#E6EAF5", muted="#8A94B3", card="rgba(255,255,255,0.045)",
        card_solid="#0E1330", border="rgba(255,255,255,0.09)", soft="rgba(255,255,255,0.055)",
        grid="rgba(255,255,255,0.06)", spike="rgba(190,200,230,0.45)",
        violet="#8B5CF6", cyan="#22D3EE", green="#34D399", orange="#FC5A3C",
        pink="#F472B6", amber="#FBBF24", blue="#60A5FA", lime="#A3E635",
        glow1="rgba(139,92,246,0.28)", glow2="rgba(34,211,238,0.16)", glow3="rgba(252,61,33,0.08)",
        shadow="0 8px 32px rgba(0,0,0,0.28)", plotly="plotly_dark", tip_bg="#0B1130", tag_text="#050816",
    ),
    "Light": dict(
        bg="#F2F4FB", text="#141A33", muted="#5B6684", card="rgba(255,255,255,0.82)",
        card_solid="#FFFFFF", border="rgba(20,26,51,0.09)", soft="rgba(20,26,51,0.05)",
        grid="rgba(20,26,51,0.07)", spike="rgba(20,26,51,0.40)",
        violet="#7C3AED", cyan="#0891B2", green="#059669", orange="#E5341A",
        pink="#DB2777", amber="#D97706", blue="#2563EB", lime="#65A30D",
        glow1="rgba(124,58,237,0.16)", glow2="rgba(8,145,178,0.12)", glow3="rgba(229,52,26,0.05)",
        shadow="0 8px 28px rgba(30,41,90,0.10)", plotly="plotly_white", tip_bg="#FFFFFF", tag_text="#FFFFFF",
    ),
}
THEME_LABELS = {"Light": "☀️ Light", "Dark": "🌙 Dark"}
VIEW_LABELS = ["Real-time", "Historical"]
PLOT_CONFIG = {"displayModeBar": False}

st.set_page_config(page_title="RN · GPU Market", layout="wide", initial_sidebar_state="collapsed")


def current_theme():
    label = st.session_state.get("theme_radio", THEME_LABELS["Dark"])
    return "Light" if label == THEME_LABELS["Light"] else "Dark"


P = PALETTES[current_theme()]
SERIES = [P["cyan"], P["violet"], P["pink"], P["amber"], P["green"], P["blue"], P["orange"], P["lime"]]

# --- STYLING ---
CSS_STATIC = """
html, body, .stApp, [class*="css"] { font-family: 'Poppins', sans-serif; }
.stApp {
    color: var(--text);
    background:
        radial-gradient(900px 520px at 6% -6%, var(--glow1), transparent 60%),
        radial-gradient(800px 520px at 100% 0%, var(--glow2), transparent 55%),
        radial-gradient(700px 600px at 50% 112%, var(--glow3), transparent 60%),
        var(--bg);
    background-attachment: fixed;
}
header[data-testid="stHeader"], footer, #MainMenu, section[data-testid="stSidebar"],
div[data-testid="stSidebarCollapsedControl"], div[data-testid="stToolbar"] { display: none !important; }
.block-container { max-width: 100% !important; padding: 0.9rem 1.75rem 1.4rem !important; }
div[data-testid="stVerticalBlock"] { gap: 0.85rem; }
div[data-testid="stHorizontalBlock"] { gap: 0.85rem; }
h1, h2, h3, h4, p, label, span, li { color: var(--text); }
*::-webkit-scrollbar { width: 6px; height: 6px; }
*::-webkit-scrollbar-thumb { background: var(--border); border-radius: 6px; }

/* st_autorefresh iframe: remove the empty gap it leaves */
.st-key-rt_refresh, .st-key-hist_refresh {
    position: absolute !important; height: 0 !important; width: 0 !important; margin: 0 !important; overflow: hidden;
}

/* Plotly draws a bold white halo (thickness + 2px) under every crosshair line: hide it so only the hairline stays */
.js-plotly-plot .spikeline:not([stroke-dasharray]), .js-plotly-plot .spikeline[stroke="rgb(255, 255, 255)"] { stroke: transparent !important; }

/* Cards */
[class*="st-key-card_"] {
    background: var(--card); border: 1px solid var(--border); border-radius: 18px;
    box-shadow: var(--shadow); backdrop-filter: blur(14px); padding: 14px 18px !important;
}
.card-title { font-size: 15px; font-weight: 600; color: var(--text); margin: 0 0 8px; }
.card-sub { font-size: 12px; color: var(--muted); font-weight: 400; margin-left: 8px; }

/* Brand */
.rn-mark {
    position: relative; width: 48px; height: 48px; border-radius: 15px; display: flex;
    align-items: center; justify-content: center; font-weight: 800; font-size: 19px;
    letter-spacing: -0.5px; color: #fff;
    background: linear-gradient(135deg, var(--violet) 0%, var(--cyan) 100%);
    box-shadow: 0 8px 24px var(--glow1), inset 0 1px 0 rgba(255,255,255,0.35);
}
.rn-mark i {
    position: absolute; top: -3px; right: -3px; width: 11px; height: 11px; border-radius: 50%;
    background: var(--green); border: 2px solid var(--bg); box-shadow: 0 0 10px var(--green);
}

/* Pill toggles (view tabs + theme switch) */
div[role="radiogroup"] {
    background: var(--soft); border: 1px solid var(--border); border-radius: 999px;
    padding: 4px; gap: 2px; width: fit-content; flex-wrap: nowrap;
}
div[role="radiogroup"] label { padding: 7px 24px; border-radius: 999px; margin: 0 !important; cursor: pointer; transition: all .2s; }
div[role="radiogroup"] label[data-testid="stRadioOption"] > div > div:first-child:not([data-testid="stMarkdownContainer"]),
div[role="radiogroup"] label > div:first-child:not(:has(p)):not([data-testid="stMarkdownContainer"]) { display: none !important; }
div[role="radiogroup"] label:has(input:checked) {
    background: linear-gradient(135deg, var(--violet), var(--cyan)); box-shadow: 0 4px 14px var(--glow1);
}
div[role="radiogroup"] label p { font-size: 13px; margin: 0; color: var(--muted); white-space: nowrap; }
div[role="radiogroup"] label:has(input:checked) p { color: #fff !important; font-weight: 600; }
.st-key-view_radio { width: 100% !important; display: flex; justify-content: center; }
.st-key-theme_radio { width: 100% !important; display: flex; justify-content: flex-end; }
.st-key-view_radio div[role="radiogroup"] label { padding: 9px 38px; }
.st-key-view_radio div[role="radiogroup"] label p { font-size: 14px; }
.st-key-range_radio div[role="radiogroup"] label { padding: 6px 14px; }

/* Inputs */
div[data-baseweb="select"] > div, [data-testid="stSelectbox"] > div > div, [data-testid="stMultiSelect"] > div > div {
    background: var(--soft) !important; border: 1px solid var(--border) !important;
    border-radius: 12px !important; color: var(--text) !important;
}
[data-testid="stSelectbox"] input, [data-testid="stMultiSelect"] input, [data-testid="stSelectbox"] button,
[data-testid="stSelectbox"] [class*="singleValue"], [data-testid="stSelectbox"] [class*="value"] {
    color: var(--text) !important; -webkit-text-fill-color: var(--text) !important;
}
[data-testid="stSelectbox"] svg, [data-testid="stMultiSelect"] svg { fill: var(--muted) !important; color: var(--muted) !important; }
div[data-baseweb="select"] * { color: var(--text) !important; }
div[data-baseweb="select"] svg { fill: var(--muted) !important; }
span[data-baseweb="tag"], span[data-tag] { background: linear-gradient(135deg, var(--violet), var(--cyan)) !important; border-radius: 8px !important; }
span[data-baseweb="tag"] *, span[data-tag] * { color: #fff !important; }
.st-key-csv_dl { width: 100% !important; display: flex; justify-content: flex-end; }
div[data-baseweb="popover"] div, ul[role="listbox"], div[data-baseweb="menu"], [data-testid="stSelectboxVirtualDropdown"],
[data-testid="stSelectboxVirtualDropdown"] > div { background: var(--card_solid) !important; }
[role="option"], [role="option"] * { color: var(--text) !important; }
[role="option"]:hover, [role="option"][aria-selected="true"], [role="option"][data-highlighted] { background: var(--soft) !important; }
div[data-testid="stWidgetLabel"] p { color: var(--muted); font-size: 12px; }
.stDownloadButton button, .stButton button {
    background: var(--soft); color: var(--text); border: 1px solid var(--border); border-radius: 999px; padding: 4px 16px;
}
.stDownloadButton button p, .stButton button p { color: var(--text); font-size: 13px; }
.stDownloadButton button:hover, .stButton button:hover { border-color: var(--cyan); color: var(--cyan); }
.stDownloadButton button:hover p, .stButton button:hover p { color: var(--cyan); }
[class*="st-key-card_export"] button { width: 100%; }
[class*="st-key-card_export"] [data-testid="stDownloadButton"], [class*="st-key-card_export"] [data-testid="stButton"] { width: 100%; }
.st-key-prep_export button { background: linear-gradient(135deg, var(--violet), var(--cyan)); border: 0; }
.st-key-prep_export button p { color: #fff !important; font-weight: 600; }
[data-testid="stNumberInput"] > div > div, [data-testid="stNumberInput"] input {
    background: var(--soft) !important; color: var(--text) !important; border-radius: 12px !important;
    border-color: var(--border) !important; -webkit-text-fill-color: var(--text) !important;
}
[data-testid="stNumberInput"] button { background: transparent !important; color: var(--muted) !important; }
[data-testid="stNumberInput"] button svg { fill: var(--muted) !important; }
.health { display: inline-flex; align-items: center; gap: 8px; }
.health i { width: 10px; height: 10px; border-radius: 50%; display: inline-block; }
.health.ok i { background: var(--green); box-shadow: 0 0 10px var(--green); animation: pulse 1.6s infinite; }
.health.bad i { background: var(--orange); box-shadow: 0 0 10px var(--orange); }
div[data-testid="stAlert"] { border-radius: 14px; }

/* Live chip */
.chip {
    display: inline-flex; align-items: center; gap: 6px; font-size: 11px; font-weight: 500; color: var(--muted);
    background: var(--soft); border: 1px solid var(--border); border-radius: 999px; padding: 3px 10px; margin-left: 10px;
    vertical-align: middle;
}
.live-dot { width: 7px; height: 7px; border-radius: 50%; background: var(--green); box-shadow: 0 0 8px var(--green); animation: pulse 1.6s infinite; }
@keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: .35; } }

/* KPI */
.kpi { display: flex; flex-direction: column; justify-content: center; height: 100%; }
.kpi-label { font-size: 11px; color: var(--muted); letter-spacing: .06em; text-transform: uppercase; }
.kpi-row { display: flex; justify-content: space-between; align-items: flex-end; gap: 12px; }
.kpi-value { font-size: 27px; font-weight: 600; color: var(--text); line-height: 1.25; }
.kpi-sub { font-size: 12px; }
.spark { width: 42%; height: 46px; flex-shrink: 0; }
.rangebar { position: relative; height: 5px; border-radius: 5px; background: var(--soft); margin-top: 8px; }
.rangebar::before { content: ''; position: absolute; inset: 0; border-radius: 5px; background: linear-gradient(90deg, var(--cyan), var(--violet)); opacity: .55; }
.rangebar i { position: absolute; top: -3px; width: 11px; height: 11px; border-radius: 50%; background: var(--text); border: 2px solid var(--bg); transform: translateX(-50%); }
.up { color: var(--green); } .down { color: var(--orange); } .flat { color: var(--muted); }
.pill { display: inline-block; font-size: 11px; background: var(--soft); border: 1px solid var(--border); color: var(--muted); border-radius: 999px; padding: 1px 9px; }

/* Aggregation */
.agg-row { margin-bottom: 11px; }
.agg-head { display: flex; justify-content: space-between; font-size: 13px; }
.agg-name { color: var(--text); } .agg-val { color: var(--muted); }
.agg-bar { height: 6px; border-radius: 6px; background: var(--soft); margin-top: 5px; overflow: hidden; }
.agg-fill { height: 100%; border-radius: 6px; background: linear-gradient(90deg, var(--violet), var(--cyan)); }
.stat-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 9px; margin-bottom: 14px; }
.stat { background: var(--soft); border-radius: 12px; padding: 9px 11px; }
.stat-l { font-size: 10.5px; color: var(--muted); text-transform: uppercase; letter-spacing: .05em; }
.stat-v { font-size: 15px; font-weight: 600; color: var(--text); }

/* Live feed + tables */
.feed { max-height: 245px; overflow-y: auto; }
.chipicon { width: 54px; height: 54px; flex-shrink: 0; opacity: .85; }
.feed-row { display: grid; grid-template-columns: 64px 1fr 1fr 74px 64px; gap: 8px; align-items: center;
    padding: 7px 4px; border-top: 1px solid var(--border); font-size: 12.5px; }
.feed-row:first-child { border-top: none; }
.f-time, .f-prov { color: var(--muted); } .f-gpu { font-weight: 500; } .f-price { text-align: right; font-weight: 500; }
.f-ch { text-align: right; font-size: 11.5px; }
.tbl-wrap { max-height: 280px; overflow: auto; border-radius: 10px; }
table.tbl { width: 100%; border-collapse: collapse; font-size: 12.5px; border: 0; }
.tbl th { position: sticky; top: 0; background: var(--card_solid); color: var(--muted); text-align: left; font-weight: 500;
    text-transform: uppercase; font-size: 10.5px; letter-spacing: .06em; padding: 8px 10px; border: 0; }
.tbl td { padding: 7px 10px; border: 0; border-top: 1px solid var(--border); color: var(--text); }
.tbl td.num, .tbl th.num { text-align: right; }
"""


def inject_css():
    variables = ";".join(f"--{k}:{v}" for k, v in P.items() if k != "plotly")
    css = (
        "<style>\n@import url('https://fonts.googleapis.com/css2?family=Poppins:wght@400;500;600;700;800&display=swap');\n"
        f":root{{{variables}}}\n{CSS_STATIC}\n</style>"
    )
    # One <style> block, no blank lines: markdown would otherwise end the HTML block early
    st.markdown("\n".join(line for line in css.splitlines() if line.strip()), unsafe_allow_html=True)


inject_css()


# --- SLIDESHOW (GPU photos via the MediaWiki API on Wikimedia Commons) ---
def _clean_title(title):
    title = re.sub(r"^File:", "", title)
    title = re.sub(r"\.(jpe?g|png)$", "", title, flags=re.I)
    return re.sub(r"\s+", " ", title.replace("_", " ")).strip()


@st.cache_data(ttl=6 * 3600, show_spinner=False)
def _fetch_gpu_slides():
    headers = {"User-Agent": "RN-GPU-Dashboard/1.0 (student project; Python requests)"}
    slides, seen = [], set()
    for query in SLIDE_QUERIES:
        try:
            r = requests.get(
                COMMONS_API,
                params={
                    "action": "query", "format": "json", "generator": "search",
                    "gsrsearch": f"{query} filetype:bitmap", "gsrnamespace": 6, "gsrlimit": 30,
                    "prop": "imageinfo", "iiprop": "url|size|mime|extmetadata",
                    "iiurlwidth": 1800, "iiextmetadatafilter": "Artist|LicenseShortName",
                },
                headers=headers, timeout=6,
            )
            r.raise_for_status()
            pages = (r.json().get("query") or {}).get("pages") or {}
        except Exception:
            break  # network problem: stop early instead of stalling the dashboard
        taken = 0
        for page in sorted(pages.values(), key=lambda p: p.get("index", 999)):
            title = page.get("title", "")
            info = (page.get("imageinfo") or [{}])[0]
            if title in seen or not GPU_TITLE.search(title.replace("_", " ").lower()):
                continue
            if info.get("mime") not in ("image/jpeg", "image/png"):
                continue
            w, h = info.get("width", 0), info.get("height", 0)
            if w < 1000 or not h or w / h < 1.2:
                continue
            url = info.get("thumburl") or info.get("url")
            if not url:
                continue
            meta = info.get("extmetadata") or {}
            artist = re.sub(r"<[^>]+>", "", (meta.get("Artist") or {}).get("value", "")).strip()
            lic = (meta.get("LicenseShortName") or {}).get("value", "")
            credit = " · ".join(x for x in (artist[:40], lic, "Wikimedia Commons") if x)
            slides.append({"url": url, "title": _clean_title(title)[:90], "credit": credit})
            seen.add(title)
            taken += 1
            if taken >= SLIDES_PER_QUERY:
                break
    if not slides:
        raise RuntimeError("no slides")
    return slides


@st.cache_data(ttl=120, show_spinner=False)
def fetch_gpu_slides():
    try:
        return _fetch_gpu_slides()
    except Exception:
        return []  # retried every 2 minutes


SLIDESHOW_TEMPLATE = """
<style>
  @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@400;500;600&display=swap');
  html, body { margin: 0; background: transparent; font-family: 'Poppins', sans-serif; }
  .wrap { position: relative; height: __HEIGHT__px; border-radius: 18px; overflow: hidden; cursor: grab;
          user-select: none; -webkit-user-select: none; touch-action: pan-y;
          border: 1px solid rgba(128,128,128,0.28); box-shadow: 0 8px 28px rgba(0,0,0,0.25); background: #0B1130; }
  .wrap.drag { cursor: grabbing; }
  .stage { position: absolute; inset: 0; }
  .stage.jump { animation: fadein .5s ease; }
  @keyframes fadein { from { opacity: .25; } to { opacity: 1; } }
  .slide { position: absolute; inset: 0; will-change: transform; background: linear-gradient(120deg,#8B5CF6,#22D3EE); }
  .slide img { width: 100%; height: 100%; object-fit: cover; pointer-events: none; display: block; }
  .slide::after { content: ''; position: absolute; inset: 0; pointer-events: none;
      background: linear-gradient(180deg, rgba(5,8,22,0) 30%, rgba(5,8,22,0.88) 100%),
                  linear-gradient(90deg, rgba(139,92,246,0.28), rgba(34,211,238,0.10)); }
  .cap { position: absolute; left: 24px; bottom: 20px; z-index: 2; color: #E6EAF5; max-width: 33%; }
  .cap b { display: block; font-size: 16px; font-weight: 600; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .cap span { display: block; font-size: 11.5px; color: #A9B6D8; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .dots { position: absolute; left: 50%; transform: translateX(-50%); bottom: 16px; z-index: 4; display: flex; gap: 8px; max-width: 40%; flex-wrap: wrap; justify-content: center; }
  .dot { width: 8px; height: 8px; padding: 0; border: 0; border-radius: 8px; background: rgba(255,255,255,0.42); cursor: pointer; transition: all .3s; }
  .dot:hover { background: rgba(255,255,255,0.8); }
  .dot.on { width: 24px; background: #22D3EE; box-shadow: 0 0 10px #22D3EE; }
  .counter { position: absolute; top: 14px; right: 16px; z-index: 4; font-size: 11px; color: #E6EAF5; font-weight: 500;
             background: rgba(5,8,22,0.55); border: 1px solid rgba(255,255,255,0.18); border-radius: 999px; padding: 3px 11px; }
  .arrow { position: absolute; top: 50%; transform: translateY(-50%); z-index: 4; width: 36px; height: 36px; border-radius: 50%;
           border: 1px solid rgba(255,255,255,0.25); background: rgba(5,8,22,0.5); color: #fff; font-size: 16px; cursor: pointer;
           opacity: 0; transition: opacity .25s; display: flex; align-items: center; justify-content: center; }
  .arrow.l { left: 14px; } .arrow.r { right: 14px; }
  .wrap:hover .arrow { opacity: 1; } .arrow:hover { background: rgba(139,92,246,0.7); }
  .bar { position: absolute; left: 0; bottom: 0; height: 3px; z-index: 5; width: 0; background: linear-gradient(90deg,#8B5CF6,#22D3EE); }
</style>
<div class="wrap" id="wrap">
  <div class="stage" id="stage"></div>
  <div class="counter" id="counter"></div>
  <button class="arrow l" id="prev" aria-label="Previous slide">&#8249;</button>
  <button class="arrow r" id="next" aria-label="Next slide">&#8250;</button>
  <div class="dots" id="dots"></div>
  <div class="bar" id="bar"></div>
</div>
<script>
(function () {
  const slides = __SLIDES__;
  const SECONDS = __SECONDS__;
  const wrap = document.getElementById('wrap'), stage = document.getElementById('stage');
  const dotsEl = document.getElementById('dots'), bar = document.getElementById('bar'), counter = document.getElementById('counter');
  const n = slides.length, half = Math.floor(n / 2);
  let idx = 0, timer = null, dragging = false, startX = 0, dx = 0;
  const offsets = [];

  const els = slides.map((s) => {
    const d = document.createElement('div'); d.className = 'slide';
    const img = new Image(); img.src = s.url; img.alt = s.title; img.draggable = false;
    img.onerror = () => { img.remove(); };
    d.appendChild(img);
    const cap = document.createElement('div'); cap.className = 'cap';
    const b = document.createElement('b'); b.textContent = s.title;
    const sp = document.createElement('span'); sp.textContent = s.credit;
    cap.appendChild(b); cap.appendChild(sp); d.appendChild(cap);
    stage.appendChild(d); return d;
  });
  const dots = slides.map((_, i) => {
    const b = document.createElement('button'); b.className = 'dot'; b.setAttribute('aria-label', 'Go to slide ' + (i + 1));
    b.onclick = () => go(i); dotsEl.appendChild(b); return b;
  });

  const rel = (i) => ((i - idx + n + half) % n) - half;      // position relative to current, wraps around
  function layout(px) {
    els.forEach((el, i) => {
      const r = rel(i), prev = offsets[i];
      const animate = prev !== undefined && Math.abs(prev) <= 2 && Math.abs(r) <= 2 && !dragging;
      el.style.transition = animate ? 'transform .65s cubic-bezier(.22,.8,.26,1)' : 'none';
      el.style.transform = 'translate3d(calc(' + (r * 100) + '% + ' + px + 'px),0,0)';
      el.style.visibility = Math.abs(r) <= 2 ? 'visible' : 'hidden';
      offsets[i] = r;
    });
  }
  function sync() {
    dots.forEach((d, i) => d.classList.toggle('on', i === idx));
    counter.textContent = String(idx + 1).padStart(2, '0') + ' / ' + String(n).padStart(2, '0');
  }
  function restart() {
    clearTimeout(timer);
    bar.style.transition = 'none'; bar.style.width = '0'; void bar.offsetWidth;
    bar.style.transition = 'width ' + SECONDS + 's linear'; bar.style.width = '100%';
    timer = setTimeout(() => go(idx + 1), SECONDS * 1000);
  }
  function go(target) {
    const t = ((target % n) + n) % n;
    const dist = Math.min(Math.abs(t - idx), n - Math.abs(t - idx));
    idx = t; layout(0); sync(); restart();
    if (dist > 1) { stage.classList.remove('jump'); void stage.offsetWidth; stage.classList.add('jump'); }
  }

  wrap.addEventListener('pointerdown', (e) => {
    if (e.target.closest('.dot, .arrow')) return;
    dragging = true; startX = e.clientX; dx = 0;
    wrap.setPointerCapture(e.pointerId); wrap.classList.add('drag');
    clearTimeout(timer); bar.style.transition = 'none'; bar.style.width = '0';
  });
  wrap.addEventListener('pointermove', (e) => { if (!dragging) return; dx = e.clientX - startX; layout(dx); });
  function endDrag() {
    if (!dragging) return;
    dragging = false; wrap.classList.remove('drag');
    const th = Math.min(120, wrap.clientWidth * 0.12), moved = dx; dx = 0;
    go(moved < -th ? idx + 1 : moved > th ? idx - 1 : idx);
  }
  wrap.addEventListener('pointerup', endDrag);
  wrap.addEventListener('pointercancel', endDrag);
  document.getElementById('prev').onclick = () => go(idx - 1);
  document.getElementById('next').onclick = () => go(idx + 1);
  window.addEventListener('keydown', (e) => { if (e.key === 'ArrowLeft') go(idx - 1); if (e.key === 'ArrowRight') go(idx + 1); });

  layout(0); sync(); restart();
})();
</script>
"""

SLIDESHOW_FALLBACK = """
<style>html,body{margin:0;font-family:'Poppins',sans-serif}
.f{height:__HEIGHT__px;border-radius:18px;background:linear-gradient(120deg,#8B5CF6,#22D3EE);display:flex;flex-direction:column;
justify-content:flex-end;padding:0 24px 22px;box-sizing:border-box;color:#fff}
.f b{font-size:20px}.f span{font-size:12px;opacity:.8}</style>
<div class="f"><b>Cloud GPU Market</b><span>GPU photos are unavailable right now — retrying automatically</span></div>
"""


def slideshow_html(slides):
    if not slides:
        return SLIDESHOW_FALLBACK.replace("__HEIGHT__", str(SLIDE_HEIGHT))
    slides = list(slides)
    while len(slides) < 3:          # the looping carousel needs at least 3 panels
        slides += slides
    data = json.dumps(slides).replace("</", "<\\/")
    return (SLIDESHOW_TEMPLATE.replace("__SLIDES__", data)
            .replace("__SECONDS__", str(SLIDE_SECONDS))
            .replace("__HEIGHT__", str(SLIDE_HEIGHT)))


# --- DATA FETCHING ---
def consume_kafka_data():
    """Latest LIVE_MESSAGES messages of the topic -> (DataFrame, error message or None)."""
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=2000
        )
        partitions = consumer.partitions_for_topic(KAFKA_TOPIC)
        if not partitions:
            consumer.close()
            return pd.DataFrame(), f"Topic '{KAFKA_TOPIC}' does not exist yet - waiting for the producer."
        tps = [TopicPartition(KAFKA_TOPIC, p) for p in partitions]
        consumer.assign(tps)
        ends = consumer.end_offsets(tps)
        per_partition = max(1, LIVE_MESSAGES // len(tps))
        for tp in tps:
            consumer.seek(tp, max(0, ends[tp] - per_partition))

        messages = [msg.value for msg in consumer]
        consumer.close()

        if messages:
            df = pd.DataFrame(messages)
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
            return df, None
        return pd.DataFrame(), None
    except Exception as exc:
        return pd.DataFrame(), f"{type(exc).__name__}: {exc}"


def _read_collection(name, time_cols, sort_col):
    """Whole MongoDB collection -> (DataFrame, error message or None)."""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        data = list(client[DB_NAME][name].find({}, {"_id": 0}))
        client.close()
    except Exception as exc:
        return pd.DataFrame(), f"{type(exc).__name__}: {exc}"
    if not data:
        return pd.DataFrame(), None
    df = pd.DataFrame(data)
    for col in time_cols:
        df[col] = pd.to_datetime(df[col], utc=True)
    return df.sort_values(sort_col).reset_index(drop=True), None


@st.cache_data(ttl=30, show_spinner=False)
def query_mongodb():
    """Every record Spark has stored (gpu_history)."""
    return _read_collection(COLLECTION_NAME, ["timestamp"], "timestamp")


@st.cache_data(ttl=30, show_spinner=False)
def query_aggregates():
    """Windowed aggregates computed by Spark (gpu_aggregates)."""
    return _read_collection(AGG_COLLECTION, ["window_start", "window_end"], "window_start")


# --- HTML HELPERS ---
def card(name, height=None):
    extra = {"height": height} if height else {}   # height=None is rejected by newer Streamlit
    return st.container(key=f"card_{name}", border=False, **extra)


def esc(x):
    return html_lib.escape(str(x))


def sparkline(values, color, uid, w=120, h=40):
    values = [float(v) for v in values]
    if len(values) < 2:
        return ""
    lo, hi = min(values), max(values)
    span = (hi - lo) or 1
    pts = " ".join(
        f"{i / (len(values) - 1) * w:.1f},{h - 3 - (v - lo) / span * (h - 6):.1f}" for i, v in enumerate(values)
    )
    return (
        f'<svg class="spark" viewBox="0 0 {w} {h}" preserveAspectRatio="none">'
        f'<defs><linearGradient id="sg{uid}" x1="0" y1="0" x2="0" y2="1">'
        f'<stop offset="0" stop-color="{color}" stop-opacity="0.35"/><stop offset="1" stop-color="{color}" stop-opacity="0"/>'
        f'</linearGradient></defs><polygon points="0,{h} {pts} {w},{h}" fill="url(#sg{uid})"/>'
        f'<polyline points="{pts}" fill="none" stroke="{color}" stroke-width="1.6" stroke-linejoin="round" '
        f'stroke-linecap="round" vector-effect="non-scaling-stroke"/></svg>'
    )


def chip_icon(color):
    return (
        f'<svg class="chipicon" viewBox="0 0 48 48" fill="none" stroke="{color}" stroke-width="2" stroke-linecap="round">'
        f'<rect x="12" y="12" width="24" height="24" rx="5"/><rect x="19" y="19" width="10" height="10" rx="2"/>'
        + "".join(f'<path d="M{p} 12V6M{p} 42V36M12 {p}H6M42 {p}H36"/>' for p in (18, 24, 30))
        + '</svg>'
    )


def kpi(label, value, sub="", sub_class="flat", spark="", foot=""):
    return (
        f'<div class="kpi"><div class="kpi-label">{label}</div><div class="kpi-row"><div>'
        f'<div class="kpi-value">{value}</div><div class="kpi-sub {sub_class}">{sub}</div></div>{spark}</div>{foot}</div>'
    )


def delta_html(current, reference, ref_label):
    if not reference:
        return "", "flat"
    pct = (current - reference) / reference * 100
    if abs(pct) < 0.005:
        return f"● 0.00% vs {ref_label}", "flat"
    arrow, cls = ("▲", "up") if pct > 0 else ("▼", "down")
    return f"{arrow} {abs(pct):.2f}% vs {ref_label}", cls


def bar_rows(series, fmt="${:.4f}/hr"):
    if series.empty:
        return '<div class="agg-val">No data</div>'
    top = series.max() or 1
    return "".join(
        f'<div class="agg-row"><div class="agg-head"><span class="agg-name">{esc(name)}</span>'
        f'<span class="agg-val">{fmt.format(val)}</span></div>'
        f'<div class="agg-bar"><div class="agg-fill" style="width:{max(4, val / top * 100):.0f}%"></div></div></div>'
        for name, val in series.items()
    )


def stat_grid(df):
    items = [
        ("Min", f"${df['value'].min():.4f}"), ("Max", f"${df['value'].max():.4f}"),
        ("Mean", f"${df['value'].mean():.4f}"), ("Median", f"${df['value'].median():.4f}"),
        ("Std dev", f"{df['value'].std(ddof=0):.4f}"), ("Samples", f"{len(df):,}"),
    ]
    return '<div class="stat-grid">' + "".join(
        f'<div class="stat"><div class="stat-l">{l}</div><div class="stat-v">{v}</div></div>' for l, v in items
    ) + '</div>'


def provider_series(df, limit=6):
    if 'provider' not in df.columns:
        return pd.Series(dtype=float)
    prov = df.groupby('provider')['value'].mean().sort_values(ascending=False).head(limit)
    prov.index = [str(p).capitalize() for p in prov.index]
    return prov


def aggregation_panel(df, height):
    with card("agg", height):
        st.markdown('<div class="card-title">Data Aggregation</div>', unsafe_allow_html=True)
        st.markdown(stat_grid(df), unsafe_allow_html=True)
        st.markdown('<div class="card-title">Provider Averages</div>', unsafe_allow_html=True)
        st.markdown(bar_rows(provider_series(df)), unsafe_allow_html=True)


def top_gpus_card(df, height, name="top", limit=6):
    with card(name, height):
        st.markdown('<div class="card-title">Top GPUs by Avg Price</div>', unsafe_allow_html=True)
        gpus = df.groupby('sensor_id')['value'].mean().sort_values(ascending=False).head(limit)
        st.markdown(bar_rows(gpus), unsafe_allow_html=True)


def feed_html(df):
    d = df.sort_values('timestamp').copy()
    d['prev'] = d.groupby('sensor_id')['value'].shift()
    rows = []
    for _, r in d.tail(15).iloc[::-1].iterrows():
        if pd.notna(r['prev']) and r['prev']:
            ch = (r['value'] - r['prev']) / r['prev'] * 100
            cls, txt = ("up", f"▲ {ch:.1f}%") if ch > 0 else (("down", f"▼ {abs(ch):.1f}%") if ch < 0 else ("flat", "● 0.0%"))
        else:
            cls, txt = "flat", "new"
        rows.append(
            f'<div class="feed-row"><span class="f-time">{r["timestamp"].strftime("%H:%M:%S")}</span>'
            f'<span class="f-gpu">{esc(r["sensor_id"])}</span>'
            f'<span class="f-prov">{esc(str(r.get("provider", "")).capitalize())}</span>'
            f'<span class="f-price">${r["value"]:.4f}</span><span class="f-ch {cls}">{txt}</span></div>'
        )
    return '<div class="feed">' + "".join(rows) + '</div>'


def raw_table_html(df, page=1, page_size=PAGE_SIZE):
    """One page of the full history, newest first."""
    start = (page - 1) * page_size
    d = df.sort_values('timestamp', ascending=False).iloc[start:start + page_size]
    head = '<tr><th class="num">#</th><th>Time (UTC)</th><th>GPU</th><th>Provider</th><th class="num">Price (USD/hr)</th></tr>'
    body = "".join(
        f'<tr><td class="num f-prov">{start + n}</td><td>{r["timestamp"].strftime("%Y-%m-%d %H:%M:%S")}</td><td>{esc(r["sensor_id"])}</td>'
        f'<td>{esc(str(r.get("provider", "")).capitalize())}</td><td class="num">${r["value"]:.4f}</td></tr>'
        for n, (_, r) in enumerate(d.iterrows(), 1)
    )
    return f'<div class="tbl-wrap"><table class="tbl">{head}{body}</table></div>'


def human_span(delta):
    secs = int(delta.total_seconds())
    d, rem = divmod(secs, 86400)
    h, rem = divmod(rem, 3600)
    m = rem // 60
    return f"{d}d {h}h" if d else (f"{h}h {m}m" if h else f"{m}m")


def export_panel(filtered, agg_filtered, label, height):
    """Builds the Excel / PDF / JPG / CSV files on demand (so the 60s refresh does not rebuild them)."""
    with card("export", height):
        st.markdown('<div class="card-title">Export Report</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="card-sub" style="margin:0 0 10px 0;display:block">{len(filtered):,} rows · current filters</div>',
                    unsafe_allow_html=True)
        if st.button("Prepare files", key="prep_export"):
            with st.spinner("Building Excel, PDF and chart..."):
                st.session_state["exports"] = {
                    "xlsx": build_xlsx(filtered, agg_filtered, label),
                    "pdf": build_pdf(filtered, agg_filtered, label),
                    "jpg": chart_image(filtered, "jpg", label),
                    "csv": filtered.to_csv(index=False).encode("utf-8"),
                    "rows": len(filtered),
                    "at": datetime.now(timezone.utc).strftime("%H:%M:%S"),
                    "stamp": datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"),
                }
        ex = st.session_state.get("exports")
        if ex:
            st.download_button("Excel workbook (.xlsx)", ex["xlsx"], f"gpu_report_{ex['stamp']}.xlsx",
                               "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="dl_xlsx")
            st.download_button("PDF report (.pdf)", ex["pdf"], f"gpu_report_{ex['stamp']}.pdf", "application/pdf", key="dl_pdf")
            st.download_button("Price chart (.jpg)", ex["jpg"], f"gpu_chart_{ex['stamp']}.jpg", "image/jpeg", key="dl_jpg")
            st.download_button("Raw data (.csv)", ex["csv"], f"gpu_history_{ex['stamp']}.csv", "text/csv", key="dl_csv")
            st.caption(f"Prepared {ex['at']} UTC · {ex['rows']:,} rows")
        else:
            st.caption("Press the button to build the files for the filters you selected.")


# --- CHARTS ---
def show_plot(fig):
    # theme=None: keep our own light/dark colors instead of Streamlit's built-in chart theme
    try:
        st.plotly_chart(fig, width="stretch", config=PLOT_CONFIG, theme=None)
    except TypeError:  # older Streamlit
        st.plotly_chart(fig, use_container_width=True, config=PLOT_CONFIG, theme=None)


def style_trading_fig(fig, y_values, height, pct=False):
    lo, hi = float(min(y_values)), float(max(y_values))
    pad = (hi - lo) * 0.15 or max(abs(hi) * 0.05, 0.01)
    # Hairline crosshair: 0.5px, translucent, follows the cursor
    spike = dict(showspikes=True, spikemode='across', spikethickness=0.5, spikecolor=P["spike"], spikedash='solid')
    fig.update_layout(
        template=P["plotly"], height=height, margin=dict(l=6, r=6, t=6, b=6),
        plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)',
        font=dict(family="Poppins, sans-serif", color=P["muted"], size=12),
        hovermode='x unified',
        hoverlabel=dict(bgcolor=P["tip_bg"], font_color=P["text"], bordercolor=P["violet"]),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, x=0, font=dict(color=P["text"])),
        xaxis=dict(showgrid=False, zeroline=False, hoverformat='%b %d, %H:%M:%S', **spike),
        yaxis=dict(side='right', range=[(lo - pad) if pct else max(0, lo - pad), hi + pad], showgrid=True, gridcolor=P["grid"],
                   tickprefix='' if pct else '$', ticksuffix='%' if pct else '', zeroline=pct, zerolinecolor=P["spike"],
                   spikesnap='cursor', **spike),
    )
    return fig


def market_tape_figure(df, gpu_filter, height):
    all_gpus = gpu_filter.startswith("All")
    if all_gpus:
        # every GPU as % change since the start of the live window: comparable even though prices range from $0.50 to $7+
        fig = go.Figure()
        pct_all = []
        for i, (gpu, g) in enumerate(df.sort_values('timestamp').groupby('sensor_id')):
            base = float(g['value'].iloc[0]) or 1.0
            pct = (g['value'] / base - 1) * 100
            pct_all.append(pct)
            fig.add_trace(go.Scatter(
                x=g['timestamp'], y=pct, mode='lines', name=str(gpu), line=dict(color=SERIES[i % len(SERIES)], width=1.8),
                customdata=g['value'], hovertemplate="%{y:+.2f}% · $%{customdata:.4f}/hr<extra>" + esc(gpu) + "</extra>"))
        style_trading_fig(fig, pd.concat(pct_all), height, pct=True)
        return fig
    color = P["violet"]
    frame = df[df['sensor_id'] == gpu_filter].sort_values('timestamp')
    fig = go.Figure(go.Scatter(
        x=frame['timestamp'], y=frame['value'], mode='lines+markers',
        line=dict(color=color, width=2.2), marker=dict(size=4, color=color),
        fill='tozeroy', fillcolor='rgba(139,92,246,0.12)',
        customdata=frame[['sensor_id']].values,
        hovertemplate="<b>%{customdata[0]}</b><br>$%{y:.4f}/hr<extra></extra>", name="Price",
    ))
    style_trading_fig(fig, frame['value'], height)
    last = float(frame['value'].iloc[-1])
    fig.add_hline(y=last, line_dash='dot', line_color=color, line_width=1, annotation_text=f"${last:.4f}",
                  annotation_position='right', annotation_font=dict(color=P["tag_text"], size=11),
                  annotation_bgcolor=color)
    fig.update_layout(showlegend=False)
    return fig


def historical_figure(df, height):
    fig = go.Figure()
    for i, (gpu, g) in enumerate(df.groupby('sensor_id')):
        g = g.sort_values('timestamp')
        fig.add_trace(go.Scatter(
            x=g['timestamp'], y=g['value'], mode='lines', name=str(gpu),
            line=dict(color=SERIES[i % len(SERIES)], width=2),
            hovertemplate="$%{y:.4f}/hr<extra>" + esc(gpu) + "</extra>",
        ))
    style_trading_fig(fig, df['value'], height)
    fig.update_xaxes(rangeslider=dict(visible=True, thickness=0.06, bgcolor=P["soft"]))
    return fig


def spark_agg_figure(agg, gpu, height):
    """Average line with the min-max band for one GPU, from the windows Spark computed."""
    g = agg[agg['sensor_id'] == gpu].sort_values('window_start')
    x = g['window_start']
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=x, y=g['max_price'], mode='lines', line=dict(width=0), hoverinfo='skip', showlegend=False))
    fig.add_trace(go.Scatter(x=x, y=g['min_price'], mode='lines', line=dict(width=0), fill='tonexty',
                             fillcolor='rgba(139,92,246,0.18)', hoverinfo='skip', showlegend=False))
    fig.add_trace(go.Scatter(
        x=x, y=g['avg_price'], mode='lines+markers', line=dict(color=P["violet"], width=2.2), marker=dict(size=4, color=P["violet"]),
        customdata=g[['min_price', 'max_price', 'samples']].values, name="avg",
        hovertemplate="avg $%{y:.4f}/hr<br>min $%{customdata[0]:.4f} · max $%{customdata[1]:.4f}<br>%{customdata[2]} samples<extra></extra>"))
    style_trading_fig(fig, pd.concat([g['min_price'], g['max_price']]), height)
    fig.update_layout(showlegend=False)
    return fig


def latest_by_gpu_figure(df, height):
    latest = df.sort_values('timestamp').groupby('sensor_id').tail(1).sort_values('value')
    fig = go.Figure(go.Bar(
        x=latest['value'], y=latest['sensor_id'], orientation='h',
        marker=dict(color=latest['value'], colorscale=[[0, P["cyan"]], [1, P["violet"]]], line=dict(width=0)),
        text=[f"${v:.2f}" for v in latest['value']], textposition='outside', cliponaxis=False,
        textfont=dict(color=P["text"], size=11), hovertemplate="<b>%{y}</b><br>$%{x:.4f}/hr<extra></extra>",
    ))
    fig.update_layout(
        template=P["plotly"], height=height, margin=dict(l=6, r=48, t=4, b=4), bargap=0.35,
        plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)',
        font=dict(family="Poppins, sans-serif", color=P["muted"], size=11),
        xaxis=dict(showgrid=False, zeroline=False, visible=False, range=[0, float(latest['value'].max()) * 1.2]),
        yaxis=dict(showgrid=False, zeroline=False, tickfont=dict(color=P["text"])),
    )
    return fig


# --- VIEWS ---
MAIN_H = 500
BOTTOM_H = 340


def render_realtime():
    st_autorefresh(interval=REALTIME_REFRESH_MS, key="rt_refresh")
    df, err = consume_kafka_data()

    if df.empty:
        if err:
            st.error(f"Cannot read Kafka at {KAFKA_BOOTSTRAP}: {err}")
        else:
            st.info("Waiting for live data from Kafka...")
        return

    df = df.sort_values('timestamp')
    latest_ts = df['timestamp'].max()
    polls = df.groupby('timestamp')['value'].mean()            # the producer sends one message per GPU per poll
    latest_by = df.groupby('sensor_id').tail(1)
    cheap, pricey = latest_by.loc[latest_by['value'].idxmin()], latest_by.loc[latest_by['value'].idxmax()]
    delta_txt, delta_cls = delta_html(float(polls.iloc[-1]), float(polls.iloc[-2]) if len(polls) > 1 else None, "previous poll")
    n_prov = df['provider'].nunique() if 'provider' in df.columns else 0

    def gpu_card(label, row, cls, color, uid):
        series = df[df['sensor_id'] == row['sensor_id']]['value']
        return kpi(label, esc(row['sensor_id']),
                   f'<span class="{cls}">${row["value"]:.4f}/hr</span> <span class="pill">{esc(str(row.get("provider", "")).capitalize())}</span>',
                   "flat", sparkline(series, color, uid))

    kpis = [
        kpi("Last Update", latest_ts.strftime("%H:%M:%S"), f"UTC · {df['sensor_id'].nunique()} GPUs · {n_prov} providers",
            "flat", chip_icon(P["cyan"])),
        kpi("Market Avg Price", f"${polls.iloc[-1]:.4f}", delta_txt, delta_cls, sparkline(polls.values, P["cyan"], "a")),
        gpu_card("Cheapest GPU", cheap, "up", P["green"], "b"),
        gpu_card("Priciest GPU", pricey, "down", P["orange"], "c"),
    ]
    for i, (col, body) in enumerate(zip(st.columns(4), kpis)):
        with col:
            with card(f"kpi{i}", 128):
                st.markdown(body, unsafe_allow_html=True)

    col_chart, col_side = st.columns([3, 1])
    with col_chart:
        with card("chart", MAIN_H):
            head_l, head_r = st.columns([3, 1.2], vertical_alignment="center")
            head_l.markdown(
                f'<div class="card-title">Live Price Trend<span class="chip"><span class="live-dot"></span>'
                f'LIVE · {latest_ts.strftime("%H:%M:%S")} UTC · {REALTIME_REFRESH_MS // 1000}s refresh</span></div>',
                unsafe_allow_html=True)
            options = ["All GPUs (% change)"] + sorted(df['sensor_id'].unique().tolist())
            choice = head_r.selectbox("GPU", options, label_visibility="collapsed", key="gpu_choice")
            show_plot(market_tape_figure(df, choice, MAIN_H - 88))
    with col_side:
        aggregation_panel(df, MAIN_H)

    b1, b2, b3 = st.columns(3)
    with b1:
        top_gpus_card(df, BOTTOM_H)
    with b2:
        with card("latest", BOTTOM_H):
            st.markdown('<div class="card-title">Latest Price per GPU</div>', unsafe_allow_html=True)
            show_plot(latest_by_gpu_figure(df, BOTTOM_H - 90))
    with b3:
        with card("feed", BOTTOM_H):
            st.markdown('<div class="card-title">Live Feed<span class="card-sub">change vs previous tick of the same GPU</span></div>',
                        unsafe_allow_html=True)
            st.markdown(feed_html(df), unsafe_allow_html=True)


def render_historical():
    st_autorefresh(interval=HISTORICAL_REFRESH_MS, key="hist_refresh")
    hist_df, hist_err = query_mongodb()
    agg_df, _ = query_aggregates()

    # Say what is wrong instead of just "no data"
    if hist_err:
        st.error(f"Cannot read MongoDB at {MONGO_URI}: {hist_err}")
        return
    if hist_df.empty:
        st.warning(f"MongoDB is reachable, but the '{COLLECTION_NAME}' collection is still empty. The Spark job fills it from "
                   "Kafka (first start takes about a minute). If it stays empty, check `docker compose logs spark`.")
        return

    # --- overview of everything collected so far
    now = pd.Timestamp.now(tz='UTC')
    first_ts, last_ts = hist_df['timestamp'].min(), hist_df['timestamp'].max()
    age = (now - last_ts).total_seconds()
    healthy = age < HEALTHY_SECONDS
    per_hour = hist_df.set_index('timestamp')['value'].resample('1h').count()
    n_prov = hist_df['provider'].nunique() if 'provider' in hist_df.columns else 0
    overview = [
        kpi("Records Stored", f"{len(hist_df):,}", f"in MongoDB · {COLLECTION_NAME}", "flat", sparkline(per_hour.values, P["cyan"], "h1")),
        kpi("Time Span", human_span(last_ts - first_ts), f"{first_ts:%b %d %H:%M} → {last_ts:%b %d %H:%M} UTC", "flat"),
        kpi("Coverage", f"{hist_df['sensor_id'].nunique()} GPUs", f"{n_prov} providers · {len(agg_df):,} Spark windows", "flat"),
        kpi("Pipeline", f'<span class="health {"ok" if healthy else "bad"}"><i></i>{"Healthy" if healthy else "Stale"}</span>',
            f"last write {int(age)}s ago" if age < 3600 else f"last write {human_span(now - last_ts)} ago",
            "up" if healthy else "down"),
    ]
    for i, (col, body) in enumerate(zip(st.columns(4), overview)):
        with col:
            with card(f"hkpi{i}", 128):
                st.markdown(body, unsafe_allow_html=True)

    # --- filters
    with card("filters"):
        f1, f2, f3 = st.columns([2, 2, 1.3], vertical_alignment="bottom")
        gpu_options = sorted(hist_df['sensor_id'].unique().tolist())
        selected_gpus = f1.multiselect("GPUs", gpu_options, default=gpu_options[:5])
        if 'provider' in hist_df.columns:
            prov_options = sorted(hist_df['provider'].dropna().unique().tolist())
            selected_prov = f2.multiselect("Providers", prov_options, default=prov_options)
        else:
            prov_options, selected_prov = [], None
        span = f3.radio("Range", ["1H", "6H", "24H", "7D", "ALL"], index=4, horizontal=True, key="range_radio")

    cutoff = None
    if span != "ALL":
        cutoff = now - timedelta(hours={"1H": 1, "6H": 6, "24H": 24, "7D": 24 * 7}[span])
    filtered = hist_df[hist_df['sensor_id'].isin(selected_gpus)]
    if selected_prov is not None:
        filtered = filtered[filtered['provider'].isin(selected_prov)]
    agg_f = agg_df[agg_df['sensor_id'].isin(selected_gpus)] if not agg_df.empty else agg_df
    if cutoff is not None:
        filtered = filtered[filtered['timestamp'] >= cutoff]
        agg_f = agg_f[agg_f['window_start'] >= cutoff] if not agg_f.empty else agg_f

    if filtered.empty:
        st.warning("No data for the selected filters.")
        return
    label = (f"GPUs: {', '.join(selected_gpus) if len(selected_gpus) <= 6 else str(len(selected_gpus)) + ' selected'} | "
             f"Providers: {'all' if selected_prov is None or len(selected_prov) == len(prov_options) else ', '.join(selected_prov)} | Range: {span}")

    # --- main chart + aggregation
    col_chart, col_side = st.columns([3, 1])
    with col_chart:
        with card("hchart", MAIN_H + 20):
            st.markdown('<div class="card-title">Historical Price Trends<span class="card-sub">UTC · drag the slider to zoom</span></div>',
                        unsafe_allow_html=True)
            show_plot(historical_figure(filtered, MAIN_H - 60))
    with col_side:
        aggregation_panel(filtered, MAIN_H + 20)

    # --- Spark aggregates + export
    s1, s2 = st.columns([3, 1])
    with s1:
        with card("spark", BOTTOM_H):
            head_l, head_r = st.columns([3, 1.2], vertical_alignment="center")
            head_l.markdown(f'<div class="card-title">Spark Aggregates<span class="chip">Structured Streaming · {esc(AGG_WINDOW)} windows</span></div>',
                            unsafe_allow_html=True)
            if agg_f.empty:
                st.info("Spark has not produced aggregate windows for this selection yet.")
            else:
                choice = head_r.selectbox("GPU", sorted(agg_f['sensor_id'].unique().tolist()), label_visibility="collapsed", key="agg_gpu")
                show_plot(spark_agg_figure(agg_f, choice, BOTTOM_H - 90))
    with s2:
        export_panel(filtered, agg_f, label, BOTTOM_H)

    # --- every collected record (paginated) + top GPUs
    b1, b2 = st.columns([2, 1])
    with b1:
        with card("raw", BOTTOM_H + 30):
            pages = max(1, math.ceil(len(filtered) / PAGE_SIZE))
            if st.session_state.get("raw_page", 1) > pages:
                st.session_state["raw_page"] = pages
            head_l, head_r = st.columns([3, 1], vertical_alignment="center")
            with head_r:
                page = st.number_input("Page", min_value=1, max_value=pages, step=1, key="raw_page", label_visibility="collapsed")
            head_l.markdown(f'<div class="card-title">All Collected Data<span class="card-sub">{len(filtered):,} rows · page {page} of {pages:,}</span></div>',
                            unsafe_allow_html=True)
            st.markdown(raw_table_html(filtered, page), unsafe_allow_html=True)
    with b2:
        top_gpus_card(filtered, BOTTOM_H + 30, "htop", limit=7)


# --- MAIN ---
def main():
    # Top bar: RN brand (left) · tabs (center) · light/dark switch (far right)
    c1, c2, c3 = st.columns([1, 2, 1], vertical_alignment="center")
    with c1:
        st.markdown('<div class="rn-mark">RN<i></i></div>', unsafe_allow_html=True)
    with c2:
        view = st.radio("View", VIEW_LABELS, horizontal=True, label_visibility="collapsed", key="view_radio")
    with c3:
        st.radio("Theme", list(THEME_LABELS.values()), horizontal=True, label_visibility="collapsed",
                 index=1, key="theme_radio")

    # Same HTML on every rerun, so the browser keeps the slideshow's 12s rhythm
    html = slideshow_html(fetch_gpu_slides())
    if hasattr(st, "iframe"):        # newer Streamlit (components.html is deprecated)
        st.iframe(html, height=SLIDE_HEIGHT + 4)
    else:
        components.html(html, height=SLIDE_HEIGHT + 4)

    if view == "Real-time":
        render_realtime()
    else:
        render_historical()


if __name__ == "__main__":
    main()
