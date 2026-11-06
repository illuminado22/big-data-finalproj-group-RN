"""GPU photo slideshow (Wikimedia Commons via the MediaWiki API)."""

import json
import re

import requests
import streamlit as st

from dashboard.config import COMMONS_API, GPU_TITLE, SLIDE_HEIGHT, SLIDE_QUERIES, SLIDE_SECONDS, SLIDES_PER_QUERY


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
                    "action": "query",
                    "format": "json",
                    "generator": "search",
                    "gsrsearch": f"{query} filetype:bitmap",
                    "gsrnamespace": 6,
                    "gsrlimit": 30,
                    "prop": "imageinfo",
                    "iiprop": "url|size|mime|extmetadata",
                    "iiurlwidth": 1800,
                    "iiextmetadatafilter": "Artist|LicenseShortName",
                },
                headers=headers,
                timeout=6,
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
    while len(slides) < 3:  # the looping carousel needs at least 3 panels
        slides += slides
    data = json.dumps(slides).replace("</", "<\\/")
    return (
        SLIDESHOW_TEMPLATE.replace("__SLIDES__", data)
        .replace("__SECONDS__", str(SLIDE_SECONDS))
        .replace("__HEIGHT__", str(SLIDE_HEIGHT))
    )
