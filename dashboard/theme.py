"""Colour palettes, CSS and the light/dark theme switch."""

import streamlit as st

# --- THEMES (deep-space navy + violet/cyan, matching the GPU / cloud-compute feel) ---
PALETTES = {
    "Dark": dict(
        bg="#050816",
        text="#E6EAF5",
        muted="#8A94B3",
        card="rgba(255,255,255,0.045)",
        card_solid="#0E1330",
        border="rgba(255,255,255,0.09)",
        soft="rgba(255,255,255,0.055)",
        grid="rgba(255,255,255,0.06)",
        spike="rgba(190,200,230,0.45)",
        violet="#8B5CF6",
        cyan="#22D3EE",
        green="#34D399",
        orange="#FC5A3C",
        pink="#F472B6",
        amber="#FBBF24",
        blue="#60A5FA",
        lime="#A3E635",
        glow1="rgba(139,92,246,0.28)",
        glow2="rgba(34,211,238,0.16)",
        glow3="rgba(252,61,33,0.08)",
        shadow="0 8px 32px rgba(0,0,0,0.28)",
        plotly="plotly_dark",
        tip_bg="#0B1130",
        tag_text="#050816",
    ),
    "Light": dict(
        bg="#F2F4FB",
        text="#141A33",
        muted="#5B6684",
        card="rgba(255,255,255,0.82)",
        card_solid="#FFFFFF",
        border="rgba(20,26,51,0.09)",
        soft="rgba(20,26,51,0.05)",
        grid="rgba(20,26,51,0.07)",
        spike="rgba(20,26,51,0.40)",
        violet="#7C3AED",
        cyan="#0891B2",
        green="#059669",
        orange="#E5341A",
        pink="#DB2777",
        amber="#D97706",
        blue="#2563EB",
        lime="#65A30D",
        glow1="rgba(124,58,237,0.16)",
        glow2="rgba(8,145,178,0.12)",
        glow3="rgba(229,52,26,0.05)",
        shadow="0 8px 28px rgba(30,41,90,0.10)",
        plotly="plotly_white",
        tip_bg="#FFFFFF",
        tag_text="#FFFFFF",
    ),
}
THEME_LABELS = {"Light": "☀️ Light", "Dark": "🌙 Dark"}
VIEW_LABELS = ["Real-time", "Historical"]
PLOT_CONFIG = {"displayModeBar": False}


def current_theme():
    label = st.session_state.get("theme_radio", THEME_LABELS["Dark"])
    return "Light" if label == THEME_LABELS["Light"] else "Dark"


class _ActivePalette:
    """Dict-like view of the palette for the current theme.

    Streamlit caches imported modules between reruns, so the palette must be looked up on every
    access instead of once at import time - otherwise the light/dark switch would not apply.
    """

    def __getitem__(self, key):
        return PALETTES[current_theme()][key]

    def items(self):
        return PALETTES[current_theme()].items()


P = _ActivePalette()


def series():
    """Line colours for multi-GPU charts, in the current theme."""
    return [P[k] for k in ("cyan", "violet", "pink", "amber", "green", "blue", "orange", "lime")]


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
