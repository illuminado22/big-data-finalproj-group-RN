"""Reusable HTML building blocks (cards, KPIs, tables, panels)."""

import html as html_lib
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

from dashboard.config import PAGE_SIZE
from dashboard.exports import build_pdf, build_xlsx, chart_image


def card(name, height=None):
    extra = {"height": height} if height else {}  # height=None is rejected by newer Streamlit
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
        ("Min", f"${df['value'].min():.4f}"),
        ("Max", f"${df['value'].max():.4f}"),
        ("Mean", f"${df['value'].mean():.4f}"),
        ("Median", f"${df['value'].median():.4f}"),
        ("Std dev", f"{df['value'].std(ddof=0):.4f}"),
        ("Samples", f"{len(df):,}"),
    ]
    return (
        '<div class="stat-grid">'
        + "".join(
            f'<div class="stat"><div class="stat-l">{l}</div><div class="stat-v">{v}</div></div>' for l, v in items
        )
        + '</div>'
    )


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
            cls, txt = (
                ("up", f"▲ {ch:.1f}%") if ch > 0 else (("down", f"▼ {abs(ch):.1f}%") if ch < 0 else ("flat", "● 0.0%"))
            )
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
    d = df.sort_values('timestamp', ascending=False).iloc[start : start + page_size]
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
        st.markdown(
            f'<div class="card-sub" style="margin:0 0 10px 0;display:block">{len(filtered):,} rows · current filters</div>',
            unsafe_allow_html=True,
        )
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
            st.download_button(
                "Excel workbook (.xlsx)",
                ex["xlsx"],
                f"gpu_report_{ex['stamp']}.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="dl_xlsx",
            )
            st.download_button(
                "PDF report (.pdf)", ex["pdf"], f"gpu_report_{ex['stamp']}.pdf", "application/pdf", key="dl_pdf"
            )
            st.download_button(
                "Price chart (.jpg)", ex["jpg"], f"gpu_chart_{ex['stamp']}.jpg", "image/jpeg", key="dl_jpg"
            )
            st.download_button("Raw data (.csv)", ex["csv"], f"gpu_history_{ex['stamp']}.csv", "text/csv", key="dl_csv")
            st.caption(f"Prepared {ex['at']} UTC · {ex['rows']:,} rows")
        else:
            st.caption("Press the button to build the files for the filters you selected.")
