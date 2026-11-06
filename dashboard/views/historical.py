"""Historical tab: everything Spark stored in MongoDB."""

import math
from datetime import timedelta

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from dashboard.charts import historical_figure, show_plot, spark_agg_figure
from dashboard.components import (
    aggregation_panel,
    card,
    esc,
    export_panel,
    human_span,
    kpi,
    raw_table_html,
    sparkline,
    top_gpus_card,
)
from dashboard.config import (
    AGG_WINDOW,
    BOTTOM_H,
    COLLECTION_NAME,
    HEALTHY_SECONDS,
    HISTORICAL_REFRESH_MS,
    MAIN_H,
    MONGO_URI,
    PAGE_SIZE,
)
from dashboard.data import query_aggregates, query_mongodb
from dashboard.theme import P


def render_historical():
    st_autorefresh(interval=HISTORICAL_REFRESH_MS, key="hist_refresh")
    hist_df, hist_err = query_mongodb()
    agg_df, _ = query_aggregates()

    # Say what is wrong instead of just "no data"
    if hist_err:
        st.error(f"Cannot read MongoDB at {MONGO_URI}: {hist_err}")
        return
    if hist_df.empty:
        st.warning(
            f"MongoDB is reachable, but the '{COLLECTION_NAME}' collection is still empty. The Spark job fills it from "
            "Kafka (first start takes about a minute). If it stays empty, check `docker compose logs spark`."
        )
        return

    # --- overview of everything collected so far
    now = pd.Timestamp.now(tz='UTC')
    first_ts, last_ts = hist_df['timestamp'].min(), hist_df['timestamp'].max()
    age = (now - last_ts).total_seconds()
    healthy = age < HEALTHY_SECONDS
    per_hour = hist_df.set_index('timestamp')['value'].resample('1h').count()
    n_prov = hist_df['provider'].nunique() if 'provider' in hist_df.columns else 0
    overview = [
        kpi(
            "Records Stored",
            f"{len(hist_df):,}",
            f"in MongoDB · {COLLECTION_NAME}",
            "flat",
            sparkline(per_hour.values, P["cyan"], "h1"),
        ),
        kpi("Time Span", human_span(last_ts - first_ts), f"{first_ts:%b %d %H:%M} → {last_ts:%b %d %H:%M} UTC", "flat"),
        kpi(
            "Coverage",
            f"{hist_df['sensor_id'].nunique()} GPUs",
            f"{n_prov} providers · {len(agg_df):,} Spark windows",
            "flat",
        ),
        kpi(
            "Pipeline",
            f'<span class="health {"ok" if healthy else "bad"}"><i></i>{"Healthy" if healthy else "Stale"}</span>',
            f"last write {int(age)}s ago" if age < 3600 else f"last write {human_span(now - last_ts)} ago",
            "up" if healthy else "down",
        ),
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
    label = (
        f"GPUs: {', '.join(selected_gpus) if len(selected_gpus) <= 6 else str(len(selected_gpus)) + ' selected'} | "
        f"Providers: {'all' if selected_prov is None or len(selected_prov) == len(prov_options) else ', '.join(selected_prov)} | Range: {span}"
    )

    # --- main chart + aggregation
    col_chart, col_side = st.columns([3, 1])
    with col_chart:
        with card("hchart", MAIN_H + 20):
            st.markdown(
                '<div class="card-title">Historical Price Trends<span class="card-sub">UTC · drag the slider to zoom</span></div>',
                unsafe_allow_html=True,
            )
            show_plot(historical_figure(filtered, MAIN_H - 60))
    with col_side:
        aggregation_panel(filtered, MAIN_H + 20)

    # --- Spark aggregates + export
    s1, s2 = st.columns([3, 1])
    with s1:
        with card("spark", BOTTOM_H):
            head_l, head_r = st.columns([3, 1.2], vertical_alignment="center")
            head_l.markdown(
                f'<div class="card-title">Spark Aggregates<span class="chip">Structured Streaming · {esc(AGG_WINDOW)} windows</span></div>',
                unsafe_allow_html=True,
            )
            if agg_f.empty:
                st.info("Spark has not produced aggregate windows for this selection yet.")
            else:
                choice = head_r.selectbox(
                    "GPU", sorted(agg_f['sensor_id'].unique().tolist()), label_visibility="collapsed", key="agg_gpu"
                )
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
                page = st.number_input(
                    "Page", min_value=1, max_value=pages, step=1, key="raw_page", label_visibility="collapsed"
                )
            head_l.markdown(
                f'<div class="card-title">All Collected Data<span class="card-sub">{len(filtered):,} rows · page {page} of {pages:,}</span></div>',
                unsafe_allow_html=True,
            )
            st.markdown(raw_table_html(filtered, page), unsafe_allow_html=True)
    with b2:
        top_gpus_card(filtered, BOTTOM_H + 30, "htop", limit=7)
