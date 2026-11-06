"""Real-time tab: live view of the Kafka stream."""

import streamlit as st
from streamlit_autorefresh import st_autorefresh

from dashboard.charts import latest_by_gpu_figure, market_tape_figure, show_plot
from dashboard.components import (
    aggregation_panel,
    card,
    chip_icon,
    delta_html,
    esc,
    feed_html,
    kpi,
    sparkline,
    top_gpus_card,
)
from dashboard.config import BOTTOM_H, KAFKA_BOOTSTRAP, MAIN_H, REALTIME_REFRESH_MS
from dashboard.data import consume_kafka_data
from dashboard.theme import P


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
    polls = df.groupby('timestamp')['value'].mean()  # the producer sends one message per GPU per poll
    latest_by = df.groupby('sensor_id').tail(1)
    cheap, pricey = latest_by.loc[latest_by['value'].idxmin()], latest_by.loc[latest_by['value'].idxmax()]
    delta_txt, delta_cls = delta_html(
        float(polls.iloc[-1]), float(polls.iloc[-2]) if len(polls) > 1 else None, "previous poll"
    )
    n_prov = df['provider'].nunique() if 'provider' in df.columns else 0

    def gpu_card(label, row, cls, color, uid):
        series = df[df['sensor_id'] == row['sensor_id']]['value']
        return kpi(
            label,
            esc(row['sensor_id']),
            f'<span class="{cls}">${row["value"]:.4f}/hr</span> <span class="pill">{esc(str(row.get("provider", "")).capitalize())}</span>',
            "flat",
            sparkline(series, color, uid),
        )

    kpis = [
        kpi(
            "Last Update",
            latest_ts.strftime("%H:%M:%S"),
            f"UTC · {df['sensor_id'].nunique()} GPUs · {n_prov} providers",
            "flat",
            chip_icon(P["cyan"]),
        ),
        kpi(
            "Market Avg Price", f"${polls.iloc[-1]:.4f}", delta_txt, delta_cls, sparkline(polls.values, P["cyan"], "a")
        ),
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
                unsafe_allow_html=True,
            )
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
            st.markdown(
                '<div class="card-title">Live Feed<span class="card-sub">change vs previous tick of the same GPU</span></div>',
                unsafe_allow_html=True,
            )
            st.markdown(feed_html(df), unsafe_allow_html=True)
