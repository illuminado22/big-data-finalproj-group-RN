"""Plotly figures."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboard.components import esc
from dashboard.theme import PLOT_CONFIG, P, series


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
        template=P["plotly"],
        height=height,
        margin=dict(l=6, r=6, t=6, b=6),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(family="Poppins, sans-serif", color=P["muted"], size=12),
        hovermode='x unified',
        hoverlabel=dict(bgcolor=P["tip_bg"], font_color=P["text"], bordercolor=P["violet"]),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, x=0, font=dict(color=P["text"])),
        xaxis=dict(showgrid=False, zeroline=False, hoverformat='%b %d, %H:%M:%S', **spike),
        yaxis=dict(
            side='right',
            range=[(lo - pad) if pct else max(0, lo - pad), hi + pad],
            showgrid=True,
            gridcolor=P["grid"],
            tickprefix='' if pct else '$',
            ticksuffix='%' if pct else '',
            zeroline=pct,
            zerolinecolor=P["spike"],
            spikesnap='cursor',
            **spike,
        ),
    )
    return fig


def market_tape_figure(df, gpu_filter, height):
    all_gpus = gpu_filter.startswith("All")
    if all_gpus:
        # every GPU as % change since the start of the live window: comparable even though prices range from $0.50 to $7+
        colors = series()
        fig = go.Figure()
        pct_all = []
        for i, (gpu, g) in enumerate(df.sort_values('timestamp').groupby('sensor_id')):
            base = float(g['value'].iloc[0]) or 1.0
            pct = (g['value'] / base - 1) * 100
            pct_all.append(pct)
            fig.add_trace(
                go.Scatter(
                    x=g['timestamp'],
                    y=pct,
                    mode='lines',
                    name=str(gpu),
                    line=dict(color=colors[i % len(colors)], width=1.8),
                    customdata=g['value'],
                    hovertemplate="%{y:+.2f}% · $%{customdata:.4f}/hr<extra>" + esc(gpu) + "</extra>",
                )
            )
        style_trading_fig(fig, pd.concat(pct_all), height, pct=True)
        return fig
    color = P["violet"]
    frame = df[df['sensor_id'] == gpu_filter].sort_values('timestamp')
    fig = go.Figure(
        go.Scatter(
            x=frame['timestamp'],
            y=frame['value'],
            mode='lines+markers',
            line=dict(color=color, width=2.2),
            marker=dict(size=4, color=color),
            fill='tozeroy',
            fillcolor='rgba(139,92,246,0.12)',
            customdata=frame[['sensor_id']].values,
            hovertemplate="<b>%{customdata[0]}</b><br>$%{y:.4f}/hr<extra></extra>",
            name="Price",
        )
    )
    style_trading_fig(fig, frame['value'], height)
    last = float(frame['value'].iloc[-1])
    fig.add_hline(
        y=last,
        line_dash='dot',
        line_color=color,
        line_width=1,
        annotation_text=f"${last:.4f}",
        annotation_position='right',
        annotation_font=dict(color=P["tag_text"], size=11),
        annotation_bgcolor=color,
    )
    fig.update_layout(showlegend=False)
    return fig


def historical_figure(df, height):
    colors = series()
    fig = go.Figure()
    for i, (gpu, g) in enumerate(df.groupby('sensor_id')):
        g = g.sort_values('timestamp')
        fig.add_trace(
            go.Scatter(
                x=g['timestamp'],
                y=g['value'],
                mode='lines',
                name=str(gpu),
                line=dict(color=colors[i % len(colors)], width=2),
                hovertemplate="$%{y:.4f}/hr<extra>" + esc(gpu) + "</extra>",
            )
        )
    style_trading_fig(fig, df['value'], height)
    fig.update_xaxes(rangeslider=dict(visible=True, thickness=0.06, bgcolor=P["soft"]))
    return fig


def spark_agg_figure(agg, gpu, height):
    """Average line with the min-max band for one GPU, from the windows Spark computed."""
    g = agg[agg['sensor_id'] == gpu].sort_values('window_start')
    x = g['window_start']
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(x=x, y=g['max_price'], mode='lines', line=dict(width=0), hoverinfo='skip', showlegend=False)
    )
    fig.add_trace(
        go.Scatter(
            x=x,
            y=g['min_price'],
            mode='lines',
            line=dict(width=0),
            fill='tonexty',
            fillcolor='rgba(139,92,246,0.18)',
            hoverinfo='skip',
            showlegend=False,
        )
    )
    fig.add_trace(
        go.Scatter(
            x=x,
            y=g['avg_price'],
            mode='lines+markers',
            line=dict(color=P["violet"], width=2.2),
            marker=dict(size=4, color=P["violet"]),
            customdata=g[['min_price', 'max_price', 'samples']].values,
            name="avg",
            hovertemplate="avg $%{y:.4f}/hr<br>min $%{customdata[0]:.4f} · max $%{customdata[1]:.4f}<br>%{customdata[2]} samples<extra></extra>",
        )
    )
    style_trading_fig(fig, pd.concat([g['min_price'], g['max_price']]), height)
    fig.update_layout(showlegend=False)
    return fig


def latest_by_gpu_figure(df, height):
    latest = df.sort_values('timestamp').groupby('sensor_id').tail(1).sort_values('value')
    fig = go.Figure(
        go.Bar(
            x=latest['value'],
            y=latest['sensor_id'],
            orientation='h',
            marker=dict(color=latest['value'], colorscale=[[0, P["cyan"]], [1, P["violet"]]], line=dict(width=0)),
            text=[f"${v:.2f}" for v in latest['value']],
            textposition='outside',
            cliponaxis=False,
            textfont=dict(color=P["text"], size=11),
            hovertemplate="<b>%{y}</b><br>$%{x:.4f}/hr<extra></extra>",
        )
    )
    fig.update_layout(
        template=P["plotly"],
        height=height,
        margin=dict(l=6, r=48, t=4, b=4),
        bargap=0.35,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(family="Poppins, sans-serif", color=P["muted"], size=11),
        xaxis=dict(showgrid=False, zeroline=False, visible=False, range=[0, float(latest['value'].max()) * 1.2]),
        yaxis=dict(showgrid=False, zeroline=False, tickfont=dict(color=P["text"])),
    )
    return fig
