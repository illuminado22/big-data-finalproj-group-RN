"""
Formatted exports of the history data: Excel (.xlsx), PDF report and chart image (.jpg / .png).

All functions take the (filtered) history DataFrame (columns: timestamp [UTC], sensor_id, provider,
value) and optionally the Spark aggregates DataFrame (window_start, sensor_id, avg_price, ...).
"""

import io
from datetime import datetime, timezone

import pandas as pd
from matplotlib.dates import AutoDateLocator, ConciseDateFormatter
from matplotlib.figure import Figure

VIOLET = "#7C3AED"
CYAN = "#0891B2"
INK = "#141A33"
MUTED = "#5B6684"
SERIES = ["#0891B2", "#7C3AED", "#DB2777", "#D97706", "#059669", "#2563EB", "#E5341A", "#65A30D", "#0E7490", "#9333EA"]
MAX_SERIES = 10


def _now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _naive(df, cols=("timestamp",)):
    """Excel cannot store timezone-aware datetimes."""
    out = df.copy()
    for c in cols:
        if c in out.columns:
            out[c] = pd.to_datetime(out[c], utc=True).dt.tz_localize(None)
    return out


# ---------------------------------------------------------------- summaries
def gpu_summary(df):
    rows = []
    for gpu, g in df.sort_values("timestamp").groupby("sensor_id"):
        first, last = float(g["value"].iloc[0]), float(g["value"].iloc[-1])
        rows.append(
            {
                "GPU": gpu,
                "Samples": len(g),
                "Min": g["value"].min(),
                "Max": g["value"].max(),
                "Mean": g["value"].mean(),
                "Median": g["value"].median(),
                "Std Dev": g["value"].std(ddof=0),
                "First": first,
                "Latest": last,
                "Change %": (last - first) / first * 100 if first else 0.0,
            }
        )
    return pd.DataFrame(rows).sort_values("Mean", ascending=False).reset_index(drop=True)


def provider_summary(df):
    if "provider" not in df.columns:
        return pd.DataFrame(columns=["Provider", "Samples", "GPUs", "Mean", "Min", "Max"])
    g = df.groupby("provider")["value"].agg(["count", "mean", "min", "max"])
    g["GPUs"] = df.groupby("provider")["sensor_id"].nunique()
    g = g.reset_index().rename(
        columns={"provider": "Provider", "count": "Samples", "mean": "Mean", "min": "Min", "max": "Max"}
    )
    g["Provider"] = g["Provider"].astype(str).str.capitalize()
    return (
        g[["Provider", "Samples", "GPUs", "Mean", "Min", "Max"]]
        .sort_values("Mean", ascending=False)
        .reset_index(drop=True)
    )


def overall_stats(df):
    t0, t1 = df["timestamp"].min(), df["timestamp"].max()
    return [
        ("Records", f"{len(df):,}"),
        ("GPU models", f"{df['sensor_id'].nunique()}"),
        ("Providers", f"{df['provider'].nunique() if 'provider' in df.columns else 0}"),
        ("From (UTC)", t0.strftime("%Y-%m-%d %H:%M")),
        ("To (UTC)", t1.strftime("%Y-%m-%d %H:%M")),
        ("Mean price", f"${df['value'].mean():.4f}/hr"),
        ("Min price", f"${df['value'].min():.4f}/hr"),
        ("Max price", f"${df['value'].max():.4f}/hr"),
    ]


# ---------------------------------------------------------------- chart image
def chart_image(df, fmt="jpg", subtitle=""):
    """Price-history line chart (one line per GPU) as JPG/PNG bytes. White background, print friendly."""
    fig = Figure(figsize=(11, 4.8), dpi=150, facecolor="white")
    ax = fig.subplots()
    order = df.groupby("sensor_id")["value"].mean().sort_values(ascending=False).index[:MAX_SERIES]
    for i, gpu in enumerate(order):
        g = df[df["sensor_id"] == gpu].sort_values("timestamp")
        ax.plot(g["timestamp"], g["value"], color=SERIES[i % len(SERIES)], linewidth=1.6, label=str(gpu))
    ax.set_ylabel("Price (USD / hour)", color=MUTED)
    ax.yaxis.set_major_formatter(lambda v, _: f"${v:,.2f}")
    loc = AutoDateLocator()
    ax.xaxis.set_major_locator(loc)
    ax.xaxis.set_major_formatter(ConciseDateFormatter(loc))
    ax.grid(True, color="#E5E8F2", linewidth=0.8)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color("#C9CEDF")
    ax.tick_params(colors=MUTED, labelsize=8)
    ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1.0), frameon=False, fontsize=8, labelcolor=INK)
    fig.suptitle("GPU Cloud Prices - History", x=0.01, ha="left", fontsize=13, fontweight="bold", color=INK)
    if subtitle:
        ax.set_title(subtitle, loc="left", fontsize=8, color=MUTED, pad=8)
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    buf = io.BytesIO()
    kwargs = {"pil_kwargs": {"quality": 92}} if fmt in ("jpg", "jpeg") else {}
    fig.savefig(buf, format=fmt, facecolor="white", **kwargs)
    return buf.getvalue()


# ---------------------------------------------------------------- Excel
def build_xlsx(df, agg_df=None, filters_text=""):
    from openpyxl import Workbook
    from openpyxl.chart import BarChart, LineChart, Reference
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
    from openpyxl.utils import get_column_letter

    head_fill = PatternFill("solid", fgColor="4C1D95")
    head_font = Font(bold=True, color="FFFFFF")
    thin = Side(style="thin", color="E0E3EF")
    border = Border(bottom=thin)

    def write_table(ws, frame, start_row, formats=None):
        for j, col in enumerate(frame.columns, 1):
            c = ws.cell(row=start_row, column=j, value=col)
            c.fill, c.font, c.alignment = head_fill, head_font, Alignment(horizontal="center")
        for i, row in enumerate(frame.itertuples(index=False), start_row + 1):
            for j, val in enumerate(row, 1):
                if isinstance(val, pd.Timestamp):
                    val = val.to_pydatetime()
                c = ws.cell(row=i, column=j, value=val)
                c.border = border
                fmt = (formats or {}).get(frame.columns[j - 1])
                if fmt:
                    c.number_format = fmt
        return start_row + len(frame)

    def autosize(ws, minimum=11, maximum=34, from_row=1):
        for col in ws.columns:
            width = max((len(str(c.value)) for c in col if c.value is not None and c.row >= from_row), default=0)
            ws.column_dimensions[get_column_letter(col[0].column)].width = max(minimum, min(maximum, width + 3))

    d = _naive(df).sort_values("timestamp")
    wb = Workbook()

    # --- Summary
    ws = wb.active
    ws.title = "Summary"
    ws["A1"] = "GPU Market Analytics - History Report"
    ws["A1"].font = Font(bold=True, size=16, color="4C1D95")
    ws["A2"], ws["B2"] = "Generated", _now()
    ws["A3"], ws["B3"] = "Filters", filters_text
    for r in (2, 3):
        ws.cell(row=r, column=1).font = Font(bold=True)
    for k, (label, value) in enumerate(overall_stats(df), start=5):
        ws.cell(row=k, column=1, value=label).font = Font(bold=True)
        ws.cell(row=k, column=2, value=value)
    summary = gpu_summary(df)
    top = 5 + len(overall_stats(df)) + 2
    ws.cell(row=top - 1, column=1, value="Per-GPU aggregation").font = Font(bold=True, size=12)
    money = "$#,##0.0000"
    end = write_table(
        ws,
        summary,
        top,
        {c: money for c in ("Min", "Max", "Mean", "Median", "Std Dev", "First", "Latest")} | {"Change %": '0.00"%"'},
    )
    autosize(ws, from_row=5)
    ws.column_dimensions["A"].width = 24
    chart = BarChart()
    chart.type, chart.title, chart.style = "bar", "Average price per GPU (USD/hr)", 10
    chart.add_data(
        Reference(ws, min_col=summary.columns.get_loc("Mean") + 1, min_row=top, max_row=end), titles_from_data=True
    )
    chart.set_categories(Reference(ws, min_col=1, min_row=top + 1, max_row=end))
    chart.legend, chart.height, chart.width = None, 9, 17
    ws.add_chart(chart, f"M{top - 1}")

    # --- Providers
    wp = wb.create_sheet("Providers")
    write_table(wp, provider_summary(df), 1, {c: money for c in ("Mean", "Min", "Max")})
    autosize(wp)

    # --- Spark aggregates
    if agg_df is not None and not agg_df.empty:
        a = _naive(agg_df, ("window_start", "window_end")).sort_values(["window_start", "sensor_id"])
        a = a[
            ["window_start", "window_end", "sensor_id", "avg_price", "min_price", "max_price", "std_price", "samples"]
        ]
        a.columns = ["Window start (UTC)", "Window end (UTC)", "GPU", "Avg", "Min", "Max", "Std Dev", "Samples"]
        wa = wb.create_sheet("Spark Aggregates")
        write_table(
            wa,
            a,
            1,
            {
                "Window start (UTC)": "yyyy-mm-dd hh:mm",
                "Window end (UTC)": "yyyy-mm-dd hh:mm",
                **{c: money for c in ("Avg", "Min", "Max", "Std Dev")},
            },
        )
        wa.freeze_panes, wa.auto_filter.ref = "A2", wa.dimensions
        autosize(wa)

    # --- Price trends (pivot + native Excel line chart)
    gpus = list(d.groupby("sensor_id")["value"].mean().sort_values(ascending=False).index[:MAX_SERIES])
    sub = d[d["sensor_id"].isin(gpus)]
    span = (sub["timestamp"].max() - sub["timestamp"].min()).total_seconds()
    rule = next(
        (r for r, s in (("1min", 60), ("5min", 300), ("15min", 900), ("1h", 3600), ("6h", 21600)) if span / s <= 1500),
        "1D",
    )
    pivot = sub.set_index("timestamp").groupby("sensor_id")["value"].resample(rule).mean().unstack(0)
    pivot = pivot.interpolate(limit_area="inside").reset_index().rename(columns={"timestamp": "Time (UTC)"})
    wt = wb.create_sheet("Price Trends")
    last_row = write_table(wt, pivot, 1, {"Time (UTC)": "yyyy-mm-dd hh:mm", **{g: money for g in gpus}})
    autosize(wt)
    if last_row > 2:
        lc = LineChart()
        lc.title, lc.style, lc.height, lc.width = f"Price per GPU ({rule} average)", 12, 11, 26
        lc.add_data(Reference(wt, min_col=2, max_col=1 + len(gpus), min_row=1, max_row=last_row), titles_from_data=True)
        lc.set_categories(Reference(wt, min_col=1, min_row=2, max_row=last_row))
        lc.x_axis.number_format, lc.x_axis.majorTimeUnit = "mm-dd hh:mm", "days"
        lc.x_axis.delete = lc.y_axis.delete = False
        wt.add_chart(lc, f"{get_column_letter(len(gpus) + 3)}2")

    # --- Raw data (everything in the current filter)
    wr = wb.create_sheet("Raw Data")
    raw = (
        d[["timestamp", "sensor_id", "provider", "value"]]
        if "provider" in d.columns
        else d[["timestamp", "sensor_id", "value"]]
    )
    raw = raw.rename(
        columns={"timestamp": "Timestamp (UTC)", "sensor_id": "GPU", "provider": "Provider", "value": "Price (USD/hr)"}
    )
    write_table(wr, raw, 1, {"Timestamp (UTC)": "yyyy-mm-dd hh:mm:ss", "Price (USD/hr)": money})
    wr.freeze_panes, wr.auto_filter.ref = "A2", wr.dimensions
    autosize(wr)

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ---------------------------------------------------------------- PDF
def build_pdf(df, agg_df=None, filters_text=""):
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import cm
    from reportlab.platypus import Image, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    ss = getSampleStyleSheet()
    title = ParagraphStyle(
        "t", parent=ss["Title"], alignment=0, textColor=colors.HexColor("#4C1D95"), fontSize=20, spaceAfter=2
    )
    h2 = ParagraphStyle(
        "h2",
        parent=ss["Heading2"],
        textColor=colors.HexColor(INK),
        fontSize=13,
        spaceBefore=12,
        spaceAfter=6,
        keepWithNext=1,
    )
    small = ParagraphStyle("s", parent=ss["Normal"], fontSize=8.5, textColor=colors.HexColor(MUTED), leading=11)

    def table(rows, col_widths=None, align_right_from=1):
        t = Table(rows, colWidths=col_widths, repeatRows=1)
        t.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#4C1D95")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, -1), 8),
                    ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F3F4FA")]),
                    ("LINEBELOW", (0, 0), (-1, -1), 0.25, colors.HexColor("#E0E3EF")),
                    ("ALIGN", (align_right_from, 0), (-1, -1), "RIGHT"),
                    ("TOPPADDING", (0, 0), (-1, -1), 3),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                ]
            )
        )
        return t

    def footer(canvas, doc):
        canvas.saveState()
        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(colors.HexColor(MUTED))
        canvas.drawString(1.5 * cm, 0.9 * cm, "RN | GPU Market Analytics - generated " + _now())
        canvas.drawRightString(landscape(A4)[0] - 1.5 * cm, 0.9 * cm, f"Page {doc.page}")
        canvas.restoreState()

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=landscape(A4),
        leftMargin=1.5 * cm,
        rightMargin=1.5 * cm,
        topMargin=1.4 * cm,
        bottomMargin=1.6 * cm,
        title="GPU Market Analytics - History Report",
        author="RN",
    )
    story = [
        Paragraph("GPU Market Analytics", title),
        Paragraph(
            "Cloud GPU price history report (Aquanode marketplace API &rarr; Kafka &rarr; Spark &rarr; MongoDB)", small
        ),
        Spacer(1, 4),
        Paragraph(f"Generated: {_now()} &nbsp;|&nbsp; Filters: {filters_text}", small),
        Spacer(1, 8),
    ]

    stats = overall_stats(df)
    kpi_tbl = Table(
        [
            [f"{k}" for k, _ in stats[:4]],
            [v for _, v in stats[:4]],
            [k for k, _ in stats[4:]],
            [v for _, v in stats[4:]],
        ],
        colWidths=[6.6 * cm] * 4,
    )
    kpi_tbl.setStyle(
        TableStyle(
            [
                ("FONTSIZE", (0, 0), (-1, -1), 8),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor(MUTED)),
                ("TEXTCOLOR", (0, 2), (-1, 2), colors.HexColor(MUTED)),
                ("FONTNAME", (0, 1), (-1, 1), "Helvetica-Bold"),
                ("FONTNAME", (0, 3), (-1, 3), "Helvetica-Bold"),
                ("FONTSIZE", (0, 1), (-1, 1), 12),
                ("FONTSIZE", (0, 3), (-1, 3), 12),
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#F3F4FA")),
                ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#E0E3EF")),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ]
        )
    )
    story += [kpi_tbl, Paragraph("Price history", h2)]
    story.append(Image(io.BytesIO(chart_image(df, "png", filters_text)), width=24 * cm, height=24 * cm * 4.8 / 11))

    s = gpu_summary(df)
    story += [Paragraph("Aggregation per GPU", h2)]
    rows = [list(s.columns)] + [
        [r["GPU"], f"{int(r['Samples']):,}"]
        + [f"${r[c]:.4f}" for c in ("Min", "Max", "Mean", "Median", "Std Dev", "First", "Latest")]
        + [f"{r['Change %']:+.2f}%"]
        for _, r in s.iterrows()
    ]
    story.append(table(rows, align_right_from=1))

    p = provider_summary(df)
    if not p.empty:
        story += [Paragraph("Aggregation per provider", h2)]
        story.append(
            table(
                [list(p.columns)]
                + [
                    [r.Provider, f"{r.Samples:,}", r.GPUs, f"${r.Mean:.4f}", f"${r.Min:.4f}", f"${r.Max:.4f}"]
                    for r in p.itertuples(index=False)
                ],
                col_widths=[5 * cm, 3 * cm, 3 * cm, 3.5 * cm, 3.5 * cm, 3.5 * cm],
            )
        )

    if agg_df is not None and not agg_df.empty:
        a = agg_df.sort_values("window_start", ascending=False).head(25)
        story += [Paragraph("Spark windowed aggregates (latest 25 windows; all windows are in the Excel export)", h2)]
        story.append(
            table(
                [["Window start (UTC)", "GPU", "Avg", "Min", "Max", "Std Dev", "Samples"]]
                + [
                    [
                        r.window_start.strftime("%Y-%m-%d %H:%M"),
                        r.sensor_id,
                        f"${r.avg_price:.4f}",
                        f"${r.min_price:.4f}",
                        f"${r.max_price:.4f}",
                        f"{r.std_price:.4f}",
                        int(r.samples),
                    ]
                    for r in a.itertuples(index=False)
                ],
                align_right_from=2,
            )
        )

    latest = df.sort_values("timestamp", ascending=False).head(40)
    story += [
        Paragraph(f"Most recent records (40 of {len(df):,}; the complete data set is in the Excel / CSV export)", h2)
    ]
    story.append(
        table(
            [["Timestamp (UTC)", "GPU", "Provider", "Price (USD/hr)"]]
            + [
                [
                    r.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    r.sensor_id,
                    str(getattr(r, "provider", "")).capitalize(),
                    f"${r.value:.4f}",
                ]
                for r in latest.itertuples(index=False)
            ],
            align_right_from=3,
        )
    )
    doc.build(story, onFirstPage=footer, onLaterPages=footer)
    return buf.getvalue()
