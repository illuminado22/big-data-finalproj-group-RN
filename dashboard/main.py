"""Page layout: top bar (brand, tabs, theme switch), photo slideshow and the selected view."""

import streamlit as st
import streamlit.components.v1 as components

from dashboard.config import SLIDE_HEIGHT
from dashboard.slideshow import fetch_gpu_slides, slideshow_html
from dashboard.theme import THEME_LABELS, VIEW_LABELS, inject_css
from dashboard.views.historical import render_historical
from dashboard.views.realtime import render_realtime


def run():
    inject_css()

    # Top bar: RN brand (left) · tabs (center) · light/dark switch (far right)
    c1, c2, c3 = st.columns([1, 2, 1], vertical_alignment="center")
    with c1:
        st.markdown('<div class="rn-mark">RN<i></i></div>', unsafe_allow_html=True)
    with c2:
        view = st.radio("View", VIEW_LABELS, horizontal=True, label_visibility="collapsed", key="view_radio")
    with c3:
        st.radio(
            "Theme",
            list(THEME_LABELS.values()),
            horizontal=True,
            label_visibility="collapsed",
            index=1,
            key="theme_radio",
        )

    # Same HTML on every rerun, so the browser keeps the slideshow's 12s rhythm
    html = slideshow_html(fetch_gpu_slides())
    if hasattr(st, "iframe"):  # newer Streamlit (components.html is deprecated)
        st.iframe(html, height=SLIDE_HEIGHT + 4)
    else:
        components.html(html, height=SLIDE_HEIGHT + 4)

    if view == "Real-time":
        render_realtime()
    else:
        render_historical()
