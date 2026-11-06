"""Streamlit entry point:  streamlit run app.py"""

import streamlit as st

# must be the first Streamlit call of the script
st.set_page_config(page_title="RN · GPU Market", layout="wide", initial_sidebar_state="collapsed")

from dashboard.main import run  # noqa: E402

run()
