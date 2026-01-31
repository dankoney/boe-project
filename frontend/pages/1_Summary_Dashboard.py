# /frontend/pages/1_Summary_Dashboard.py

import streamlit as st
import requests
import pandas as pd
import plotly.express as px
from datetime import date, timedelta

# --- Authentication Check ---
# Only show this page if the user is logged in
if not st.session_state.get('authentication_status'):
    st.warning("Please log in from the home page to view this dashboard.")
    st.stop()

# --- Configuration ---
# NOTE: Set this to your running FastAPI service URL
API_BASE_URL = "http://127.0.0.1:8000" 

# --- Caching Function to speed up app loading ---
@st.cache_data(ttl=600)  # Cache data for 10 minutes
def fetch_summary_stats(query_type: str, query_value: str):
    """Fetches high-level KPIs from the FastAPI summary endpoint."""
    url = f"{API_BASE_URL}/stats/summary/"
    params = {"query_type": query_type, "query_value": query_value}
    try:
        response = requests.get(url, params=params)
        response.raise_for_status() # Raises an HTTPError for bad responses (4xx or 5xx)
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching summary data: {e}. Is the FastAPI server running at {API_BASE_URL}?")
        return None

@st.cache_data(ttl=600)
def fetch_timeseries_data(start: date, end: date):
    """Fetches time-series data from the FastAPI weight endpoint."""
    url = f"{API_BASE_URL}/stats/timeseries/weight/"
    params = {
        "start_date": start.isoformat(),
        "end_date": end.isoformat()
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching timeseries data: {e}")
        return None# Continuation of /frontend/pages/1_Summary_Dashboard.py

# --- Dashboard Layout ---
st.title("Executive Summary: Global Trade Flow")
st.markdown("---")

# 1. Filters (Sidebar)
with st.sidebar:
    st.header("Date Range Selection")
    
    today = date.today()
    default_start = today - timedelta(days=90)
    
    start_date = st.date_input("Start Date", default_start)
    end_date = st.date_input("End Date", today)

    st.header("Trade Filter (Demo)")
    # NOTE: These values will be passed to your FastAPI /stats/summary/ endpoint
    filter_type = st.selectbox(
        "Filter Type", 
        options=['importer_tin', 'hs_code', 'vessel_name'],
        index=1
    )
    filter_value = st.text_input(
        f"Enter {filter_type} value (e.g., 847130)", 
        value='847130' # Demo value
    )


# 2. Top-Row KPI Metrics
# ----------------------

summary_data = fetch_summary_stats(filter_type, filter_value)
if summary_data:
    
    # Format numbers for better enterprise display
    total_records = f"{summary_data.get('total_records', 0):,}"
    total_net_weight = f"{summary_data.get('grand_total_net_weight', 0.0):,.2f} Tons"
    total_gross_weight = f"{summary_data.get('grand_total_gross_weight', 0.0):,.2f} Tons"

    col1, col2, col3 = st.columns(3)

    # Use st.metric for a clean KPI card design
    col1.metric("Total Declarations", total_records, "Since Start Date")
    col2.metric("Total Net Weight", total_net_weight, "Filter Applied")
    col3.metric("Total Gross Weight", total_gross_weight, "Filter Applied")

st.markdown("---")

# 3. Time-Series Chart (Main Content)
# -----------------------------------
st.header("Weight Trend Analysis")

timeseries_data = fetch_timeseries_data(start_date, end_date)

if timeseries_data and timeseries_data != []:
    
    df_ts = pd.DataFrame(timeseries_data)
    df_ts['date'] = pd.to_datetime(df_ts['date'])
    
    # Create an interactive Plotly chart (best practice for Streamlit)
    fig = px.line(
        df_ts, 
        x='date', 
        y=['total_net_weight', 'total_gross_weight'],
        title='Daily Net vs. Gross Weight Trend',
        labels={'value': 'Total Weight (Tons)', 'date': 'Declaration Date'},
        template='plotly_white'
    )
    fig.update_layout(legend_title_text='Weight Type')
    
    # Display the chart
    st.plotly_chart(fig, width='stretch')
else:
    st.info("No time-series data found for the selected date range.")