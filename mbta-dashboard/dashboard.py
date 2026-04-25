import streamlit as st
from datetime import datetime, timedelta
from data_access import query
from tabs.occupancy_route_tab import render as render_occupancy_route
from tabs.alerts_route_tab import render as render_alerts_route
from tabs.alerts_stop_tab import render as render_alerts_stop
from tabs.on_time_performance_tab import render as render_on_time_performance
from tabs.service_delivered_tab import render as render_service_delivered


st.set_page_config(
    page_title="MBTA Bus Performance Dashboard",
    page_icon="🚌",
    layout="wide"
)

st.markdown("""
<style>
    .metric-card {
        background: #f0f4f8;
        border-radius: 8px;
        padding: 1rem;
        text-align: center;
    }
    .stTabs [data-baseweb="tab"] {
        font-size: 15px;
        font-weight: 500;
    }
</style>
""", unsafe_allow_html=True)

st.title("MBTA Bus Performance Dashboard")

st.sidebar.header("Filters")

date_range = st.sidebar.date_input(
    "Date range",
    value=[datetime(2026, 3, 1), datetime.now()],
    max_value=datetime.now()
)
start_date = date_range[0] if len(date_range) == 2 else datetime.now() - timedelta(days=14)
end_date = date_range[1] if len(date_range) == 2 else datetime.now()

routes_df = query("""
    SELECT DISTINCT ROUTE_ID, ROUTE_SHORT_NAME
    FROM LEMMING_DB.FINAL_PROJECT_MART.METRIC_OCCUPANCY_ROUTE_DAY
    WHERE ROUTE_SHORT_NAME IS NOT NULL
""")

# list routes in dropdown in numerical order, with non-numeric routes at the very end
def _route_sort_key(route):
    try:
        return (0, int(route), "")
    except (TypeError, ValueError):
        return (1, 0, str(route))

all_routes = sorted(routes_df["ROUTE_SHORT_NAME"].dropna().tolist(), key=_route_sort_key)
selected_routes = st.sidebar.multiselect(
    "Select routes (leave blank for all)",
    options=all_routes,
    default=[]
)

if not selected_routes:
    selected_routes = all_routes

route_filter = "', '".join(selected_routes)

occupancy_tab, alerts_route_tab, alerts_stop_tab, otp_tab, service_tab = st.tabs([
    "Occupancy %",
    "Alerts by route",
    "Alerts by stop",
    "On time performance",
    "Service delivered %"
])

with occupancy_tab:
    render_occupancy_route(query, start_date, end_date, route_filter)

with alerts_route_tab:
    render_alerts_route(query, start_date, end_date, route_filter)

with alerts_stop_tab:
    render_alerts_stop(query, start_date, end_date)

with otp_tab:
    render_on_time_performance(query, start_date, end_date, route_filter)

with service_tab:
    render_service_delivered(query, start_date, end_date, route_filter)

st.divider()
st.caption("MBTA Bus Performance Dashboard · Data Source: MBTA GTFS APIs · Built with Streamlit")
