import streamlit as st
import plotly.express as px


def render(query, start_date, end_date):
    st.subheader("Alert hotspots by stop")
    st.caption("Number of alerts affecting bus stops in the system for the selected date range. **Route filters will not affect this view.**")

    alerts_stop_df = query(f"""
        SELECT
            stop_name,
            AVG(stop_lat) as stop_lat,
            AVG(stop_lon) as stop_lon,
            SUM(alert_count) as alert_count,
            SUM(severe_count) as severe_count,
            SUM(warning_count) as warning_count,
            SUM(info_count) as info_count,
        FROM LEMMING_DB.FINAL_PROJECT_MART.METRIC_ALERTS_BY_DAY_STOPS
        WHERE alert_date BETWEEN '{start_date}' AND '{end_date}'
          AND stop_lat IS NOT NULL
          AND stop_lon IS NOT NULL
        GROUP BY stop_name
    """)

    if not alerts_stop_df.empty:
        stop_agg = alerts_stop_df.groupby(['STOP_NAME','STOP_LAT','STOP_LON']).agg(
            ALERT_COUNT=('ALERT_COUNT','sum'),
            SEVERE_COUNT=('SEVERE_COUNT','sum'),
            WARNING_COUNT=('WARNING_COUNT','sum'),
            INFO_COUNT=('INFO_COUNT','sum'),
        ).reset_index()

        fig_map = px.scatter_mapbox(
            stop_agg,
            lat='STOP_LAT',
            lon='STOP_LON',
            size='ALERT_COUNT',
            color='SEVERE_COUNT',
            hover_name='STOP_NAME',
            hover_data={'ALERT_COUNT': True, 'SEVERE_COUNT': True},
            color_continuous_scale=px.colors.sequential.Reds[3:],
            size_max=20,
            zoom=10.75,
            title='Alert hotspots',
            subtitle='Bubble size = alert count, Color = number of severe alerts'
        )
        fig_map.update_layout(
            mapbox_style='carto-positron',
            height=500,
            margin={"r":0,"t":60,"l":0,"b":0}
        )
        st.plotly_chart(fig_map, use_container_width=True)

        type_to_col = {
            "SEVERE": "SEVERE_COUNT",
            "WARNING": "WARNING_COUNT",
            "INFO": "INFO_COUNT",
        }
        legend_names = {
            'SEVERE_COUNT': 'SEVERE',
            'WARNING_COUNT': 'WARNING',
            'INFO_COUNT': 'INFO',
        }
        if "top_stops_alert_types" not in st.session_state:
            st.session_state["top_stops_alert_types"] = list(type_to_col.keys())
        selected_types = st.session_state["top_stops_alert_types"]

        if not selected_types:
            st.info("Select at least one alert type to display the Top 10 ranking.")
        else:
            selected_cols = [type_to_col[t] for t in selected_types]
            stop_ranked = stop_agg.copy()
            stop_ranked["TOTAL"] = stop_ranked[selected_cols].sum(axis=1)
            stop_ranked = stop_ranked[stop_ranked["TOTAL"] > 0]

            if stop_ranked.empty:
                st.info("No stops with the selected alert types in this date range.")
            else:
                top_stops = stop_ranked.sort_values('TOTAL', ascending=False).head(10)
                top_stops_sorted = top_stops.sort_values('TOTAL')
                top_stops_sorted['STOP_NAME'] = top_stops_sorted['STOP_NAME'].astype(str)
                fig_top = px.bar(
                    top_stops_sorted,
                    x=selected_cols,
                    y='STOP_NAME',
                    orientation='h',
                    title='Top 10 stops most affected by alerts',
                    labels={'value': 'Alert count', 'STOP_NAME': 'Stop', 'variable': 'Alert type'},
                    barmode='stack',
                    color_discrete_map={
                        'SEVERE_COUNT': '#e63946',
                        'WARNING_COUNT': '#f4a261',
                        'INFO_COUNT':    '#457b9d',
                    },
                )
                fig_top.for_each_trace(lambda t: t.update(name=legend_names.get(t.name, t.name)))
                fig_top.update_yaxes(
                    type='category',
                    categoryorder='array',
                    categoryarray=top_stops_sorted['STOP_NAME'].tolist(),
                    title=None,
                )
                st.plotly_chart(fig_top, use_container_width=True)

        st.multiselect(
            "Alert types to include in Top 10 ranking",
            options=list(type_to_col.keys()),
            key="top_stops_alert_types",
        )
    else:
        st.info("No alert stop data for selected date range.")
