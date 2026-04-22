import math

import streamlit as st
import plotly.express as px


def render(query, start_date, end_date, route_filter=None):
    st.subheader("Service alerts by route")
    st.caption("Number of alerts that affect the selected routes for the selected date ranges.")

    route_clause = ""
    if route_filter:
        route_clause = f"AND route_name IN ('{route_filter}')"

    alerts_route_df = query(f"""
        SELECT
            alert_date,
            route_id,
            route_name,
            alert_count,
            severe_count,
            warning_count,
            info_count
        FROM LEMMING_DB.FINAL_PROJECT_MART.METRIC_ALERTS_BY_DAY
        WHERE alert_date BETWEEN '{start_date}' AND '{end_date}'
        {route_clause}
        ORDER BY alert_date DESC
    """)

    if not alerts_route_df.empty:
        daily = alerts_route_df.groupby('ALERT_DATE')[
            ['SEVERE_COUNT', 'WARNING_COUNT', 'INFO_COUNT']
        ].sum().reset_index()

        fig = px.bar(
            daily,
            x='ALERT_DATE',
            y=['SEVERE_COUNT', 'WARNING_COUNT', 'INFO_COUNT'],
            title='Daily alert counts by type',
            subtitle="Common causes for each type - <b>SEVERE</b>: delays >20 min · <b>WARNING</b>: delays <20 min · <b>INFO</b>: detours, snow routes, or other advisories",
            labels={
                'value': 'Alert count',
                'ALERT_DATE': 'Date',
                'variable': 'Alert type'
            },
            barmode='stack',
            color_discrete_map={
                'SEVERE_COUNT': '#e63946',
                'WARNING_COUNT': '#f4a261',
                'INFO_COUNT':    '#457b9d'
            }
        )
        legend_names = {
            'SEVERE_COUNT': 'SEVERE',
            'WARNING_COUNT': 'WARNING',
            'INFO_COUNT': 'INFO',
        }
        fig.for_each_trace(lambda t: t.update(name=legend_names.get(t.name, t.name)))
        max_daily_total = int(
            daily[['SEVERE_COUNT', 'WARNING_COUNT', 'INFO_COUNT']].sum(axis=1).max()
        )
        y_dtick = max(1, math.ceil(max_daily_total / 10))
        fig.update_yaxes(tickformat='d', tick0=0, dtick=y_dtick, rangemode='tozero')
        st.plotly_chart(fig, use_container_width=True)

        type_to_col = {
            "SEVERE": "severe_count",
            "WARNING": "warning_count",
            "INFO": "info_count",
        }
        if "top_routes_alert_types" not in st.session_state:
            st.session_state["top_routes_alert_types"] = list(type_to_col.keys())
        selected_types = st.session_state["top_routes_alert_types"]

        if not selected_types:
            st.info("Select at least one alert type to display the Top 10 ranking.")
        else:
            rank_expr = " + ".join(
                f"SUM({type_to_col[t]})" for t in selected_types
            )
            top_routes = query(f"""
                SELECT
                    route_name,
                    {rank_expr}        AS total,
                    SUM(severe_count)  AS severe_count,
                    SUM(warning_count) AS warning_count,
                    SUM(info_count)    AS info_count
                FROM LEMMING_DB.FINAL_PROJECT_MART.METRIC_ALERTS_BY_DAY
                WHERE alert_date BETWEEN '{start_date}' AND '{end_date}'
                GROUP BY 1
                HAVING {rank_expr} > 0
                ORDER BY total DESC
                LIMIT 10
            """)

            if top_routes.empty:
                st.info("No routes with the selected alert types in this date range.")
            else:
                top_routes_sorted = top_routes.sort_values("TOTAL")
                top_routes_sorted["ROUTE_NAME"] = top_routes_sorted["ROUTE_NAME"].astype(str)
                selected_cols = [f"{type_to_col[t].upper()}" for t in selected_types]
                fig2 = px.bar(
                    top_routes_sorted,
                    x=selected_cols,
                    y="ROUTE_NAME",
                    orientation="h",
                    title="Top 10 routes most affected by alerts",
                    subtitle="Routes that are most impacted by alerts. <b>Route filters will not affect this view.</b>",
                    labels={"value": "Alert count", "ROUTE_NAME": "Route", "variable": "Alert type"},
                    barmode="stack",
                    color_discrete_map={
                        'SEVERE_COUNT': '#e63946',
                        'WARNING_COUNT': '#f4a261',
                        'INFO_COUNT':    '#457b9d',
                    },
                )
                fig2.for_each_trace(lambda t: t.update(name=legend_names.get(t.name, t.name)))
                fig2.update_yaxes(
                    type="category",
                    categoryorder="array",
                    categoryarray=top_routes_sorted["ROUTE_NAME"].tolist(),
                    title=None,
                )
                st.plotly_chart(fig2, use_container_width=True)

        st.multiselect(
            "Alert types to include in Top 10 ranking",
            options=list(type_to_col.keys()),
            key="top_routes_alert_types",
        )
    else:
        st.info("No alert route data for selected date range.")
