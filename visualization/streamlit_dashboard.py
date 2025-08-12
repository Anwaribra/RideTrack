import streamlit as st
import clickhouse_connect
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import folium
from streamlit_folium import st_folium
import numpy as np
from datetime import datetime, timedelta
import os


st.set_page_config(
    page_title="NYC Taxi Analytics Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)


st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .sidebar .sidebar-content {
        background-color: #f8f9fa;
    }
    .nav-button {
        width: 100%;
        margin-bottom: 10px;
        padding: 10px;
        border: none;
        border-radius: 5px;
        background-color: #f0f2f6;
        color: #333;
        font-weight: 500;
        cursor: pointer;
        transition: all 0.3s ease;
    }
    .nav-button:hover {
        background-color: #e0e6ed;
        transform: translateY(-2px);
    }
    .nav-button.active {
        background-color: #1f77b4;
        color: white;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_resource
def init_connection():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "e6avfkndk8.us-west-2.aws.clickhouse.cloud"),
        user=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "PG_n1E.ujk9mo"),
        secure=True
    )


@st.cache_data(ttl=600)
def run_query(query):
    client = init_connection()
    try:
        result = client.query(query)
        return pd.DataFrame(result.result_set, columns=result.column_names)
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()


def main():
    st.markdown("<h1 class=\"main-header\">NYC Taxi Analytics Dashboard</h1>", unsafe_allow_html=True)
    
    # Sidebar navigation
    st.sidebar.title("Dashboard Navigation")
    page = st.sidebar.selectbox(
        "Choose a view:",
        ["Business Overview", "Revenue Analysis", "Trip Patterns", "Location Analytics", "Operational Metrics", "Customer Insights", "Predictive Trip Analytics"]
    )
    

    if page == "Business Overview":
        business_overview()
    elif page == "Revenue Analysis":
        revenue_analysis()
    elif page == "Trip Patterns":
        trip_patterns()
    elif page == "Location Analytics":
        location_analytics()
    elif page == "Operational Metrics":
        operational_metrics()
    elif page == "Customer Insights":
        customer_insights()
    elif page == "Predictive Trip Analytics":
        advanced_analytics()

def business_overview():
    st.header("Business Overview")
    
    
    col1, col2, col3, col4 = st.columns(4)
    
    summary_query = """
    SELECT 
        'Total Trips' as metric,
        COUNT(*)::String as value
    FROM fact_trips
    UNION ALL
    SELECT 
        'Total Revenue' as metric,
        '$' || toString(ROUND(SUM(total_amount), 2)) as value
    FROM fact_trips
    UNION ALL
    SELECT 
        'Average Fare' as metric,
        '$' || toString(ROUND(AVG(fare_amount), 2)) as value
    FROM fact_trips
    UNION ALL
    SELECT 
        'Average Trip Distance' as metric,
        toString(ROUND(AVG(trip_distance), 2)) || ' miles' as value
    FROM fact_trips
    """
    
    summary_df = run_query(summary_query)
    
    if not summary_df.empty:
        for i, (_, row) in enumerate(summary_df.iterrows()):
            if i < 4:
                cols = [col1, col2, col3, col4]
                with cols[i]:
                    st.metric(row['metric'], row['value'])
    
    
    st.subheader("Revenue by Hour of Day")
    hourly_revenue_query = """
    SELECT 
        dt.hour_of_day,
        COUNT(*) as trips,
        ROUND(SUM(ft.total_amount), 2) as revenue,
        ROUND(AVG(ft.total_amount), 2) as avg_fare
    FROM fact_trips ft
    JOIN dim_time dt ON ft.pickup_time_key = dt.time_key
    GROUP BY dt.hour_of_day
    ORDER BY dt.hour_of_day
    """
    
    hourly_df = run_query(hourly_revenue_query)
    
    if not hourly_df.empty:
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        fig.add_trace(
            go.Bar(x=hourly_df['hour_of_day'], y=hourly_df['revenue'], 
                   name="Revenue", marker_color='lightblue'),
            secondary_y=False,
        )
        
        fig.add_trace(
            go.Scatter(x=hourly_df['hour_of_day'], y=hourly_df['trips'], 
                      mode='lines+markers', name="Trip Count", 
                      line=dict(color='red', width=3)),
            secondary_y=True,
        )
        
        fig.update_xaxes(title_text="Hour of Day")
        fig.update_yaxes(title_text="Revenue ($)", secondary_y=False)
        fig.update_yaxes(title_text="Number of Trips", secondary_y=True)
        fig.update_layout(title="Revenue and Trip Count by Hour", height=400)
        
        st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("Payment Method Distribution")
    payment_query = """
    SELECT 
        dp.payment_description,
        COUNT(*) as trips,
        ROUND(SUM(ft.total_amount), 2) as revenue
    FROM fact_trips ft
    JOIN dim_payment dp ON ft.payment_key = dp.payment_key
    GROUP BY dp.payment_description
    ORDER BY revenue DESC
    """
    
    payment_df = run_query(payment_query)
    
    if not payment_df.empty:
        fig = px.pie(payment_df, values='trips', names='payment_description',
                    title="Trip Distribution by Payment Method")
        st.plotly_chart(fig, use_container_width=True)

def revenue_analysis():
    st.header("Revenue Analysis")
    
    # monthly revenue trends
    st.subheader("Monthly Revenue Trends")
    monthly_query = """
    SELECT 
        dt.year,
        dt.month,
        COUNT(*) as trips,
        ROUND(SUM(ft.total_amount), 2) as revenue,
        ROUND(AVG(ft.total_amount), 2) as avg_fare
    FROM fact_trips ft
    JOIN dim_time dt ON ft.pickup_time_key = dt.time_key
    GROUP BY dt.year, dt.month
    ORDER BY dt.year DESC, dt.month DESC
    LIMIT 12
    """
    
    monthly_df = run_query(monthly_query)
    
    if not monthly_df.empty:
        monthly_df['month_year'] = monthly_df['year'].astype(str) + '-' + monthly_df['month'].astype(str).str.zfill(2)
        
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        fig.add_trace(
            go.Bar(x=monthly_df['month_year'], y=monthly_df['revenue'], 
                   name="Revenue", marker_color='green'),
            secondary_y=False,
        )
        
        fig.add_trace(
            go.Scatter(x=monthly_df['month_year'], y=monthly_df['avg_fare'], 
                      mode='lines+markers', name="Average Fare", 
                      line=dict(color='orange', width=3)),
            secondary_y=True,
        )
        
        fig.update_layout(title="Monthly Revenue and Average Fare Trends", height=400)
        fig.update_xaxes(title_text="Month")
        fig.update_yaxes(title_text="Revenue ($)", secondary_y=False)
        fig.update_yaxes(title_text="Average Fare ($)", secondary_y=True)
        
        st.plotly_chart(fig, use_container_width=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Revenue by Payment Method")
        payment_revenue_query = """
        SELECT 
            dp.payment_description,
            COUNT(*) as trips,
            ROUND(SUM(ft.total_amount), 2) as revenue,
            ROUND(AVG(ft.total_amount), 2) as avg_fare,
            ROUND(SUM(ft.tip_amount), 2) as total_tips
        FROM fact_trips ft
        JOIN dim_payment dp ON ft.payment_key = dp.payment_key
        GROUP BY dp.payment_description
        ORDER BY revenue DESC
        """
        
        payment_revenue_df = run_query(payment_revenue_query)
        
        if not payment_revenue_df.empty:
            fig = px.bar(payment_revenue_df, x='payment_description', y='revenue',
                        title="Revenue by Payment Method",
                        color='avg_fare',
                        color_continuous_scale='viridis')
            fig.update_xaxes(title_text="Payment Method")
            fig.update_yaxes(title_text="Revenue ($)")
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Tip Analysis")
        tip_query = """
        SELECT 
            dp.payment_description,
            ROUND(AVG(ft.tip_amount), 2) as avg_tip,
            ROUND(AVG(ft.tip_amount / NULLIF(ft.fare_amount, 0) * 100), 2) as avg_tip_percentage,
            COUNT(CASE WHEN ft.tip_amount > 0 THEN 1 END) as trips_with_tips,
            ROUND(COUNT(CASE WHEN ft.tip_amount > 0 THEN 1 END) * 100.0 / COUNT(*), 1) as tip_frequency_percent
        FROM fact_trips ft
        JOIN dim_payment dp ON ft.payment_key = dp.payment_key
        GROUP BY dp.payment_description
        ORDER BY avg_tip_percentage DESC
        """
        
        tip_df = run_query(tip_query)
        
        if not tip_df.empty:
            fig = px.bar(tip_df, x='payment_description', y='avg_tip_percentage',
                        title="Average Tip Percentage by Payment Method",
                        color='tip_frequency_percent',
                        color_continuous_scale='blues')
            fig.update_xaxes(title_text="Payment Method")
            fig.update_yaxes(title_text="Average Tip Percentage (%)")
            st.plotly_chart(fig, use_container_width=True)

def trip_patterns():
    st.header("Trip Patterns Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Trip Distance Distribution")
        distance_query = """
        SELECT 
            CASE 
                WHEN ft.trip_distance <= 1 THEN '0-1 miles'
                WHEN ft.trip_distance <= 3 THEN '1-3 miles'
                WHEN ft.trip_distance <= 5 THEN '3-5 miles'
                WHEN ft.trip_distance <= 10 THEN '5-10 miles'
                ELSE '10+ miles'
            END as distance_bucket,
            COUNT(*) as trip_count,
            ROUND(AVG(ft.fare_amount), 2) as avg_fare
        FROM fact_trips ft
        WHERE ft.trip_distance > 0
        GROUP BY distance_bucket
        ORDER BY AVG(ft.trip_distance)
        """
        
        distance_df = run_query(distance_query)
        
        if not distance_df.empty:
            fig = px.bar(distance_df, x='distance_bucket', y='trip_count',
                        title="Trip Count by Distance",
                        color='avg_fare',
                        color_continuous_scale='plasma')
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Trip Duration Distribution")
        duration_query = """
        SELECT 
            CASE 
                WHEN ft.trip_duration_minutes <= 5 THEN '0-5 min'
                WHEN ft.trip_duration_minutes <= 15 THEN '5-15 min'
                WHEN ft.trip_duration_minutes <= 30 THEN '15-30 min'
                WHEN ft.trip_duration_minutes <= 60 THEN '30-60 min'
                ELSE '60+ min'
            END as duration_bucket,
            COUNT(*) as trip_count,
            ROUND(AVG(ft.fare_amount), 2) as avg_fare
        FROM fact_trips ft
        WHERE ft.trip_duration_minutes > 0
        GROUP BY duration_bucket
        ORDER BY AVG(ft.trip_duration_minutes)
        """
        
        duration_df = run_query(duration_query)
        
        if not duration_df.empty:
            fig = px.bar(duration_df, x='duration_bucket', y='trip_count',
                        title="Trip Count by Duration",
                        color='avg_fare',
                        color_continuous_scale='cividis')
            st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("Peak Hours Analysis")
    peak_hours_query = """
    SELECT 
        dt.hour_of_day,
        COUNT(*) as trip_count,
        ROUND(AVG(ft.fare_amount), 2) as avg_fare,
        ROUND(AVG(ft.trip_distance), 2) as avg_distance,
        ROUND(AVG(ft.trip_duration_minutes), 1) as avg_duration
    FROM fact_trips ft
    JOIN dim_time dt ON ft.pickup_time_key = dt.time_key
    GROUP BY dt.hour_of_day
    ORDER BY dt.hour_of_day
    """
    
    peak_df = run_query(peak_hours_query)
    
    if not peak_df.empty:
        fig = px.line(peak_df, x='hour_of_day', y='trip_count',
                     title="Trip Count by Hour of Day",
                     markers=True)
        fig.update_xaxes(title_text="Hour of Day")
        fig.update_yaxes(title_text="Trip Count")
        st.plotly_chart(fig, use_container_width=True)

def location_analytics():
    st.header("Location Analytics")
    
    # Top pickup and dropoff locations
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Top Pickup Locations")
        pickup_query = """
        SELECT 
            dl.zone,
            dl.city,
            COUNT(*) as pickup_count,
            ROUND(AVG(ft.fare_amount), 2) as avg_fare
        FROM fact_trips ft
        JOIN dim_location dl ON ft.pickup_location_key = dl.location_key
        GROUP BY dl.zone, dl.city
        ORDER BY pickup_count DESC
        LIMIT 15
        """
        
        pickup_df = run_query(pickup_query)
        
        if not pickup_df.empty:
            fig = px.bar(pickup_df, x='pickup_count', y='zone',
                        title="Top 15 Pickup Zones",
                        orientation='h',
                        color='avg_fare',
                        color_continuous_scale='viridis')
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Top Dropoff Locations")
        dropoff_query = """
        SELECT 
            dl.zone,
            dl.city,
            COUNT(*) as dropoff_count,
            ROUND(AVG(ft.fare_amount), 2) as avg_fare
        FROM fact_trips ft
        JOIN dim_location dl ON ft.dropoff_location_key = dl.location_key
        GROUP BY dl.zone, dl.city
        ORDER BY dropoff_count DESC
        LIMIT 15
        """
        
        dropoff_df = run_query(dropoff_query)
        
        if not dropoff_df.empty:
            fig = px.bar(dropoff_df, x='dropoff_count', y='zone',
                        title="Top 15 Dropoff Zones",
                        orientation='h',
                        color='avg_fare',
                        color_continuous_scale='plasma')
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)

    st.subheader("Most Popular Routes")
    routes_query = """
    SELECT 
        pickup.zone as pickup_zone,
        dropoff.zone as dropoff_zone,
        COUNT(*) as trip_count,
        ROUND(AVG(ft.fare_amount), 2) as avg_fare,
        ROUND(AVG(ft.trip_distance), 2) as avg_distance
    FROM fact_trips ft
    JOIN dim_location pickup ON ft.pickup_location_key = pickup.location_key
    JOIN dim_location dropoff ON ft.dropoff_location_key = dropoff.location_key
    GROUP BY pickup.zone, dropoff.zone
    HAVING trip_count >= 100
    ORDER BY trip_count DESC
    LIMIT 20
    """
    
    routes_df = run_query(routes_query)
    
    if not routes_df.empty:
        routes_df['route'] = routes_df['pickup_zone'] + ' → ' + routes_df['dropoff_zone']
        
        fig = px.bar(routes_df, x='trip_count', y='route',
                    title="Top 20 Popular Routes",
                    orientation='h',
                    color='avg_fare',
                    color_continuous_scale='turbo')
        fig.update_layout(height=600)
        st.plotly_chart(fig, use_container_width=True)

def operational_metrics():
    st.header("Operational Metrics")
 
    st.subheader("Average Speed Analysis by Hour")
    speed_query = """
    SELECT 
        dt.hour_of_day as time_bucket,
        COUNT(*) as trips,
        ROUND(AVG(ft.trip_distance), 2) as avg_distance,
        ROUND(AVG(ft.trip_duration_minutes), 1) as avg_duration,
        ROUND(AVG(ft.trip_distance / NULLIF(ft.trip_duration_minutes, 0) * 60), 1) as avg_speed_kmh
    FROM fact_trips ft
    JOIN dim_time dt ON ft.pickup_time_key = dt.time_key
    WHERE ft.trip_duration_minutes > 0 AND ft.trip_distance > 0
    GROUP BY dt.hour_of_day
    ORDER BY dt.hour_of_day
    """
    
    speed_df = run_query(speed_query)
    
    if not speed_df.empty:
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        fig.add_trace(
            go.Bar(x=speed_df['time_bucket'], y=speed_df['trips'], 
                   name="Trip Count", marker_color='lightcoral'),
            secondary_y=False,
        )
        
        fig.add_trace(
            go.Scatter(x=speed_df['time_bucket'], y=speed_df['avg_speed_kmh'], 
                      mode='lines+markers', name="Average Speed (km/h)", 
                      line=dict(color='blue', width=3)),
            secondary_y=True,
        )
        
        fig.update_layout(title="Trip Count and Average Speed by Hour", height=400)
        fig.update_xaxes(title_text="Hour of Day")
        fig.update_yaxes(title_text="Number of Trips", secondary_y=False)
        fig.update_yaxes(title_text="Average Speed (km/h)", secondary_y=True)
        
        st.plotly_chart(fig, use_container_width=True)
 
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Rate Code Distribution")
        rate_query = """
        SELECT 
            dr.rate_description,
            COUNT(*) as trip_count,
            ROUND(AVG(ft.fare_amount), 2) as avg_fare,
            ROUND(SUM(ft.total_amount), 2) as total_revenue
        FROM fact_trips ft
        JOIN dim_rate dr ON ft.rate_key = dr.rate_key
        GROUP BY dr.rate_description
        ORDER BY trip_count DESC
        """
        
        rate_df = run_query(rate_query)
        
        if not rate_df.empty:
            fig = px.pie(rate_df, values='trip_count', names='rate_description',
                        title="Trip Distribution by Rate Code")
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Extra Charges Analysis")
        extra_query = """
        SELECT 
            dt.hour_of_day,
            COUNT(*) as trips,
            ROUND(AVG(ft.extra), 2) as avg_extra,
            ROUND(AVG(ft.congestion_surcharge), 2) as avg_congestion_surcharge,
            COUNT(CASE WHEN ft.extra > 0 THEN 1 END) as trips_with_extra
        FROM fact_trips ft
        JOIN dim_time dt ON ft.pickup_time_key = dt.time_key
        GROUP BY dt.hour_of_day
        ORDER BY avg_extra DESC
        LIMIT 10
        """
        
        extra_df = run_query(extra_query)
        
        if not extra_df.empty:
            fig = px.bar(extra_df, x='hour_of_day', y='avg_extra',
                        title="Average Extra Charges by Hour",
                        color='avg_congestion_surcharge',
                        color_continuous_scale='reds')
            st.plotly_chart(fig, use_container_width=True)

def customer_insights():
    st.header("Customer Insights")
 
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Passenger Count Distribution")
        passenger_query = """
        SELECT 
            ft.passenger_count,
            COUNT(*) as trip_count,
            ROUND(AVG(ft.fare_amount), 2) as avg_fare,
            ROUND(AVG(ft.tip_amount), 2) as avg_tip
        FROM fact_trips ft
        WHERE ft.passenger_count > 0 AND ft.passenger_count <= 6
        GROUP BY ft.passenger_count
        ORDER BY ft.passenger_count
        """
        
        passenger_df = run_query(passenger_query)
        
        if not passenger_df.empty:
            fig = px.bar(passenger_df, x='passenger_count', y='trip_count',
                        title="Trip Count by Passenger Count",
                        color='avg_fare',
                        color_continuous_scale='viridis')
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Tip Frequency by Payment Method")
        tip_freq_query = """
        SELECT 
            dp.payment_description,
            COUNT(*) as trips,
            COUNT(CASE WHEN ft.tip_amount > 0 THEN 1 END) as trips_with_tips,
            ROUND(COUNT(CASE WHEN ft.tip_amount > 0 THEN 1 END) * 100.0 / COUNT(*), 1) as tip_frequency_percent
        FROM fact_trips ft
        JOIN dim_payment dp ON ft.payment_key = dp.payment_key
        GROUP BY dp.payment_description
        ORDER BY tip_frequency_percent DESC
        """
        
        tip_freq_df = run_query(tip_freq_query)
        
        if not tip_freq_df.empty:
            fig = px.bar(tip_freq_df, x='payment_description', y='tip_frequency_percent',
                        title="Tip Frequency by Payment Method (%)",
                        color='trips_with_tips',
                        color_continuous_scale='blues')
            st.plotly_chart(fig, use_container_width=True)
    
    # zone performance analysis
    st.subheader("Top Performing Zones")
    zone_performance_query = """
    SELECT 
        dl.zone,
        dl.city,
        COUNT(*) as total_trips,
        ROUND(AVG(ft.total_amount), 2) as avg_fare,
        ROUND(AVG(ft.trip_distance), 2) as avg_distance,
        ROUND(SUM(ft.total_amount), 2) as total_revenue
    FROM fact_trips ft
    JOIN dim_location dl ON ft.pickup_location_key = dl.location_key
    GROUP BY dl.zone, dl.city
    HAVING total_trips >= 100
    ORDER BY total_revenue DESC
    LIMIT 20
    """
    
    zone_perf_df = run_query(zone_performance_query)
    
    if not zone_perf_df.empty:
        fig = px.scatter(zone_perf_df, x='total_trips', y='avg_fare',
                        size='total_revenue', hover_name='zone',
                        title="Zone Performance: Trip Volume vs Average Fare",
                        color='avg_distance',
                        color_continuous_scale='viridis',
                        size_max=60)
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)
        st.subheader("Top Performing Zones Data")
        st.dataframe(zone_perf_df.head(10), use_container_width=True)

def advanced_analytics():
    st.header("Predictive Trip Analytics")
    
    # trip duration vs distance scatter plot
    st.subheader("Trip Duration vs Distance Analysis")
    
    # add filters for better interactivity
    col1, col2, col3 = st.columns(3)
    
    with col1:
        max_distance = st.slider("Maximum Distance (miles)", 0, 50, 20)
    with col2:
        max_duration = st.slider("Maximum Duration (minutes)", 0, 120, 60)
    with col3:
        sample_size = st.selectbox("Sample Size", [1000, 5000, 10000, 20000], index=1)
    
    scatter_query = f"""
    SELECT 
        ft.trip_distance,
        ft.trip_duration_minutes,
        ft.fare_amount,
        ft.total_amount,
        ft.passenger_count,
        dp.payment_description,
        dt.hour_of_day,
        CASE 
            WHEN dt.hour_of_day BETWEEN 6 AND 11 THEN 'Morning'
            WHEN dt.hour_of_day BETWEEN 12 AND 17 THEN 'Afternoon'
            WHEN dt.hour_of_day BETWEEN 18 AND 23 THEN 'Evening'
            ELSE 'Night'
        END as time_period
    FROM fact_trips ft
    JOIN dim_payment dp ON ft.payment_key = dp.payment_key
    JOIN dim_time dt ON ft.pickup_time_key = dt.time_key
    WHERE ft.trip_distance > 0 
        AND ft.trip_duration_minutes > 0 
        AND ft.trip_distance <= {max_distance}
        AND ft.trip_duration_minutes <= {max_duration}
        AND ft.fare_amount > 0
    ORDER BY RAND()
    LIMIT {sample_size}
    """
    
    scatter_df = run_query(scatter_query)
    
    if not scatter_df.empty:
        fig = px.scatter(
            scatter_df, 
            x='trip_distance', 
            y='trip_duration_minutes',
            color='total_amount',
            size='passenger_count',
            hover_data=['fare_amount', 'payment_description', 'time_period'],
            title=f"Trip Duration vs Distance (Sample: {len(scatter_df):,} trips)",
            labels={
                'trip_distance': 'Trip Distance (miles)',
                'trip_duration_minutes': 'Trip Duration (minutes)',
                'total_amount': 'Total Fare ($)'
            },
            color_continuous_scale='viridis',
            size_max=15
        )
        
        fig.update_layout(height=600)
        st.plotly_chart(fig, use_container_width=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Efficiency Analysis by Time Period")
            efficiency_query = """
            SELECT 
                CASE 
                    WHEN dt.hour_of_day BETWEEN 6 AND 11 THEN 'Morning'
                    WHEN dt.hour_of_day BETWEEN 12 AND 17 THEN 'Afternoon'
                    WHEN dt.hour_of_day BETWEEN 18 AND 23 THEN 'Evening'
                    ELSE 'Night'
                END as time_period,
                COUNT(*) as trip_count,
                ROUND(AVG(ft.trip_distance / NULLIF(ft.trip_duration_minutes, 0) * 60), 2) as avg_speed_mph,
                ROUND(AVG(ft.fare_amount / NULLIF(ft.trip_distance, 0)), 2) as fare_per_mile,
                ROUND(AVG(ft.fare_amount / NULLIF(ft.trip_duration_minutes, 0)), 2) as fare_per_minute
            FROM fact_trips ft
            JOIN dim_time dt ON ft.pickup_time_key = dt.time_key
            WHERE ft.trip_distance > 0 AND ft.trip_duration_minutes > 0
            GROUP BY time_period
            ORDER BY avg_speed_mph DESC
            """
            
            efficiency_df = run_query(efficiency_query)
            
            if not efficiency_df.empty:
                fig_eff = px.bar(
                    efficiency_df, 
                    x='time_period', 
                    y='avg_speed_mph',
                    color='fare_per_mile',
                    title="Average Speed by Time Period",
                    color_continuous_scale='plasma'
                )
                st.plotly_chart(fig_eff, use_container_width=True)
        
        with col2:
            st.subheader("Distance vs Duration Correlation")
            
            # Calculate correlation
            correlation = scatter_df['trip_distance'].corr(scatter_df['trip_duration_minutes'])
            
            st.metric("Correlation Coefficient", f"{correlation:.3f}")
            
            # Create a trend line plot
            fig_trend = px.scatter(
                scatter_df.sample(n=min(2000, len(scatter_df))), 
                x='trip_distance', 
                y='trip_duration_minutes',
                trendline="ols",
                title="Distance vs Duration Trend",
                opacity=0.6
            )
            st.plotly_chart(fig_trend, use_container_width=True)
    
    # Fare Prediction Analysis
    st.subheader("Fare Analysis by Trip Characteristics")
    
    fare_analysis_query = """
    SELECT 
        CASE 
            WHEN ft.trip_distance <= 2 THEN 'Short (≤2 mi)'
            WHEN ft.trip_distance <= 5 THEN 'Medium (2-5 mi)'
            WHEN ft.trip_distance <= 10 THEN 'Long (5-10 mi)'
            ELSE 'Very Long (>10 mi)'
        END as distance_category,
        CASE 
            WHEN ft.trip_duration_minutes <= 10 THEN 'Quick (≤10 min)'
            WHEN ft.trip_duration_minutes <= 20 THEN 'Normal (10-20 min)'
            WHEN ft.trip_duration_minutes <= 40 THEN 'Slow (20-40 min)'
            ELSE 'Very Slow (>40 min)'
        END as duration_category,
        COUNT(*) as trip_count,
        ROUND(AVG(ft.total_amount), 2) as avg_fare,
        ROUND(stddevPop(ft.total_amount), 2) as fare_std,
        ROUND(MIN(ft.total_amount), 2) as min_fare,
        ROUND(MAX(ft.total_amount), 2) as max_fare
    FROM fact_trips ft
    WHERE ft.trip_distance > 0 AND ft.trip_duration_minutes > 0 AND ft.total_amount > 0
    GROUP BY distance_category, duration_category
    HAVING trip_count >= 50
    ORDER BY avg_fare DESC
    """
    
    fare_analysis_df = run_query(fare_analysis_query)
    
    if not fare_analysis_df.empty:
        pivot_df = fare_analysis_df.pivot(index='duration_category', columns='distance_category', values='avg_fare')
        
        fig_heatmap = px.imshow(
            pivot_df,
            title="Average Fare by Distance and Duration Categories",
            labels=dict(x="Distance Category", y="Duration Category", color="Average Fare ($)"),
            color_continuous_scale='RdYlBu_r'
        )
        
        st.plotly_chart(fig_heatmap, use_container_width=True)
        
        # Display the data table
        st.subheader("Detailed Fare Analysis")
        st.dataframe(fare_analysis_df, use_container_width=True)

if __name__ == "__main__":
    main()

