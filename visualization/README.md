# RideTrack360 Streamlit Dashboard

## Overview
Real-time interactive dashboard for monitoring NYC taxi fleet operations with live GPS tracking, weather integration, and business analytics.

## Features

### 🗺️ **Live Vehicle Tracking**
- **Interactive Map**: Real-time vehicle positions on NYC map
- **Speed Visualization**: Color-coded vehicles by speed (Red: >40km/h, Yellow: 20-40km/h, Green: <20km/h)
- **Vehicle Details**: Hover tooltips showing vehicle ID, speed, zone, and weather
- **Live Updates**: Auto-refresh every 30 seconds

### 📊 **Key Performance Indicators (KPIs)**
- **Active Trips**: Current number of ongoing trips
- **Average ETA**: Real-time trip duration estimates
- **Average Speed**: Fleet-wide speed monitoring
- **Revenue Tracking**: Live revenue calculations
- **Active Vehicles**: Number of vehicles currently operating

### 🌤️ **Weather Intelligence**
- **Weather Impact Analysis**: Trip correlation with weather conditions
- **Live Weather Data**: Current conditions affecting operations
- **Performance Metrics**: Speed and trip patterns by weather

### 📈 **Business Analytics**
- **Hourly Trip Distribution**: Peak hours and demand patterns
- **Payment Method Analysis**: Credit card vs cash usage trends
- **Zone Activity Heatmap**: Most active pickup/dropoff locations
- **Vehicle Performance Rankings**: Top performing vehicles

### ⚡ **Real-Time Features**
- **Auto-Refresh**: Dashboard updates every 30 seconds
- **Live Data Streams**: Direct connection to ClickHouse database
- **System Health Monitoring**: Data quality and connection status
- **Interactive Controls**: Manual refresh and configuration options

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Run Dashboard
```bash
python run_dashboard.py
```

### 3. Access Dashboard
Open your browser to: `http://localhost:8501`

## Dashboard Sections

### **Top Metrics Row**
5 key KPI cards with real-time values and trend indicators

### **Live Map Section**
- Interactive NYC map with vehicle positions
- Speed-based color coding
- Weather overlay information
- Zone boundaries and labels

### **Analytics Charts**
- **Hourly Trends**: Line chart showing trip distribution throughout the day
- **Payment Distribution**: Pie chart of payment method usage
- **Weather Impact**: Bar chart correlating weather with trip volume

### **Performance Tables**
- **Vehicle Rankings**: Top 10 vehicles by average speed
- **Zone Activity**: Most active zones by vehicle count
- **System Health**: Real-time status indicators

## Technical Features

### **Data Sources**
- **ClickHouse**: Primary analytics database
- **Kafka Streams**: Real-time GPS data
- **OpenWeather API**: Live weather conditions

### **Visualization Technologies**
- **Streamlit**: Web application framework
- **Plotly**: Interactive charts and graphs
- **PyDeck**: 3D map visualizations
- **Pandas**: Data processing and analysis

### **Performance Optimizations**
- **Cached Connections**: Persistent database connections
- **Efficient Queries**: Optimized SQL for real-time performance
- **Lazy Loading**: Data loaded on-demand
- **Auto-refresh**: Smart update intervals

## Configuration

### **Dashboard Settings**
- **Auto-refresh interval**: 30 seconds (configurable)
- **Map zoom level**: NYC metropolitan area
- **Data refresh rate**: Real-time
- **Chart update frequency**: Every refresh cycle

### **Database Connection**
The dashboard connects directly to your ClickHouse instance using the credentials from your data warehouse setup.

## Monitoring & Health

### **System Health Indicators**
- 🟢 **Data Freshness**: Real-time status
- 🟢 **Database Connection**: Connection health
- 🟢 **Active Streams**: Number of live data feeds
- 🟢 **Data Quality Score**: Overall data health percentage

### **Performance Metrics**
- Response time monitoring
- Query execution tracking
- Data update frequency
- User interaction analytics

## Usage Examples

### **Fleet Manager Dashboard**
Monitor vehicle utilization, identify high-performance vehicles, and track operational efficiency.

### **Operations Center**
Real-time oversight of active trips, weather impacts, and zone-based demand patterns.

### **Business Intelligence**
Revenue tracking, payment method analysis, and demand forecasting for strategic decisions.

### **Performance Monitoring**
Vehicle performance rankings, zone activity analysis, and system health monitoring.

## Future Enhancements

- **Predictive Analytics**: Demand forecasting models
- **Alert System**: Automated notifications for anomalies
- **Mobile Optimization**: Responsive design for mobile devices
- **Advanced Filters**: Custom date ranges and vehicle selections
- **Export Features**: Data download and report generation
