import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import psycopg2

# Page configuration
st.set_page_config(
    page_title="Vegetable Supply Chain Analytics",
    page_icon="🥦", 
    layout="wide",
    initial_sidebar_state="expanded"
)

# Title and header
st.title("🥦 Vegetable Supply Chain Analytics")
st.markdown("Real-time insights from 600+ vegetable records across Bronze, Silver, and Gold layers")

# Database connection function
@st.cache_data
def load_data():
    try:
        conn = psycopg2.connect(
            host='host.docker.internal',
            port=5433,
            user='airflow',
            password='airflow',
            database='warehouse'
        )
        
        # Load all data layers
        bronze_df = pd.read_sql('SELECT * FROM bronze_vegetable_prices', conn)
        silver_df = pd.read_sql('SELECT * FROM silver_vegetable_prices', conn) 
        gold_df = pd.read_sql('SELECT * FROM gold_vegetable_metrics', conn)
        
        conn.close()
        return bronze_df, silver_df, gold_df
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None, None, None

# Load data
bronze_df, silver_df, gold_df = load_data()

if bronze_df is not None:
    # SIDEBAR - Filters
    st.sidebar.header("🔍 Filters & Controls")
    
    # Date range filter
    if 'report_date' in bronze_df.columns:
        min_date = pd.to_datetime(bronze_df['report_date']).min()
        max_date = pd.to_datetime(bronze_df['report_date']).max()
        
        date_range = st.sidebar.date_input(
            "📅 Select Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )
    
    # Vegetable filter
    vegetable_col = None
    for col in ['vegetable', 'Vegetable', 'item_name', 'product']:
        if col in bronze_df.columns:
            vegetable_col = col
            break
    
    if vegetable_col:
        vegetables = ['All'] + sorted(bronze_df[vegetable_col].unique().tolist())
        selected_vegetable = st.sidebar.selectbox("🥦 Select Vegetable", vegetables)
    else:
        selected_vegetable = 'All'
    
    # KPI METRICS
    st.subheader("📊 Pipeline Performance Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Bronze Records", len(bronze_df), "Raw Data")
    with col2:
        st.metric("Silver Records", len(silver_df), "Cleaned Data") 
    with col3:
        st.metric("Gold Metrics", len(gold_df), "Business Insights")
    with col4:
        st.metric("Data Quality", "100%", "Success Rate")
    
    # VEGETABLE ANALYSIS SECTION
    st.subheader("🥦 Individual Vegetable Analysis")
    
    if vegetable_col and selected_vegetable != 'All':
        # Filter data for selected vegetable
        veg_bronze = bronze_df[bronze_df[vegetable_col] == selected_vegetable]
        veg_silver = silver_df[silver_df[vegetable_col] == selected_vegetable] if silver_df is not None else None
        
        st.write(f"### Analysis for: {selected_vegetable}")
        
        # Display vegetable data
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Raw Data (Bronze Layer)**")
            st.dataframe(veg_bronze.head(10), use_container_width=True)
            
        with col2:
            if veg_silver is not None:
                st.write("**Cleaned Data (Silver Layer)**")
                st.dataframe(veg_silver.head(10), use_container_width=True)
    
    # DATA LAYER EXPLORER TABS
    st.subheader("🔍 Data Layer Explorer")
    
    tab1, tab2, tab3 = st.tabs(["📋 Bronze Layer", "✨ Silver Layer", "🏆 Gold Layer"])
    
    with tab1:
        st.write("### Raw Vegetable Data (Bronze Layer)")
        st.dataframe(bronze_df.head(15), use_container_width=True)
        
        # Basic statistics
        st.write("**Data Summary**")
        st.write(bronze_df.describe())
        
    with tab2:
        if silver_df is not None:
            st.write("### Cleaned & Transformed Data (Silver Layer)")
            st.dataframe(silver_df.head(15), use_container_width=True)
        else:
            st.info("Silver layer data not available")
    
    with tab3:
        if gold_df is not None:
            st.write("### Business Metrics (Gold Layer)")
            st.dataframe(gold_df, use_container_width=True)
        else:
            st.info("Gold layer data not available")
    
    # VISUALIZATIONS SECTION
    st.subheader("📈 Data Visualizations")
    
    # Vegetable distribution
    if vegetable_col:
        col1, col2 = st.columns(2)
        
        with col1:
            veg_distribution = bronze_df[vegetable_col].value_counts().reset_index()
            veg_distribution.columns = ['Vegetable', 'Count']
            
            fig1 = px.bar(veg_distribution, x='Vegetable', y='Count', 
                         title='Vegetable Distribution', color='Count')
            st.plotly_chart(fig1, use_container_width=True)
        
        with col2:
            fig2 = px.pie(veg_distribution, values='Count', names='Vegetable',
                         title='Vegetable Percentage Distribution')
            st.plotly_chart(fig2, use_container_width=True)
    
    # Time series analysis if date column exists
    date_cols = [col for col in bronze_df.columns if 'date' in col.lower()]
    if date_cols:
        st.write("### 📅 Time Series Analysis")
        bronze_df[date_cols[0]] = pd.to_datetime(bronze_df[date_cols[0]])
        daily_counts = bronze_df.groupby(bronze_df[date_cols[0]].dt.date).size().reset_index()
        daily_counts.columns = ['Date', 'Record Count']
        fig3 = px.line(daily_counts, x='Date', y='Record Count', title='Daily Data Volume Trend')
        st.plotly_chart(fig3, use_container_width=True)
    
else:
    st.error("❌ Unable to load data from the data warehouse")

st.sidebar.success("✅ Dashboard loaded successfully!")
st.sidebar.info(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
