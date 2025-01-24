"""
Streamlit app for visualizing transformed data from PostgreSQL database.
"""

import os
import pandas as pd
import streamlit as st
import plotly.express as px
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Page config
st.set_page_config(
    page_title="Energy Data Analytics",
    page_icon="⚡",
    layout="wide"
)

# Load environment variables
load_dotenv()

def get_db_connection():
    """Create database connection using environment variables."""
    connection_string = (
        f"postgresql://{os.getenv('PGUSER')}:{os.getenv('PGPASSWORD')}"
        f"@{os.getenv('PGHOST')}/{os.getenv('PGDATABASE')}?sslmode=require"
    )
    return create_engine(connection_string)

def run_query(query: str) -> pd.DataFrame:
    """Execute a SQL query and return results as a DataFrame."""
    engine = get_db_connection()
    return pd.read_sql_query(query, engine)

def main():
    """Main Streamlit app."""
    st.title("⚡ Energy Data Analytics Dashboard")
    
    # Get schema name
    schema = os.getenv('ANALYTICS_SCHEMA', 'dev')
    
    # Sidebar
    st.sidebar.title("Navigation")
    page = st.sidebar.radio(
        "Select a page:",
        ["Active Agreements", "Consumption Analysis", "Product Performance"]
    )
    
    if page == "Active Agreements":
        st.header("Active Agreements Analysis")
        
        # Get active agreements data
        query = f"""
        SELECT 
            display_name,
            is_variable,
            COUNT(*) as agreement_count
        FROM {schema}.active_agreements
        GROUP BY display_name, is_variable
        ORDER BY agreement_count DESC
        """
        df_agreements = run_query(query)
        
        # Display data
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Agreements by Product")
            st.dataframe(df_agreements, use_container_width=True)
        
        with col2:
            st.subheader("Product Distribution")
            fig = px.pie(
                df_agreements,
                values='agreement_count',
                names='display_name',
                title='Distribution of Agreements by Product'
            )
            st.plotly_chart(fig, use_container_width=True)
            
        # Variable vs Fixed Rate
        st.subheader("Variable vs Fixed Rate Products")
        fig = px.bar(
            df_agreements,
            x='display_name',
            y='agreement_count',
            color='is_variable',
            title='Agreement Count by Product Type'
        )
        st.plotly_chart(fig, use_container_width=True)
        
    elif page == "Consumption Analysis":
        st.header("Consumption Analysis")
        
        # Get consumption data
        query = f"""
        SELECT 
            datetime,
            meterpoint_count,
            total_consumption_kwh
        FROM {schema}.halfhourly_consumption
        ORDER BY datetime
        """
        df_consumption = run_query(query)
        
        # Time series plot
        st.subheader("Consumption Over Time")
        fig = px.line(
            df_consumption,
            x='datetime',
            y='total_consumption_kwh',
            title='Total Energy Consumption Over Time'
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Hourly patterns
        query = f"""
        SELECT 
            EXTRACT(HOUR FROM datetime) as hour_of_day,
            AVG(total_consumption_kwh) as avg_consumption,
            AVG(meterpoint_count) as avg_active_meters
        FROM {schema}.halfhourly_consumption
        GROUP BY EXTRACT(HOUR FROM datetime)
        ORDER BY hour_of_day
        """
        df_hourly = run_query(query)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Average Hourly Consumption")
            fig = px.bar(
                df_hourly,
                x='hour_of_day',
                y='avg_consumption',
                title='Average Consumption by Hour'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("Active Meters by Hour")
            fig = px.line(
                df_hourly,
                x='hour_of_day',
                y='avg_active_meters',
                title='Average Active Meters by Hour'
            )
            st.plotly_chart(fig, use_container_width=True)
            
    else:  # Product Performance
        st.header("Product Performance Analysis")
        
        # Get product performance data
        query = f"""
        SELECT 
            product_display_name,
            date,
            meterpoint_count,
            total_consumption_kwh
        FROM {schema}.daily_product_consumption
        ORDER BY date, product_display_name
        """
        df_products = run_query(query)
        
        # Product summary
        st.subheader("Product Performance Summary")
        product_summary = df_products.groupby('product_display_name').agg({
            'total_consumption_kwh': ['sum', 'mean'],
            'meterpoint_count': 'mean'
        }).round(2)
        st.dataframe(product_summary, use_container_width=True)
        
        # Product comparison chart
        st.subheader("Product Consumption Comparison")
        fig = px.box(
            df_products,
            x='product_display_name',
            y='total_consumption_kwh',
            title='Distribution of Daily Consumption by Product'
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Time series by product
        st.subheader("Consumption Trends by Product")
        fig = px.line(
            df_products,
            x='date',
            y='total_consumption_kwh',
            color='product_display_name',
            title='Daily Consumption by Product'
        )
        st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    main()
