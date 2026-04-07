"""
FastFeast Analytics Dashboard
==============================
Streamlit dashboard for visualizing KPIs and ticket analytics.

Run with:
    streamlit run analytics/dashboard.py

Or:
    python -m streamlit run analytics/dashboard.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Initialize DB pool before importing analytics (which uses get_conn)
from config.settings import get_settings
from warehouse.connection import init_pool

settings = get_settings()
init_pool(settings)

from analytics import AnalyticsClient, get_summary_metrics, ensure_analytics_schema

# Page config
st.set_page_config(
    page_title="FastFeast Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
    }
    .kpi-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .kpi-value {
        font-size: 2rem;
        font-weight: bold;
        color: #1f77b4;
    }
    .kpi-label {
        font-size: 0.9rem;
        color: #666;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_data(ttl=300)
def load_kpi_summary():
    """Cache KPI data for 5 minutes."""
    client = AnalyticsClient()
    return {
        'kpi': client.get_kpi_summary(),
        'reopen': client.get_reopen_rate(),
        'revenue': client.get_revenue_impact(),
        'by_location': client.get_tickets_by_location(top_n=15),
        'by_restaurant': client.get_tickets_by_restaurant(top_n=15),
        'by_driver': client.get_tickets_by_driver(top_n=15),
    }


def render_header():
    """Render dashboard header."""
    st.markdown('<p class="main-header">📊 FastFeast Analytics Dashboard</p>', 
                unsafe_allow_html=True)
    st.markdown("Operational ticket analytics: SLA compliance, refunds, and revenue impact.")
    st.divider()


def _safe_float(value, default=0.0):
    """Safely convert value to float, handling None."""
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def _safe_int(value, default=0):
    """Safely convert value to int, handling None."""
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def render_kpi_cards(data):
    """Render KPI metric cards."""
    kpi = data['kpi']
    reopen = data['reopen']
    revenue = data['revenue']
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Total Tickets",
            value=f"{_safe_int(kpi.get('total_tickets')):,}",
            delta=None
        )
    
    with col2:
        sla_rate = _safe_float(kpi.get('sla_resolution_breach_rate_pct'))
        st.metric(
            label="SLA Breach Rate",
            value=f"{sla_rate:.1f}%",
            delta=f"{'↑' if sla_rate > 10 else '↓'} {'High' if sla_rate > 10 else 'Good'}",
            delta_color="inverse"
        )
    
    with col3:
        avg_res = _safe_float(kpi.get('avg_resolution_minutes'))
        st.metric(
            label="Avg Resolution Time",
            value=f"{avg_res:.0f} min",
            delta=None
        )
    
    with col4:
        reopen_rate = _safe_float(reopen.get('reopen_rate_pct'))
        st.metric(
            label="Reopen Rate",
            value=f"{reopen_rate:.1f}%",
            delta=None
        )
    
    # Second row of KPIs
    col5, col6, col7, col8 = st.columns(4)
    
    with col5:
        total_refund = _safe_float(kpi.get('total_refund_amount'))
        st.metric(
            label="Total Refunds",
            value=f"${total_refund:,.2f}",
            delta=None
        )
    
    with col6:
        net_rev = _safe_float(revenue.get('net_revenue'))
        st.metric(
            label="Net Revenue",
            value=f"${net_rev:,.2f}",
            delta=None
        )
    
    with col7:
        avg_refund = _safe_float(kpi.get('avg_refund_amount'))
        st.metric(
            label="Avg Refund",
            value=f"${avg_refund:.2f}",
            delta=None
        )
    
    with col8:
        refund_impact = _safe_float(revenue.get('refund_impact_rate_pct'))
        st.metric(
            label="Refund Impact",
            value=f"{refund_impact:.2f}%",
            delta=None
        )

    # Third row — first-response SLA (PDF spec: First Response Time + breach rate)
    col9, col10, col11, col12 = st.columns(4)
    with col9:
        fr_min = _safe_float(kpi.get('avg_first_response_minutes'))
        st.metric(
            label="Avg First Response",
            value=f"{fr_min:.1f} min",
            delta=None,
        )
    with col10:
        fr_breach = _safe_float(kpi.get('sla_first_response_breach_rate_pct'))
        st.metric(
            label="First Response SLA Breach",
            value=f"{fr_breach:.1f}%",
            delta=None,
        )
    with col11:
        res_breach = _safe_float(kpi.get('sla_resolution_breach_rate_pct'))
        st.metric(
            label="Resolution SLA Breach",
            value=f"{res_breach:.1f}%",
            delta=None,
        )
    with col12:
        st.metric(
            label="Tickets w/ Refund",
            value=f"{_safe_int(kpi.get('tickets_with_refund')):,}",
            delta=None,
        )
    
    st.divider()


def render_trends(data):
    """Removed by design (out of scope for required analytics)."""
    return


def render_breakdowns(data):
    """Render dimensional breakdowns."""
    st.subheader("📊 Breakdown by Dimensions")
    
    tab1, tab2, tab3 = st.tabs(["By Location", "By Restaurant", "By Driver"])
    
    with tab1:
        by_location = data['by_location']
        if not by_location.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.bar(
                    by_location.head(10),
                    x='city_name',
                    y='total_tickets',
                    color='region_name',
                    title='Top 10 Cities by Ticket Volume',
                    labels={'city_name': 'City', 'total_tickets': 'Tickets'}
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, width='stretch')
            
            with col2:
                fig = px.bar(
                    by_location.head(10),
                    x='city_name',
                    y='sla_breach_rate_pct',
                    color='total_refund_amount',
                    title='SLA Breach Rate by City',
                    labels={'city_name': 'City', 'sla_breach_rate_pct': 'Breach %'}
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, width='stretch')
    
    with tab2:
        by_restaurant = data['by_restaurant']
        if not by_restaurant.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.bar(
                    by_restaurant.head(10),
                    x='restaurant_name',
                    y='total_tickets',
                    color='category_name',
                    title='Top 10 Restaurants by Ticket Volume',
                    labels={'restaurant_name': 'Restaurant', 'total_tickets': 'Tickets'}
                )
                fig.update_layout(height=400, xaxis_tickangle=-45)
                st.plotly_chart(fig, width='stretch')
            
            with col2:
                # Ensure data is proper pandas DataFrame for plotly
                by_rest_plot = by_restaurant.copy()
                by_rest_plot['total_refund_amount'] = by_rest_plot['total_refund_amount'].astype(float)
                fig = px.scatter(
                    by_rest_plot,
                    x='total_tickets',
                    y='avg_resolution_minutes',
                    size=by_rest_plot['total_refund_amount'].values,
                    color='category_name',
                    hover_data=['restaurant_name'],
                    title='Resolution Time vs Ticket Volume',
                    labels={'total_tickets': 'Total Tickets', 
                           'avg_resolution_minutes': 'Avg Resolution (min)'}
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, width='stretch')
    
    with tab3:
        by_driver = data['by_driver']
        if not by_driver.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.bar(
                    by_driver.head(10),
                    x='driver_name',
                    y='total_tickets',
                    color='vehicle_type',
                    title='Top 10 Drivers by Ticket Volume',
                    labels={'driver_name': 'Driver', 'total_tickets': 'Tickets'}
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, width='stretch')
            
            with col2:
                fig = px.box(
                    by_driver,
                    x='vehicle_type',
                    y='avg_resolution_minutes',
                    title='Resolution Time by Vehicle Type',
                    labels={'vehicle_type': 'Vehicle Type', 
                           'avg_resolution_minutes': 'Resolution (min)'}
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, width='stretch')
    
    st.divider()


def render_sla_analysis(data):
    """Removed by design (out of scope for required analytics)."""
    return


def render_data_tables(data):
    """Render raw data tables."""
    st.subheader("📋 Detailed Data")
    
    with st.expander("View tables"):
        tab1, tab2, tab3 = st.tabs(["By Location", "By Restaurant", "By Driver"])

        with tab1:
            st.dataframe(data['by_location'], width='stretch')

        with tab2:
            st.dataframe(data['by_restaurant'], width='stretch')

        with tab3:
            st.dataframe(data['by_driver'], width='stretch')


def main():
    """Main dashboard entry point."""
    render_header()
    
    # Sidebar controls
    with st.sidebar:
        st.header("⚙️ Controls")
        
        if st.button("🔄 Refresh Data"):
            st.cache_data.clear()
            st.rerun()
        
        st.divider()
        st.markdown("### About")
        st.markdown("""
        This dashboard provides real-time analytics for the FastFeast 
        support ticket system.
        
        **Key Metrics:**
        - Ticket volume and trends
        - SLA compliance rates
        - Resolution times
        - Refund impact
        
        Data refreshes every 5 minutes automatically.
        """)
    
    # Load data
    try:
        data = load_kpi_summary()
        
        # Render sections
        render_kpi_cards(data)
        render_trends(data)
        render_breakdowns(data)
        render_data_tables(data)
        
    except Exception as e:
        st.error(f"Error loading dashboard data: {str(e)}")
        st.info("Make sure the analytics views are created. Run: `python -c \"from analytics import ensure_analytics_schema; ensure_analytics_schema()\"`")


if __name__ == "__main__":
    main()
