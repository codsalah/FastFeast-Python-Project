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
        'order_summary': client.get_order_summary(),
        'order_trends': client.get_order_trends_daily(),
        'orders_by_region': client.get_orders_by_region(),
        'orders_by_restaurant': client.get_orders_by_restaurant(),
    }


def render_header():
    """Render dashboard header."""
    st.markdown('<p class="main-header">📊 FastFeast Business Analytics Dashboard</p>', 
                unsafe_allow_html=True)
    st.markdown("Comprehensive business analytics: orders, tickets, SLA compliance, refunds, and revenue impact.")
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
    order_summary = data['order_summary']
    
    # Order Metrics Row
    st.subheader("🛒 Order Metrics")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_orders = _safe_int(order_summary.get('total_orders'))
        st.metric(
            label="Total Orders",
            value=f"{total_orders:,}",
            delta=None
        )
    
    with col2:
        avg_order = _safe_float(order_summary.get('avg_order_amount'))
        st.metric(
            label="Avg Order Value",
            value=f"${avg_order:.2f}",
            delta=None
        )
    
    with col3:
        delivery_rate = _safe_float(order_summary.get('delivery_rate_pct'))
        st.metric(
            label="Delivery Rate",
            value=f"{delivery_rate:.1f}%",
            delta=f"{'✓' if delivery_rate > 90 else '⚠'} {'Excellent' if delivery_rate > 90 else 'Review'}",
            delta_color="normal"
        )
    
    with col4:
        orders_with_tickets = _safe_int(order_summary.get('orders_with_tickets'))
        ticket_rate = (orders_with_tickets / total_orders * 100) if total_orders > 0 else 0
        st.metric(
            label="Orders w/ Tickets",
            value=f"{orders_with_tickets:,} ({ticket_rate:.1f}%)",
            delta=f"{'⚠' if ticket_rate > 10 else '✓'} {'High' if ticket_rate > 10 else 'Good'}",
            delta_color="inverse"
        )
    
    st.divider()
    
    # Ticket Metrics Row
    st.subheader("🎫 Ticket Metrics")
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

    # Third row — first-response and refund metrics
    col9, col10 = st.columns(2)
    with col9:
        fr_min = _safe_float(kpi.get('avg_first_response_minutes'))
        st.metric(
            label="Avg First Response",
            value=f"{fr_min:.1f} min",
            delta=None,
        )
    with col10:
        st.metric(
            label="Tickets w/ Refund",
            value=f"{_safe_int(kpi.get('tickets_with_refund')):,}",
            delta=None,
        )
    
    st.divider()


def render_trends(data):
    """Render order and ticket trends."""
    st.subheader("📈 Trends Analysis")
    
    tab1, tab2 = st.tabs(["Order Trends", "Ticket Breakdown"])
    
    with tab1:
        order_trends = data['order_trends']
        if not order_trends.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.line(
                    order_trends.sort_values('order_date'),
                    x='order_date',
                    y='total_orders',
                    title='Daily Order Volume',
                    labels={'order_date': 'Date', 'total_orders': 'Orders'},
                    markers=True
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, width='stretch')
            
            with col2:
                fig = px.line(
                    order_trends.sort_values('order_date'),
                    x='order_date',
                    y='daily_revenue',
                    title='Daily Revenue',
                    labels={'order_date': 'Date', 'daily_revenue': 'Revenue ($)'},
                    markers=True
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, width='stretch')
    
    with tab2:
        order_trends = data['order_trends']
        if not order_trends.empty:
            fig = px.bar(
                order_trends.sort_values('order_date'),
                x='order_date',
                y=['delivered_orders', 'cancelled_orders', 'orders_with_tickets'],
                title='Order Status Breakdown',
                labels={'order_date': 'Date', 'value': 'Orders'},
                barmode='stack'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, width='stretch')
    
    st.divider()


def render_breakdowns(data):
    """Render dimensional breakdowns."""
    st.subheader("📊 Breakdown by Dimensions")
    
    tab1, tab2, tab3, tab4 = st.tabs(["Orders by Region", "Orders by Restaurant", "Tickets by Location", "Tickets by Restaurant"])
    
    with tab1:
        orders_by_region = data['orders_by_region']
        if not orders_by_region.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.bar(
                    orders_by_region.head(10),
                    x='region_name',
                    y='total_orders',
                    title='Orders by Region',
                    labels={'region_name': 'Region', 'total_orders': 'Orders'}
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, width='stretch')
            
            with col2:
                fig = px.pie(
                    orders_by_region.head(10),
                    values='total_revenue',
                    names='region_name',
                    title='Revenue Distribution by Region',
                    hole=0.3
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, width='stretch')
    
    with tab2:
        orders_by_restaurant = data['orders_by_restaurant']
        if not orders_by_restaurant.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.bar(
                    orders_by_restaurant.head(10),
                    x='restaurant_name',
                    y='total_orders',
                    color='category_name',
                    title='Top 10 Restaurants by Order Volume',
                    labels={'restaurant_name': 'Restaurant', 'total_orders': 'Orders'}
                )
                fig.update_layout(height=400, xaxis_tickangle=-45)
                st.plotly_chart(fig, width='stretch')
            
            with col2:
                orders_plot = orders_by_restaurant.head(10).copy()
                orders_plot['total_revenue'] = orders_plot['total_revenue'].astype(float)
                fig = px.scatter(
                    orders_plot,
                    x='total_orders',
                    y='avg_order_value',
                    size=orders_plot['total_revenue'].tolist(),
                    color='category_name',
                    hover_data=['restaurant_name', 'region_name'],
                    title='Order Volume vs Avg Order Value',
                    labels={'total_orders': 'Total Orders', 'avg_order_value': 'Avg Order ($)'}
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, width='stretch')
    
    with tab3:
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
    
    with tab4:
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
                by_rest_plot = by_restaurant.copy()
                by_rest_plot['total_refund_amount'] = by_rest_plot['total_refund_amount'].astype(float)
                fig = px.scatter(
                    by_rest_plot,
                    x='total_tickets',
                    y='avg_resolution_minutes',
                    size=by_rest_plot['total_refund_amount'].tolist(),
                    color='category_name',
                    hover_data=['restaurant_name'],
                    title='Resolution Time vs Ticket Volume',
                    labels={'total_tickets': 'Total Tickets', 
                           'avg_resolution_minutes': 'Avg Resolution (min)'}
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
        tab1, tab2, tab3, tab4 = st.tabs(["Orders by Region", "Orders by Restaurant", "Tickets by Location", "Tickets by Restaurant"])

        with tab1:
            st.dataframe(data['orders_by_region'], width='stretch')

        with tab2:
            st.dataframe(data['orders_by_restaurant'], width='stretch')

        with tab3:
            st.dataframe(data['by_location'], width='stretch')

        with tab4:
            st.dataframe(data['by_restaurant'], width='stretch')


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
