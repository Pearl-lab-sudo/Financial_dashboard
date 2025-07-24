# enhanced_financial_dashboard.py
import streamlit as st
import pandas as pd
from datetime import date, timedelta, datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import logging
import plotly.express as px
import plotly.graph_objects as go

# --- SETUP ---
load_dotenv()
logging.basicConfig(level=logging.INFO)

# --- DB CONNECTION ---
DB_USER = st.secrets["DB_USER"]
DB_PASSWORD = st.secrets["DB_PASSWORD"]
DB_HOST = st.secrets["DB_HOST"]
DB_PORT = st.secrets["DB_PORT"]
DB_NAME = st.secrets["DB_NAME"]
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# --- PAGE CONFIG ---
st.set_page_config(
    page_title="Ladder Financial Dashboard", 
    layout="wide", 
    page_icon="💼",
    initial_sidebar_state="expanded"
)

# --- CUSTOM CSS FOR BETTER STYLING ---
st.markdown("""
<style>
.metric-container {
    background-color: #f0f2f6;
    padding: 1rem;
    border-radius: 0.5rem;
    border-left: 4px solid #1f77b4;
}

.tooltip {
    position: relative;
    display: inline-block;
    cursor: help;
}

.tooltip .tooltiptext {
    visibility: hidden;
    width: 200px;
    background-color: black;
    color: white;
    text-align: center;
    border-radius: 6px;
    padding: 5px;
    position: absolute;
    z-index: 1;
    bottom: 125%;
    left: 50%;
    margin-left: -100px;
    opacity: 0;
    transition: opacity 0.3s;
}

.tooltip:hover .tooltiptext {
    visibility: visible;
    opacity: 1;
}

.page-header {
    background: linear-gradient(90deg, #1f77b4, #ff7f0e);
    color: white;
    padding: 1rem;
    border-radius: 0.5rem;
    margin-bottom: 1rem;
}

.section-divider {
    border-top: 2px solid #e0e0e0;
    margin: 2rem 0;
}
</style>
""", unsafe_allow_html=True)

# --- METRIC EXPLANATIONS ---
METRIC_EXPLANATIONS = {
    "deposit_value_ghs": "Total amount deposited by customers in Ghana Cedis during the selected period",
    "deposit_value_usd": "Total amount deposited by customers in US Dollars during the selected period",
    "deposit_count": "Number of successful deposit transactions completed during the selected period",
    "withdrawal_value_ghs": "Total amount withdrawn by customers in Ghana Cedis during the selected period",
    "withdrawal_value_usd": "Total amount withdrawn by customers in US Dollars during the selected period",
    "withdrawal_count": "Number of successful withdrawal transactions completed during the selected period",
    "aum_ghs": "Assets Under Management - Net amount (Deposits minus Withdrawals) in Ghana Cedis",
    "aum_usd": "Assets Under Management - Net amount (Deposits minus Withdrawals) in US Dollars",
    "total_depositors": "Number of unique customers who made at least one deposit during the period",
    "new_depositors": "Customers making their very first deposit during the selected period",
    "recurring_depositors": "Customers who have made deposits on multiple different days (repeat customers)",
    "total_withdrawers": "Number of unique customers who made at least one withdrawal during the period",
    "avg_deposit_value_ghs": "Average amount per deposit transaction in Ghana Cedis",
    "avg_deposit_value_usd": "Average amount per deposit transaction in US Dollars",
    "avg_withdrawal_value_ghs": "Average amount per withdrawal transaction in Ghana Cedis",
    "avg_withdrawal_value_usd": "Average amount per withdrawal transaction in US Dollars",
    "estimated_revenue": "Calculated revenue based on asset-specific fee structures and transaction volumes",
    "registered_users": "Number of user registrations/signups during the selected period",
    "kyc_users": "Total number of users who have completed KYC verification (all time)",
    "asset_type_count": "Number of different asset types with transactions during the period",
    "total_users": "Number of users with asset type during the period",
    "active_users": "Number of users who have made at least one transaction (deposit or withdrawal) or used the spending feature during the period",
    "users_with_most_recent_activity": "Users who have had any form of activity on the app"
}

def create_metric_with_tooltip(label, value, explanation):
    """
    Create a metric display with tooltip explanation
    """
    try:
        # logging.info(f"testing params: {label}, {value}, {explanation}")
        return f"""
        <div class="metric-container">
            <div class="tooltip">
                <strong>{label}</strong> ✍🏿
                <span class="tooltiptext">{explanation}</span>
            </div>
            <div style="font-size: 24px; font-weight: bold; color: #1f77b4;">
                {value}
            </div>
        </div>
        """
    except Exception as e:
        logging.info(f"Error creating metric with tooltip: {e}")


def create_metric(label, value, explanation):
    return f""" (   
       <div class="metric-container">
            <div class="tooltip">
                <strong>{label}</strong> ✍🏿
                <span class="tooltiptext">{explanation}</span>
            </div>
            <div style="font-size: 24px; font-weight: bold; color: #1f77b4;">
                {value}
            </div>
        </div>
        """


# --- DATE RANGE UTILITY ---
def get_date_range(option):
    today = date.today()
    start = end = today
    if option == "Today":
        start = end = today
    elif option == "Past 7 Days":
        start = today - timedelta(days=6)
        end = today
    elif option == "Last 14 Days":
        start = today - timedelta(days=13)
        end = today
    elif option == "This Week":
        start = today - timedelta(days=today.weekday() + 1) if today.weekday() != 6 else today
        end = today
    elif option == "Last Week":
        end = today - timedelta(days=today.weekday() + 1)
        start = end - timedelta(days=6)
    elif option == "Last 30 Days":
        start = today - timedelta(days=29)
        end = today
    elif option == "Last Month":
        first_this_month = today.replace(day=1)
        last_month_end = first_this_month - timedelta(days=1)
        start = last_month_end.replace(day=1)
        end = last_month_end
    elif option == "All Time":
        start = date(2022, 1, 1)
        end = today
    return start, end

# --- SIDEBAR ---
st.sidebar.markdown('<div class="page-header"><h2>📊 Dashboard Controls</h2></div>', unsafe_allow_html=True)

page = st.sidebar.selectbox(
    "Select a Page:", 
    ["📈 General Overview", "📊 Asset Breakdown", "👥 User Information"],
    help="Navigate between different dashboard sections"
)

# --- DATE RANGE FILTER ---
st.sidebar.markdown("### 📅 Date Range Selection")
date_options = [
    "Today", "Past 7 Days", "Last 14 Days", "This Week", "Last Week",
    "Last 30 Days", "Last Month", "All Time"
]
date_choice = st.sidebar.radio(
    "Quick Date Filters", 
    date_options,
    help="Select a predefined date range for analysis"
)
range_start, range_end = get_date_range(date_choice)

# --- MANUAL DATE INPUT OVERRIDE ---
st.sidebar.markdown("### 🗓️ Custom Date Range")
custom_start = st.sidebar.date_input(
    "Start Date", 
    value=range_start, 
    min_value=date(2022, 1, 1), 
    max_value=date.today(),
    help="Select custom start date for analysis"
)
custom_end = st.sidebar.date_input(
    "End Date", 
    value=range_end, 
    min_value=custom_start, 
    max_value=date.today(),
    help="Select custom end date for analysis"
)

# --- FINAL DATE RANGE TO USE ---
start_date, end_date = custom_start, custom_end
st.sidebar.success(f"📆 Analysis Period: {start_date.strftime('%B %d, %Y')} → {end_date.strftime('%B %d, %Y')}")

# --- ENHANCED QUERY FUNCTIONS ---
@st.cache_data(show_spinner=True)
def load_general_metrics(start_date, end_date):
    """
    Load general financial metrics for the dashboard.
    
    This query calculates:
    - Total deposits and withdrawals (both GHS and USD)
    - Assets Under Management (AUM)
    - Customer segmentation (new vs recurring depositors)
    - Average transaction values
    """
    try:
        with engine.connect() as conn:
            general_query = text("""
            WITH first_transactions AS (
                -- Find each customer's first transaction date
                SELECT 
                    COALESCE(p.user_id, ip.user_id) AS user_id,
                    MIN(t.updated_at) AS first_transaction_date
                FROM transactions t
                LEFT JOIN investment_plans ip ON ip.id = t.investment_plan_id
                LEFT JOIN plans p ON p.id = t.plan_id
                WHERE t.status = 'success'
                GROUP BY COALESCE(p.user_id, ip.user_id)
            ),

            base_data AS (
                -- Main transaction data with customer and asset information
                SELECT 
                    t.id AS transaction_id,
                    CASE 
                        WHEN t.metadata::text ILIKE '%Monthly maintenance fee deduction%' THEN 'maintenance_fee'
                        ELSE t.tx_type
                    END AS transaction_type,
                    CASE
                        WHEN t.investment_plan_id IS NOT NULL THEN a.name
                        WHEN t.plan_id IS NOT NULL THEN p.plan_option
                        ELSE NULL
                    END AS asset_type,
                    u.id AS customer_id,
                    u.first_name || ' ' || u.last_name AS customer_name,
                    ft.first_transaction_date,
                    TO_CHAR(t.updated_at, 'YYYY-MM') AS transaction_cohort,
                    TO_CHAR(u.created_at, 'YYYY-MM') AS sign_up_cohort,
                    EXTRACT(WEEK FROM t.updated_at) AS week_number,
                    t.updated_at AS transaction_date,
                    t.amount AS ghs_amount,
                    t.usd_amount,
                    t.exchange_rate,
                    u.created_at AS sign_up_date,
                    CASE  
                        WHEN t.investment_plan_id IS NOT NULL THEN ip.plan_option
                        WHEN t.plan_id IS NOT NULL THEN p.plan_option
                        ELSE NULL
                    END AS investment_type,
                    a.name AS asset_name,
                    a.maturity_date AS assets_maturity_date,
                    ip.maturity_date AS investment_maturity_date
                FROM transactions t
                LEFT JOIN investment_plans ip ON ip.id = t.investment_plan_id
                LEFT JOIN plans p ON p.id = t.plan_id
                LEFT JOIN users u ON u.id = COALESCE(p.user_id, ip.user_id)
                LEFT JOIN first_transactions ft ON ft.user_id = u.id
                LEFT JOIN assets a ON a.id = ip.asset_id
                WHERE t.status = 'success' 
                  AND u.restricted = 'false' 
                  AND t.provider_number != 'Flex Dollar'
            ),

            deposits AS (
                SELECT * FROM base_data WHERE transaction_type = 'deposit'
            ),

            withdrawals AS (
                SELECT * FROM base_data WHERE transaction_type = 'withdrawal'
            ),

            depositors_summary AS (
                -- Customer deposit behavior analysis
                SELECT 
                    customer_id,
                    COUNT(*) AS deposit_count,
                    MIN(transaction_date) AS first_deposit_date,
                    COUNT(DISTINCT transaction_date) AS tx_days
                FROM deposits
                GROUP BY customer_id
            )

            -- Final aggregated metrics
            SELECT
                COUNT(DISTINCT asset_type) AS asset_type_count,

                -- Deposit metrics
                COUNT(*) FILTER (WHERE transaction_type = 'deposit') AS deposit_count,
                SUM(ghs_amount) FILTER (WHERE transaction_type = 'deposit') AS deposit_value_ghs,
                SUM(usd_amount) FILTER (WHERE transaction_type = 'deposit') AS deposit_value_usd,

                -- Withdrawal metrics
                COUNT(*) FILTER (WHERE transaction_type = 'withdrawal') AS withdrawal_count,
                SUM(ghs_amount) FILTER (WHERE transaction_type = 'withdrawal') AS withdrawal_value_ghs,
                SUM(usd_amount) FILTER (WHERE transaction_type = 'withdrawal') AS withdrawal_value_usd,

                -- Assets Under Management (Net Position)
                SUM(ghs_amount) FILTER (WHERE transaction_type = 'deposit') - 
                    COALESCE(SUM(ghs_amount) FILTER (WHERE transaction_type = 'withdrawal'), 0) AS aum_ghs,
                SUM(usd_amount) FILTER (WHERE transaction_type = 'deposit') - 
                    COALESCE(SUM(usd_amount) FILTER (WHERE transaction_type = 'withdrawal'), 0) AS aum_usd,

                -- Customer segmentation
                COUNT(DISTINCT base_data.customer_id) FILTER (WHERE transaction_type = 'deposit') AS total_depositors,
                COUNT(DISTINCT base_data.customer_id) FILTER (WHERE transaction_type = 'withdrawal') AS total_withdrawers,
                COUNT(DISTINCT ds.customer_id) FILTER (WHERE tx_days > 1) AS recurring_depositors,
                COUNT(DISTINCT ds.customer_id) FILTER (WHERE tx_days = 1 AND DATE(first_deposit_date) BETWEEN :start_date AND :end_date) AS new_depositors,

                -- Average transaction values
                AVG(ghs_amount) FILTER (WHERE transaction_type = 'deposit') AS avg_deposit_value_ghs,
                AVG(usd_amount) FILTER (WHERE transaction_type = 'deposit') AS avg_deposit_value_usd,
                AVG(ghs_amount) FILTER (WHERE transaction_type = 'withdrawal') AS avg_withdrawal_value_ghs,
                AVG(usd_amount) FILTER (WHERE transaction_type = 'withdrawal') AS avg_withdrawal_value_usd

            FROM base_data
            LEFT JOIN depositors_summary ds USING (customer_id)
            WHERE transaction_date BETWEEN :start_date AND :end_date;
            """)
            df = pd.read_sql(general_query, conn, params={"start_date": str(start_date), "end_date": str(end_date)})
            return df
    except Exception as e:
        logging.error("General metrics load failed: %s", str(e))
        st.error(f"Failed to load general metrics: {str(e)}")
        return pd.DataFrame()

def load_transaction_trend(start_date, end_date, granularity="month"):
    try:
        with engine.connect() as conn:
            trend_query = text(f"""
                SELECT
                    DATE_TRUNC(:granularity, t.updated_at) AS period,
                    t.tx_type,
                    SUM(t.amount) AS total_amount
                FROM transactions t
                LEFT JOIN investment_plans ip ON ip.id = t.investment_plan_id
                LEFT JOIN plans p ON p.id = t.plan_id
                LEFT JOIN users u ON u.id = COALESCE(p.user_id, ip.user_id)
                WHERE t.status = 'success'
                  AND u.restricted = false
                  AND t.provider_number != 'Flex Dollar'
                  AND t.updated_at BETWEEN :start_date AND :end_date
                GROUP BY period, t.tx_type
                ORDER BY period ASC
            """)
            df = pd.read_sql(trend_query, conn, params={
                "granularity": granularity,
                "start_date": str(start_date),
                "end_date": str(end_date)
            })
            return df
    except Exception as e:
        logging.error("Trend data load failed: %s", str(e))
        st.error(f"Failed to load transaction trend: {str(e)}")
        return pd.DataFrame()

@st.cache_data(show_spinner=True)
def load_asset_metrics(start_date, end_date):
    """
    Load asset-specific financial metrics.
    
    This query breaks down performance by asset type and calculates:
    - Revenue estimates based on asset-specific fee structures
    - Asset-specific customer behavior
    - Maintenance fees and early withdrawal penalties
    """
    try:
        with engine.connect() as conn:
            asset_query = text("""
            WITH first_transactions AS (
                SELECT 
                    COALESCE(p.user_id, ip.user_id) AS user_id,
                    MIN(t.updated_at) AS first_transaction_date
                FROM transactions t
                LEFT JOIN investment_plans ip ON ip.id = t.investment_plan_id
                LEFT JOIN plans p ON p.id = t.plan_id
                WHERE t.status = 'success'
                GROUP BY COALESCE(p.user_id, ip.user_id)
            ),

            base_data AS (
                SELECT 
                    t.id AS transaction_id,
                    CASE 
                        WHEN t.metadata::text ILIKE '%Monthly maintenance fee deduction%' THEN 'maintenance_fee'
                        ELSE t.tx_type
                    END AS transaction_type,
                    CASE
                        WHEN t.investment_plan_id IS NOT NULL THEN a.name
                        WHEN t.plan_id IS NOT NULL THEN p.plan_option
                        ELSE NULL
                    END AS asset_type,
                    u.id AS customer_id,
                    u.first_name || ' ' || u.last_name AS customer_name,
                    ft.first_transaction_date,
                    TO_CHAR(t.updated_at, 'YYYY-MM') AS transaction_cohort,
                    TO_CHAR(u.created_at, 'YYYY-MM') AS sign_up_cohort,
                    EXTRACT(WEEK FROM t.updated_at) AS week_number,
                    t.updated_at AS transaction_date,
                    t.amount AS ghs_amount,
                    t.usd_amount,
                    t.exchange_rate,
                    u.created_at AS sign_up_date,
                    CASE  
                        WHEN t.investment_plan_id IS NOT NULL THEN ip.plan_option
                        WHEN t.plan_id IS NOT NULL THEN p.plan_option
                        ELSE NULL
                    END AS investment_type,
                    a.name AS asset_name,
                    a.maturity_date AS assets_maturity_date,
                    ip.maturity_date AS investment_maturity_date
                FROM transactions t
                LEFT JOIN investment_plans ip ON ip.id = t.investment_plan_id
                LEFT JOIN plans p ON p.id = t.plan_id
                LEFT JOIN users u ON u.id = COALESCE(p.user_id, ip.user_id)
                LEFT JOIN first_transactions ft ON ft.user_id = u.id
                LEFT JOIN assets a ON a.id = ip.asset_id
                WHERE t.status = 'success' AND u.restricted = 'false' AND t.provider_number != 'Flex Dollar'
            ),

            deposits AS (
                SELECT * FROM base_data WHERE transaction_type = 'deposit'
            ),

            withdrawals AS (
                SELECT * FROM base_data WHERE transaction_type = 'withdrawal'
            ),

            depositors_summary AS (
                SELECT 
                    customer_id,
                    asset_type,
                    COUNT(*) AS deposit_count,
                    MIN(transaction_date) AS first_deposit_date,
                    COUNT(DISTINCT transaction_date) AS tx_days
                FROM deposits
                GROUP BY customer_id, asset_type
            ),

            asset_metrics AS (
                SELECT
                    bd.asset_type,
                    COUNT(*) FILTER (WHERE transaction_type = 'deposit') AS deposit_count,
                    SUM(ghs_amount) FILTER (WHERE transaction_type = 'deposit') AS deposit_value_ghs,
                    SUM(usd_amount) FILTER (WHERE transaction_type = 'deposit') AS deposit_value_usd,

                    COUNT(*) FILTER (WHERE transaction_type = 'withdrawal') AS withdrawal_count,
                    SUM(ghs_amount) FILTER (WHERE transaction_type = 'withdrawal') AS withdrawal_value_ghs,
                    SUM(usd_amount) FILTER (WHERE transaction_type = 'withdrawal') AS withdrawal_value_usd,

                    SUM(ghs_amount) FILTER (WHERE transaction_type = 'deposit') - 
                        COALESCE(SUM(ghs_amount) FILTER (WHERE transaction_type = 'withdrawal'), 0) AS aum_ghs,
                    SUM(usd_amount) FILTER (WHERE transaction_type = 'deposit') - 
                        COALESCE(SUM(usd_amount) FILTER (WHERE transaction_type = 'withdrawal'), 0) AS aum_usd,

                    COUNT(DISTINCT bd.customer_id) FILTER (WHERE transaction_type = 'deposit') AS total_depositors,
                    COUNT(DISTINCT ds.customer_id) FILTER (WHERE tx_days > 1) AS recurring_depositors,
                    COUNT(DISTINCT ds.customer_id) FILTER (WHERE tx_days = 1 AND DATE(ds.first_deposit_date) BETWEEN :start_date AND :end_date) AS new_depositors,

                    AVG(ghs_amount) FILTER (WHERE transaction_type = 'deposit') AS avg_deposit_value_ghs,
                    AVG(usd_amount) FILTER (WHERE transaction_type = 'deposit') AS avg_deposit_value_usd,

                    AVG(ghs_amount) FILTER (WHERE transaction_type = 'withdrawal') AS avg_withdrawal_value_ghs,
                    AVG(usd_amount) FILTER (WHERE transaction_type = 'withdrawal') AS avg_withdrawal_value_usd,

                    -- Revenue calculation based on asset-specific fee structures
                    CASE
                        WHEN bd.asset_type = 'Arbitrage' THEN 0.01 * (
                            COALESCE(SUM(ghs_amount) FILTER (WHERE transaction_type = 'deposit'),0) +
                            COALESCE(SUM(ghs_amount) FILTER (WHERE transaction_type = 'withdrawal'),0)
                        )
                        WHEN bd.asset_type = 'flex dollar savings' THEN (0.0475 / 12.0) * 
                            COALESCE(SUM(usd_amount) FILTER (WHERE transaction_type = 'deposit'),0)
                        WHEN bd.asset_type = 'Ladder Lock' THEN (0.06 / 12.0) * 
                            COALESCE(SUM(usd_amount) FILTER (WHERE transaction_type = 'deposit'),0)
                        WHEN bd.asset_type = 'goal savings' THEN (0.06 / 12.0) * 
                            COALESCE(SUM(usd_amount) FILTER (WHERE transaction_type = 'deposit'),0)
                        WHEN bd.asset_type = 'Risevest fixed income' THEN (0.02 / 12.0) * 
                            COALESCE(SUM(usd_amount) FILTER (WHERE transaction_type = 'deposit'),0)
                        WHEN bd.asset_type = 'Risevest real estate' THEN (0.04 / 12.0) * 
                            COALESCE(SUM(usd_amount) FILTER (WHERE transaction_type = 'deposit'),0)
                        WHEN bd.asset_type = 'Equity' THEN 0.02 * COALESCE(SUM(ghs_amount) FILTER (WHERE transaction_type = 'deposit'),0)
                        WHEN bd.asset_type = 'Mutual funds' THEN 0.02 * COALESCE(SUM(ghs_amount) FILTER (WHERE transaction_type = 'deposit'),0)
                        WHEN bd.asset_type = 'ETFs' THEN 0.02 * COALESCE(SUM(usd_amount) FILTER (WHERE transaction_type = 'deposit'),0)
                        ELSE 0
                    END AS estimated_revenue,

                    -- Fee collections
                    SUM(ghs_amount) FILTER (WHERE transaction_type = 'maintenance_fee') AS maintenance_fees_ghs,
                    SUM(usd_amount) FILTER (WHERE transaction_type = 'early_withdrawal') * 0.025 AS early_withdrawal_fees_usd

                FROM base_data bd
                LEFT JOIN depositors_summary ds ON ds.customer_id = bd.customer_id AND ds.asset_type = bd.asset_type
                WHERE transaction_date BETWEEN :start_date AND :end_date
                GROUP BY bd.asset_type
            )

            SELECT * FROM asset_metrics
            ORDER BY deposit_value_usd DESC NULLS LAST;
            """)
            df = pd.read_sql(asset_query, conn, params={"start_date": str(start_date), "end_date": str(end_date)})
            return df
    except Exception as e:
        logging.error("Asset metrics load failed: %s", str(e))
        st.error(f"Failed to load asset metrics: {str(e)}")
        return pd.DataFrame()

@st.cache_data(show_spinner=True)
def load_user_counts(start_date, end_date):
    """
    Load user registration and KYC completion metrics.
    
    Note: All values are filtered by the selected date range.
    """
    try:
        with engine.connect() as conn:
            # All-time registered users
            registered_query = text("""
                SELECT COUNT(*) AS registered_users 
                FROM users 
                WHERE DATE(created_at) BETWEEN :start_date AND :end_date;
            """)

            # All-time KYC completed users
            kyc_query = text("""
                SELECT COUNT(*) AS kyc_users 
                FROM users 
                WHERE metadata::text ILIKE '%kyc_completed%' 
                  AND DATE(updated_at) BETWEEN :start_date AND :end_date;
            """)

            params = {"start_date": str(start_date), "end_date": str(end_date)}

            reg_df = pd.read_sql(registered_query, conn, params=params)
            kyc_df = pd.read_sql(kyc_query, conn, params=params)

            return (
                reg_df.loc[0, "registered_users"], 
                kyc_df.loc[0, "kyc_users"], 
            )
    except Exception as e:
        logging.error("User counts load failed: %s", str(e))
        return 0, 0

@st.cache_data(show_spinner=True)
def load_total_users_by_asset_type(start_date, end_date):
    """
    Load user count by asset type for the selected period.
    Shows how many unique users interacted with each asset type.
    """
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT
                    CASE
                        WHEN t.investment_plan_id IS NOT NULL THEN a.name
                        WHEN t.plan_id IS NOT NULL THEN p.plan_option
                        ELSE NULL
                    END AS asset_type,
                    COUNT(DISTINCT u.id) AS total_users
                FROM transactions t
                LEFT JOIN investment_plans ip ON ip.id = t.investment_plan_id
                LEFT JOIN plans p ON p.id = t.plan_id
                LEFT JOIN users u ON u.id = COALESCE(p.user_id, ip.user_id)
                LEFT JOIN assets a ON a.id = ip.asset_id
                WHERE t.status = 'success'
                  AND u.restricted = 'false'
                  AND t.provider_number != 'Flex Dollar'
                  AND t.updated_at BETWEEN :start_date AND :end_date
                  AND CASE
                        WHEN t.investment_plan_id IS NOT NULL THEN a.name
                        WHEN t.plan_id IS NOT NULL THEN p.plan_option
                        ELSE NULL
                    END IS NOT NULL
                GROUP BY asset_type
                ORDER BY total_users DESC
            """)
            df = pd.read_sql(query, conn, params={"start_date": str(start_date), "end_date": str(end_date)})
            return df
    except Exception as e:
        logging.error("Total users by asset type load failed: %s", str(e))
        return pd.DataFrame()

@st.cache_data(show_spinner=True)
def get_asset_types(start_date, end_date):
    """Get list of available asset types for the selected period"""
    try:
        with engine.connect() as conn:
            query = text("""
                SELECT DISTINCT 
                    CASE
                        WHEN t.investment_plan_id IS NOT NULL THEN a.name
                        WHEN t.plan_id IS NOT NULL THEN p.plan_option
                        ELSE NULL
                    END AS asset_type
                FROM transactions t
                LEFT JOIN investment_plans ip ON ip.id = t.investment_plan_id
                LEFT JOIN plans p ON p.id = t.plan_id
                LEFT JOIN assets a ON a.id = ip.asset_id
                WHERE t.status = 'success'
                  AND t.updated_at BETWEEN :start_date AND :end_date
                  AND CASE
                        WHEN t.investment_plan_id IS NOT NULL THEN a.name
                        WHEN t.plan_id IS NOT NULL THEN p.plan_option
                        ELSE NULL
                    END IS NOT NULL
                ORDER BY asset_type
            """)
            df = pd.read_sql(query, conn, params={"start_date": str(start_date), "end_date": str(end_date)})
            return df["asset_type"].tolist()
    except Exception as e:
        logging.error("Asset types load failed: %s", str(e))
        return []

# --- UTILITY FUNCTIONS ---
def fmt(val):
    """Format currency values with commas and 2 decimal places"""
    return f"{val:,.2f}" if pd.notnull(val) and val is not None else "0.00"

def fmt_int(val):
    """Format integer values"""
    return f"{int(val):,}" if pd.notnull(val) and val is not None else "0"

def create_summary_chart(df, title):
    """Create a summary chart for asset performance"""
    fig = px.bar(
        df, 
        x='asset_type', 
        y='deposit_value_usd', 
        title=title,
        labels={'deposit_value_usd': 'Deposit Value (USD)', 'asset_type': 'Asset Type'}
    )
    fig.update_layout(xaxis_tickangle=-45)
    return fig

# --- PAGE IMPLEMENTATIONS ---

if page == "📈 General Overview":
    st.markdown('<div class="page-header"><h1>📈 Financial Performance Overview</h1><p>Comprehensive view of platform-wide financial metrics and customer behavior</p></div>', unsafe_allow_html=True)
    
    # Load data
    general_df = load_general_metrics(start_date, end_date)
    registered_users, kyc_users = load_user_counts(start_date, end_date)

    if not general_df.empty:
        row = general_df.iloc[0]
        
        # Key Performance Indicators
        st.markdown("## 💰 Transaction Volume & Value")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown(
                create_metric_with_tooltip(
                    "Total Deposits (GHS)", 
                    f"₵{fmt(row['deposit_value_ghs'])}", 
                    METRIC_EXPLANATIONS["deposit_value_ghs"]
                ), 
                unsafe_allow_html=True
            )
        with col2:
            st.markdown(
                create_metric_with_tooltip(
                    "Total Deposits (USD)", 
                    f"${fmt(row['deposit_value_usd'])}", 
                    METRIC_EXPLANATIONS["deposit_value_usd"]
                ),
                unsafe_allow_html=True
            )
        with col3:
            st.markdown(
                create_metric_with_tooltip(
                    "Total Deposits Count", 
                    fmt_int(row['deposit_count']), 
                    METRIC_EXPLANATIONS["deposit_count"]
                ),
                unsafe_allow_html=True
            )
        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
        col4, col5, col6 = st.columns(3)
        with col4:
            st.markdown(
                create_metric_with_tooltip(
                    "Total Withdrawals (GHS)", 
                    f"₵{fmt(row['withdrawal_value_ghs'])}", 
                    METRIC_EXPLANATIONS["withdrawal_value_ghs"]
                ),
                unsafe_allow_html=True
            )
        with col5:
            st.markdown(
                create_metric_with_tooltip(
                    "Total Withdrawals (USD)", 
                    f"${fmt(row['withdrawal_value_usd'])}", 
                    METRIC_EXPLANATIONS["withdrawal_value_usd"]
                ),
                unsafe_allow_html=True
            )
        with col6:
            st.markdown(
                create_metric_with_tooltip(
                    "Total Withdrawals Count", 
                    fmt_int(row['withdrawal_count']), 
                    METRIC_EXPLANATIONS["withdrawal_count"]
                ),
                unsafe_allow_html=True
            )
        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
        col7, col8, col9 = st.columns(3)
        with col7:
            st.markdown(
                create_metric_with_tooltip(
                    "Assets Under Management (AUM) GHS", 
                    f"₵{fmt(row['aum_ghs'])}", 
                    METRIC_EXPLANATIONS["aum_ghs"]
                ),
                unsafe_allow_html=True
            )
        with col8:
            st.markdown(
                create_metric_with_tooltip(
                    "Assets Under Management (AUM) USD", 
                    f"${fmt(row['aum_usd'])}", 
                    METRIC_EXPLANATIONS["aum_usd"]
                ),
                unsafe_allow_html=True
            )
        with col9:
            st.markdown(
                create_metric_with_tooltip(
                    "New Depositors", 
                    fmt_int(row['new_depositors']), 
                    METRIC_EXPLANATIONS["new_depositors"]
                ),
                unsafe_allow_html=True
            )
        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
        col10, col11, col12 = st.columns(3)
        with col10:
            st.markdown(
                create_metric_with_tooltip(
                    "Recurring Depositors", 
                    fmt_int(row['recurring_depositors']), 
                    METRIC_EXPLANATIONS["recurring_depositors"]
                ),
                unsafe_allow_html=True
            )
        with col11:
            st.markdown(
                create_metric_with_tooltip(
                    "Registered Users", 
                    fmt_int(registered_users), 
                    METRIC_EXPLANATIONS["registered_users"]
                ),
                unsafe_allow_html=True
            )
        with col12:
            st.markdown(
                create_metric_with_tooltip(
                    "KYC Verified Users", 
                    fmt_int(kyc_users), 
                    METRIC_EXPLANATIONS["kyc_users"]
                ),
                unsafe_allow_html=True
            )
            st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
        st.markdown("---")
        
        # Trend Chart Title
        st.markdown("## 📊 Transaction Volume Trend", unsafe_allow_html=True)

        # Date Granularity Dropdown
        granularity_option = st.selectbox(
            "Group transactions by:",
            options=["day", "week", "month"],
            index=2  # default is 'month'
        )

        # Load Data
        trend_df = load_transaction_trend(start_date, end_date, granularity_option)

        if not trend_df.empty:
            # Prepare Data
            pivot_df = trend_df.pivot(index="period", columns="tx_type", values="total_amount").fillna(0)
            pivot_df = pivot_df.rename(columns={"deposit": "Deposits", "withdrawal": "Withdrawals"}).reset_index()

            # Ensure both columns exist
            if "Deposits" not in pivot_df.columns:
                pivot_df["Deposits"] = 0
            if "Withdrawals" not in pivot_df.columns:
                pivot_df["Withdrawals"] = 0

            # Optional Debugging
            # st.write("Columns in pivot_df:", pivot_df.columns.tolist())
            # st.dataframe(pivot_df.head())

            # Create Plotly Figure
            fig = go.Figure()

            # Deposits Line
            fig.add_trace(go.Scatter(
                x=pivot_df["period"],
                y=pivot_df["Deposits"],
                mode='lines+markers',
                name='Deposits',
                line=dict(color='#1f77b4', width=3),
                marker=dict(size=6)
            ))

            # Withdrawals Line
            fig.add_trace(go.Scatter(
                x=pivot_df["period"],
                y=pivot_df["Withdrawals"],
                mode='lines+markers',
                name='Withdrawals',
                line=dict(color='#ff7f0e', width=3),
                marker=dict(size=6)
            ))

            # Update Layout: Remove grid, add polish
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                title='Deposit & Withdrawal Trends Over Time',
                xaxis=dict(title='Period', showgrid=False, tickangle=-45),
                yaxis=dict(title='Amount (GHS)', showgrid=False),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                margin=dict(l=20, r=20, t=50, b=20),
                height=400
            )

            # Render in Streamlit
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No transaction trend data for the selected period.")


elif page == "📊 Asset Breakdown":
    st.markdown('<div class="page-header"><h1>📊 Asset Performance Overview</h1><p>Detailed financial metrics by asset type</p></div>', unsafe_allow_html=True)
    # Load asset metrics
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.markdown("### 📊 Asset Type Selection", unsafe_allow_html=True)
    # Get asset types for the dropdown
    asset_types = get_asset_types(start_date, end_date)
    selected_asset_type = st.selectbox("Select an asset type to view detailed metrics and performance indicators.", asset_types)
        
    asset_df = load_asset_metrics(start_date, end_date)
    users_by_asset_df = load_total_users_by_asset_type(start_date, end_date)

    # Filter asset_df and users_by_asset_df for the selected asset type
    asset_row = asset_df[asset_df['asset_type'] == selected_asset_type]
    users_row = users_by_asset_df[users_by_asset_df['asset_type'] == selected_asset_type]

    if not asset_row.empty:
        row = asset_row.iloc[0]
        st.subheader(f"Metrics for {selected_asset_type}")
        col1, col2, col3 = st.columns(3)
        # with col1:
        with col1:
            st.markdown(
                create_metric_with_tooltip(
                    "Total Deposits (GHS)", 
                    f"₵{fmt(row['deposit_value_ghs'])}", 
                    METRIC_EXPLANATIONS["deposit_value_ghs"]
                ), 
                unsafe_allow_html=True
            )
                
        with col2:
            st.markdown(
                create_metric_with_tooltip(
                    "Total Deposits (USD)", 
                    f"${fmt(row['deposit_value_usd'])}", 
                    METRIC_EXPLANATIONS["deposit_value_usd"]
                ), 
                unsafe_allow_html=True
            )
        with col3:
            st.markdown(
                create_metric_with_tooltip(
                    "📥 Deposit Count", 
                    fmt_int(row['deposit_count']),
                    METRIC_EXPLANATIONS["deposit_count"]
                ),
                unsafe_allow_html=True
            )
        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

        col4, col5, col6 = st.columns(3)
        with col4:
            st.markdown(
                create_metric_with_tooltip(
                    "💸 Withdrawal (GHS)", 
                    f"₵{fmt(row['withdrawal_value_ghs'])}",
                    METRIC_EXPLANATIONS["withdrawal_value_ghs"]
                ),
                unsafe_allow_html=True
            )
        with col5:
            st.markdown(
                create_metric_with_tooltip(
                    "Withdrawal (USD)", 
                    f"${fmt(row['withdrawal_value_usd'])}",
                    METRIC_EXPLANATIONS["withdrawal_value_usd"]
                ),
                unsafe_allow_html=True
            )
        with col6:
            st.markdown(
                create_metric_with_tooltip(
                    "📥 Total Withdrawals Count",
                    fmt_int(row['withdrawal_count']),
                    METRIC_EXPLANATIONS["withdrawal_count"]
                ),
                unsafe_allow_html=True
            )
        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

        col7, col8, col9 = st.columns(3)
        with col7:
            st.markdown(
                create_metric_with_tooltip(
                    "📊 AUM (GHS)",
                    fmt(row['aum_ghs']),
                    METRIC_EXPLANATIONS["aum_ghs"]
                ),
                unsafe_allow_html=True
            )
        with col8:
            st.markdown(
                create_metric_with_tooltip(
                    "📊 AUM (USD)",
                    fmt(row['aum_usd']),
                    METRIC_EXPLANATIONS["aum_usd"]
                ),
                unsafe_allow_html=True
            )
        with col9:
            st.markdown(
                create_metric_with_tooltip(
                    "👥 Total Depositors",
                    fmt_int(row['total_depositors']),
                    METRIC_EXPLANATIONS["total_depositors"]
                ),
                unsafe_allow_html=True
            )
        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

        col10, col11, col12 = st.columns(3)    
        with col10:
            st.markdown(
                create_metric_with_tooltip(
                    "🆕 New Depositors",
                    fmt_int(row['new_depositors']),
                    METRIC_EXPLANATIONS["new_depositors"]
                ),
                unsafe_allow_html=True
            )
        with col11:
            st.markdown(
                create_metric_with_tooltip(
                    "🔁 Recurring Depositors",
                    fmt_int(row['recurring_depositors']),
                    METRIC_EXPLANATIONS["recurring_depositors"]
                ),
                unsafe_allow_html=True
            )
        with col12:
            st.markdown(
                create_metric_with_tooltip(
                    "💰 Avg Deposit (GHS)",
                    fmt(row['avg_deposit_value_ghs']),
                    METRIC_EXPLANATIONS["avg_deposit_value_ghs"]
                ),
                unsafe_allow_html=True
            )
        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

        col13, col14, col15 = st.columns(3)
        with col13:
            st.markdown(
                create_metric_with_tooltip(
                    "💰 Avg Deposit (USD)",
                    fmt(row['avg_deposit_value_usd']),
                    METRIC_EXPLANATIONS["avg_deposit_value_usd"]
            ),
                unsafe_allow_html=True
            )
        with col14:
            st.markdown(
                create_metric_with_tooltip(
                    "💸 Avg Withdrawal (GHS)",
                    fmt(row['avg_withdrawal_value_ghs']),
                    METRIC_EXPLANATIONS["avg_withdrawal_value_ghs"]
                ),
                unsafe_allow_html=True
            )
        with col15:
            st.markdown(
                create_metric_with_tooltip(
                    "💸 Avg Withdrawal (USD)",
                    fmt(row['avg_withdrawal_value_usd']),
                    METRIC_EXPLANATIONS["avg_withdrawal_value_usd"]
                ),
                unsafe_allow_html=True
            )
        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
                
        col16, col17 = st.columns(2)
        with col16:
            st.markdown(
                create_metric_with_tooltip(
                    "💰 Estimated Revenue",
                    fmt(row['estimated_revenue']),
                    METRIC_EXPLANATIONS["estimated_revenue"]
                ),
                unsafe_allow_html=True
            )

        # Show total users for this asset type
        with col17:
            if not users_row.empty:
                st.markdown(
                    create_metric_with_tooltip(
                        "👥 Total Users",
                        fmt_int(users_row.iloc[0]['total_users']),
                        METRIC_EXPLANATIONS["total_users"]
                    ),
                    unsafe_allow_html=True
                )

        st.divider()
    else:
        st.info("No data for the selected asset type and date range.")
elif page == "👥 User Information":
    st.markdown('<div class="page-header"><h1>👥 User Information Overview</h1><p>Detailed user metrics and insights</p></div>', unsafe_allow_html=True)
    st.markdown("Analyze user demographics, activity, and engagement metrics over the selected period.")
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    st.markdown("### 📅 Date Range Selection", unsafe_allow_html=True)
    st.markdown(f"**Selected Date Range:** {start_date.strftime('%B %d, %Y')} to {end_date.strftime('%B %d, %Y')}", unsafe_allow_html=True)

    @st.cache_data(show_spinner=True)
    def load_user_insights(start_date, end_date):
        try:
            with engine.connect() as conn:
                query = f"""
                WITH active_users AS (
                    SELECT user_id FROM budgets WHERE created_at BETWEEN '{start_date}' AND '{end_date}'
                    UNION
                    SELECT user_id FROM manual_and_external_transactions WHERE created_at BETWEEN '{start_date}' AND '{end_date}'
                    UNION
                    SELECT user_id FROM investment_plans WHERE created_at BETWEEN '{start_date}' AND '{end_date}'
                    UNION
                    SELECT user_id FROM plans WHERE created_at BETWEEN '{start_date}' AND '{end_date}'
                    UNION
                    SELECT DISTINCT COALESCE(p.user_id, ip.user_id) AS user_id
                    FROM transactions t
                    LEFT JOIN investment_plans ip ON ip.id = t.investment_plan_id
                    LEFT JOIN plans p ON p.id = t.plan_id
                    WHERE t.status = 'success' AND t.updated_at BETWEEN '{start_date}' AND '{end_date}'
                ),
                recent_activity AS (
                    SELECT id FROM users WHERE DATE(most_recent_activity) BETWEEN '{start_date}' AND '{end_date}'
                )
                SELECT 
                    u.id AS user_id,
                    u.gender,
                    u.country,
                    u.dob,
                    u.status,
                    u.ladder_use_option,
                    p.employment_status,
                    u.created_at,
                    CASE WHEN ra.id IS NOT NULL THEN TRUE ELSE FALSE END AS is_recent,
                    CASE WHEN au.user_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_active
                FROM users u
                LEFT JOIN profiles p ON p.user_id = u.id
                LEFT JOIN recent_activity ra ON ra.id = u.id
                LEFT JOIN active_users au ON au.user_id = u.id
                WHERE u.created_at BETWEEN '{start_date}' AND '{end_date}';
                """
                return pd.read_sql(query, conn)
        except Exception as e:
            st.error(f"Failed to load user insights: {str(e)}")
            return pd.DataFrame()

    # LOAD DATA
    df = load_user_insights(start_date, end_date)

    if df.empty:
        st.warning("No user data found for the selected range.")
    else:
        # --- METRICS ---
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(
                create_metric_with_tooltip("🧍 Total Registered Users", len(df), METRIC_EXPLANATIONS["registered_users"]),
                unsafe_allow_html=True
            )
        with col2:
            st.markdown(
                create_metric_with_tooltip("🟢 Active Users", df['is_active'].sum(), METRIC_EXPLANATIONS["active_users"]),
                unsafe_allow_html=True
            )
        with col3:
            st.markdown(
                create_metric_with_tooltip("👤 Users With Recent Activity", df['is_recent'].sum(), METRIC_EXPLANATIONS["users_with_most_recent_activity"]),
                unsafe_allow_html=True
            )

        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

        # --- CLEAN & TRANSFORM DATA ---
        from datetime import date
        import plotly.express as px

        df['gender'] = df['gender'].str.strip().str.lower().replace({
            'non binary': 'non-binary',
            'non-binary': 'non-binary',
            'female': 'female',
            'male': 'male'
        })
        df['ladder_use_option'] = df['ladder_use_option'].str.strip().str.lower().replace({
            'investment': 'investments',
            'investments': 'investments'
        })
        df['status'] = df['status'].str.strip().str.lower().replace({
            'kyc_verifeid': 'kyc_verified'
        })
        df['country'] = df['country'].str.strip()

        df['dob'] = pd.to_datetime(df['dob'], errors='coerce')
        df['age'] = df['dob'].apply(lambda x: (date.today().year - x.year) if pd.notnull(x) else None)
        df['age_group'] = pd.cut(df['age'], bins=list(range(18, 81, 5)), right=False)

        # Helper: Plotly bar chart with custom style
        def plot_bar(data, title, color="#636EFA", xaxis_title="", yaxis_title="Count"):
            fig = px.bar(
                data,
                x=data.index.astype(str),
                y=data.values,
                text=data.values,
                labels={'x': xaxis_title, 'y': yaxis_title},
                title=title
            )
            fig.update_traces(marker_color=color, textposition='outside')
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                xaxis=dict(showgrid=False),
                yaxis=dict(showgrid=False),
                margin=dict(l=20, r=20, t=40, b=20),
                height=350
            )
            return fig

        # --- VISUALS ---
        st.subheader("📊 User Demographics")

        gender_counts = df['gender'].value_counts()
        st.plotly_chart(plot_bar(gender_counts, "Gender Distribution", "#1f77b4"), use_container_width=True)

        country_counts = df['country'].value_counts()
        st.plotly_chart(plot_bar(country_counts, "Countries With Registered Users", "#ff7f0e"), use_container_width=True)

        age_group_counts = df['age_group'].value_counts().sort_index()
        st.plotly_chart(plot_bar(age_group_counts, "Age Distribution (18+)", "#2ca02c"), use_container_width=True)

        st.subheader("💼 Ladder Use Option")
        ladder_counts = df['ladder_use_option'].value_counts()
        st.plotly_chart(plot_bar(ladder_counts, "Ladder Use Options", "#9467bd"), use_container_width=True)

        st.subheader("🏷️ Status")
        status_counts = df['status'].value_counts()
        st.plotly_chart(plot_bar(status_counts, "User Status (Verification)", "#8c564b"), use_container_width=True)

        st.subheader("🧑 Employment Status")
        emp_counts = df['employment_status'].value_counts()
        st.plotly_chart(plot_bar(emp_counts, "Employment Status", "#e377c2"), use_container_width=True)
