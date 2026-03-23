import streamlit as st
import pymysql
import pandas as pd
from datetime import date, datetime
import os
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "charset": "utf8mb4",
}

# Login credentials  (email → {password, display_name})
USERS = {
    "admin@ninjacart.com":                {"password": "Admin@123", "name": "Admin"},
    "mallikarjun.m@ninjacart.com":        {"password": "123456",    "name": "Mallikarjun M"},
    "ravikantbiradar872@ninjacart.com":   {"password": "123456",    "name": "Ravi Kant"},
    "naveenarumugam@ninjacart.com":       {"password": "123456",    "name": "Naveen Arumugam"},
}

# Dropdown static options
CUSTOMER_OPTIONS    = ["Walk-in Customer", "Retail Shop", "Hotel / Restaurant",
                       "Institution", "Canteen", "Other"]
CUSTOMER_NATURE     = ["PG","Horeca","PushCart","Others"]
SALE_TYPE_OPTIONS   = ["DP Sales", "Line Sales", "Walk-in Sales","Stock Not Received"]
PAYMENT_TYPE_OPTIONS = ["Cash", "UPI", "Bank Transfer", "Cheque", "Credit"]
CREDIT_DURATION_OPT = ["0 Days", "1 Days", "2 Days", "3 Days", ">3 Days"]

# ─────────────────────────────────────────────
# DATABASE HELPERS
# ─────────────────────────────────────────────
def get_connection():
    # Check that secrets/env vars are loaded
    if not DB_CONFIG["host"] or not DB_CONFIG["password"]:
        st.error("❌ Database credentials not found. Please add secrets in Streamlit Cloud: Settings → Secrets.")
        st.stop()
    try:
        return pymysql.connect(**DB_CONFIG)
    except pymysql.err.OperationalError as e:
        err_code = e.args[0]
        if err_code == 2003:
            st.error(
                f"❌ Cannot connect to database at `{DB_CONFIG['host']}:{DB_CONFIG['port']}`. "
                "The server may be blocking Streamlit Cloud IPs. "
                "Please whitelist Streamlit Cloud on your DB firewall."
            )
        elif err_code == 1045:
            st.error("❌ Database access denied. Check DB_USER and DB_PASSWORD in Secrets.")
        else:
            st.error(f"❌ Database connection error: {e}")
        st.stop()


def run_query(sql, params=None):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
        return pd.DataFrame(rows, columns=cols)
    finally:
        conn.close()


def run_write(sql, params=None):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
    finally:
        conn.close()


# ─────────────────────────────────────────────
# DATA FETCH FUNCTIONS
# ─────────────────────────────────────────────
@st.cache_data(ttl=60)
def get_delivery_dates():
    df = run_query(
        "SELECT DISTINCT DeliveryDate FROM FnV_Adhoc_Base "
        "ORDER BY DeliveryDate DESC"
    )
    return [r for r in df["DeliveryDate"]]


@st.cache_data(ttl=60)
def get_facilities(delivery_date):
    df = run_query(
        "SELECT DISTINCT Facility FROM FnV_Adhoc_Base "
        "WHERE DeliveryDate = %s AND Facility IS NOT NULL "
        "ORDER BY Facility",
        params=(delivery_date,),
    )
    return df["Facility"].tolist()


@st.cache_data(ttl=60)
def get_skus(delivery_date, facility):
    df = run_query(
        "SELECT DISTINCT Sku FROM FnV_Adhoc_Base "
        "WHERE DeliveryDate = %s AND Facility = %s AND Sku IS NOT NULL "
        "ORDER BY Sku",
        params=(delivery_date, facility),
    )
    return df["Sku"].tolist()


def get_base_row(delivery_date, facility, sku):
    """Returns (ReturnKg, ReturnValue) from FnV_Adhoc_Base."""
    df = run_query(
        "SELECT ReturnKg, ReturnValue FROM FnV_Adhoc_Base "
        "WHERE DeliveryDate = %s AND Facility = %s AND Sku = %s "
        "LIMIT 1",
        params=(delivery_date, facility, sku),
    )
    if df.empty:
        return None, None
    return float(df["ReturnKg"].iloc[0] or 0), float(df["ReturnValue"].iloc[0] or 0)


def get_already_liquidated(delivery_date, facility, sku):
    """Sum of LiqudationKg already recorded in FnV_Adhoc_Sale."""
    df = run_query(
        "SELECT COALESCE(SUM(LiqudationKg), 0) AS used_kg "
        "FROM FnV_Adhoc_Sale "
        "WHERE DeliveryDate = %s AND Facility = %s AND Sku = %s",
        params=(delivery_date, facility, sku),
    )
    return float(df["used_kg"].iloc[0])


@st.cache_data(ttl=60)
def get_customers():
    df = run_query(
        "SELECT DISTINCT Customer FROM FnV_Adhoc_Sale "
        "WHERE Customer IS NOT NULL AND Customer != '' "
        "ORDER BY Customer"
    )
    return df["Customer"].tolist()


def get_sale_records(delivery_date=None):
    where = "WHERE 1=1"
    params = []
    if delivery_date:
        where += " AND DeliveryDate = %s"
        params.append(delivery_date)
    df = run_query(
        f"SELECT * FROM FnV_Adhoc_Sale {where} ORDER BY Id DESC",
        params=params if params else None,
    )
    return df


# ─────────────────────────────────────────────
# LOGIN PAGE
# ─────────────────────────────────────────────
def show_login():
    st.set_page_config(page_title="Adhoc Sale — Login", page_icon="🥦", layout="centered")
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("## 🥦 FnV Adhoc Sale Portal")
        st.markdown("##### Distribution Partner Login")
        st.divider()
        username = st.text_input("Mail ID", placeholder="Enter your email address")
        password = st.text_input("Password", type="password", placeholder="Enter your password")
        if st.button("Login", use_container_width=True, type="primary"):
            if username in USERS and USERS[username]["password"] == password:
                st.session_state["logged_in"] = True
                st.session_state["username"] = username
                st.session_state["display_name"] = USERS[username]["name"]
                st.rerun()
            else:
                st.error("Invalid username or password.")


# ─────────────────────────────────────────────
# MAIN APP
# ─────────────────────────────────────────────
def show_app():
    st.set_page_config(page_title="FnV Adhoc Sale", page_icon="🥦", layout="wide")

    # Sidebar
    with st.sidebar:
        st.markdown(f"### 👤 {st.session_state['display_name']}")
        st.divider()
        page = st.radio("Navigation", ["📋 Record Sale", "📊 View Records"])
        st.divider()
        if st.button("🚪 Logout"):
            st.session_state.clear()
            st.rerun()

    if page == "📋 Record Sale":
        show_record_sale()
    else:
        show_view_records()


# ─────────────────────────────────────────────
# RECORD SALE PAGE
# ─────────────────────────────────────────────
def show_record_sale():
    st.title("📋 Record Adhoc Liquidation Sale")
    st.markdown("Select the Facility and SKU to view available return stock, then record the liquidation entry.")
    st.divider()

    # ── Delivery Date ──
    delivery_dates = get_delivery_dates()
    if not delivery_dates:
        st.warning("No delivery data found in FnV_Adhoc_Base.")
        return
    delivery_date = st.selectbox(
        "📅 Delivery Date",
        delivery_dates,
        format_func=lambda d: d.strftime("%d %b %Y") if hasattr(d, "strftime") else str(d),
    )

    # ── Facility ──
    facilities = get_facilities(delivery_date)
    if not facilities:
        st.warning("No facilities found for the selected delivery date.")
        return
    facility = st.selectbox("🏭 Facility", facilities)

    # ── SKU ──
    skus = get_skus(delivery_date, facility)
    if not skus:
        st.warning("No SKUs found for the selected facility and date.")
        return
    sku = st.selectbox("🛒 SKU", skus)

    # ── Sale Date ──
    sale_date = st.date_input("📅 Sale Date", value=date.today(), min_value=delivery_date)

    # ── Customer ──
    customers = get_customers()
    customer_options = (customers or []) + ["➕ Add New Customer"]
    selected_customer = st.selectbox("👤 Customer", customer_options)
    if selected_customer == "➕ Add New Customer":
        customer = st.text_input("Enter New Customer Name", placeholder="Type customer name here")
        if not customer.strip():
            st.warning("Please enter a customer name.")
            return
    else:
        customer = selected_customer

    # ── Customer Nature & Sale Type ──
    col1, col2 = st.columns(2)
    with col1:
        customer_nature = st.selectbox("🏷️ Customer Nature", CUSTOMER_NATURE)
    with col2:
        sale_type = st.selectbox("📦 Sale Type", SALE_TYPE_OPTIONS)

    # ── Return stock availability ──
    base_kg, base_value = get_base_row(delivery_date, facility, sku)
    already_used_kg = get_already_liquidated(delivery_date, facility, sku)

    if base_kg is None:
        st.error("Could not fetch base data for selected SKU.")
        return

    available_kg    = max(0.0, base_kg - already_used_kg)
    per_kg_rate     = (base_value / base_kg) if base_kg > 0 else 0
    available_value = round(available_kg * per_kg_rate, 2)

    st.divider()
    st.markdown("#### 📦 Return Stock Availability")
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Return Qty (Kg)", f"{base_kg:.2f}")
    m2.metric("Already Liquidated (Kg)", f"{already_used_kg:.2f}")
    m3.metric("Available Qty (Kg)", f"{available_kg:.2f}",
              delta=f"-{already_used_kg:.2f}" if already_used_kg > 0 else None,
              delta_color="inverse")
    m4.metric("Available Value (₹)", f"₹{available_value:,.2f}")

    if available_kg <= 0:
        st.error("⚠️ No stock available for this SKU. Entire quantity has already been liquidated.")
        return

    # ── Auto-populated return fields ──
    col1, col2 = st.columns(2)
    with col1:
        st.text_input("Return Qty (Kg) — Available", value=f"{available_kg:.2f}", disabled=True)
    with col2:
        st.text_input("Return Value (₹) — Available", value=f"₹{available_value:,.2f}", disabled=True)

    # ── Liquidation entry ──
    st.divider()
    st.markdown("#### ✏️ Liquidation Entry")
    col1, col2 = st.columns(2)
    with col1:
        liq_kg_input = st.text_input(
            "Liquidation Qty (Kg) *",
            placeholder=f"Max: {available_kg:.2f} Kg",
        )
    with col2:
        # Pre-fill liquidation value based on entered kg
        try:
            liq_kg_val = float(liq_kg_input) if liq_kg_input.strip() else 0.0
            prefill_value = round(liq_kg_val * per_kg_rate, 2) if liq_kg_val > 0 else 0.0
        except ValueError:
            liq_kg_val = 0.0
            prefill_value = 0.0

        liq_value_input = st.text_input(
            "Liquidation Value (₹) *",
            value=str(prefill_value) if prefill_value > 0 else "",
            placeholder="Auto-filled (editable)",
        )

    # ── Payment info ──
    col1, col2 = st.columns(2)
    with col1:
        payment_type = st.selectbox("💳 Payment Type", PAYMENT_TYPE_OPTIONS)
    with col2:
        if payment_type == "Credit":
            credit_duration = st.selectbox("⏳ Credit Duration", CREDIT_DURATION_OPT)
        else:
            credit_duration = "0 Days"
            st.empty()

    # ── Submit ──
    st.divider()
    if st.button("✅ Submit Entry", type="primary", use_container_width=True):
        errors = []

        # Validate liquidation kg
        try:
            liq_kg = float(liq_kg_input)
            if liq_kg <= 0:
                errors.append("Liquidation Qty must be greater than 0.")
            elif liq_kg > available_kg:
                errors.append(f"Liquidation Qty ({liq_kg:.2f} Kg) exceeds available stock ({available_kg:.2f} Kg).")
        except (ValueError, AttributeError):
            errors.append("Liquidation Qty must be a valid number.")
            liq_kg = None

        # Validate liquidation value
        try:
            liq_value = float(liq_value_input)
            if liq_value < 0:
                errors.append("Liquidation Value cannot be negative.")
        except (ValueError, AttributeError):
            errors.append("Liquidation Value must be a valid number.")
            liq_value = None

        if errors:
            for e in errors:
                st.error(e)
        else:
            try:
                run_write(
                    """INSERT INTO FnV_Adhoc_Sale
                       (DeliveryDate, SaleDate, Customer, CustomerNature, SaleType,
                        Facility, Sku, ReturnKg, ReturnValue, LiqudationKg,
                        LiqudationValue, PaymentType, CreditDuration,
                        CreatedBy, CreatedAt, UpdatedBy, UpdatedAt)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    params=(
                        delivery_date, sale_date, customer, customer_nature, sale_type,
                        facility, sku,
                        round(available_kg, 4), round(available_value, 4),
                        round(liq_kg, 4), round(liq_value, 4),
                        payment_type, credit_duration,
                        st.session_state["username"], datetime.now(),
                        st.session_state["username"], datetime.now(),
                    ),
                )
                st.success(
                    f"✅ Entry recorded successfully! "
                    f"{liq_kg:.2f} Kg of **{sku}** liquidated for ₹{liq_value:,.2f}."
                )
                # Bust cache so available qty updates immediately
                get_facilities.clear()
                get_skus.clear()
                get_delivery_dates.clear()
                st.balloons()
            except Exception as ex:
                st.error(f"Database error: {ex}")


# ─────────────────────────────────────────────
# VIEW RECORDS PAGE
# ─────────────────────────────────────────────
def show_view_records():
    st.title("📊 Liquidation Sale Records")
    st.divider()

    col1, col2 = st.columns([2, 1])
    with col1:
        delivery_dates = get_delivery_dates()
        filter_date = st.selectbox(
            "Filter by Delivery Date",
            [None] + delivery_dates,
            format_func=lambda d: "All Dates" if d is None else (
                d.strftime("%d %b %Y") if hasattr(d, "strftime") else str(d)
            ),
        )
    with col2:
        st.markdown("")
        st.markdown("")
        if st.button("🔄 Refresh"):
            st.rerun()

    df = get_sale_records(filter_date)

    if df.empty:
        st.info("No sale records found.")
        return

    # Summary metrics
    m1, m2, m3 = st.columns(3)
    m1.metric("Total Records", len(df))
    m2.metric("Total Liquidation Qty", f"{df['LiqudationKg'].sum():.2f} Kg")
    m3.metric("Total Liquidation Value", f"₹{df['LiqudationValue'].sum():,.2f}")

    st.divider()

    # Rename for display
    display_df = df.rename(columns={
        "Id": "ID",
        "DeliveryDate": "Delivery Date",
        "SaleDate": "Sale Date",
        "LiqudationKg": "Liq. Qty (Kg)",
        "LiqudationValue": "Liq. Value (₹)",
        "ReturnKg": "Avail. Qty (Kg)",
        "ReturnValue": "Avail. Value (₹)",
        "PaymentType": "Payment",
        "CreditDuration": "Credit",
        "CustomerNature": "Cust. Nature",
        "SaleType": "Sale Type",
    })
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    # Download
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Download CSV", data=csv, file_name="adhoc_sale_records.csv", mime="text/csv")


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__" or True:
    if not st.session_state.get("logged_in"):
        show_login()
    else:
        show_app()
