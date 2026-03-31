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

# Login credentials  (email → {password, display_name, cities})
# cities: list of allowed cities. Use ["Bengaluru", "Chennai"] for all-access (admin).
USERS = {
    "admin@ninjacart.com":                {"password": "Admin@123", "name": "Admin",            "cities": ["Bengaluru", "Chennai"]},
    "mallikarjun.m@ninjacart.com":        {"password": "123456",    "name": "Mallikarjun M",    "cities": ["Bengaluru"]},
    "ravikantbiradar872@ninjacart.com":   {"password": "123456",    "name": "Ravi Kant",        "cities": ["Bengaluru"]},
    "naveenarumugam@ninjacart.com":       {"password": "123456",    "name": "Naveen Arumugam",  "cities": ["Bengaluru"]},
    "abishanbarasan@ninjacart.com":       {"password": "123456",    "name": "Abishan Barasan",  "cities": ["Bengaluru"]},
}

# Dropdown static options
CUSTOMER_OPTIONS    = ["Walk-in Customer", "Retail Shop", "Hotel / Restaurant",
                       "Institution", "Canteen", "Other"]
CUSTOMER_NATURE     = ["PG","Horeca","PushCart","General Trade","Existing GT Customer","Others"]
SALE_TYPE_OPTIONS   = ["DP Sales", "Line Sales", "Walk-in Sales","Stock Not Received","Sku Damage"]
PAYMENT_TYPE_OPTIONS = ["Cash", "UPI", "Bank Transfer", "Cheque", "Credit"]
CREDIT_DURATION_OPT = ["0 Days", "1 Days", "2 Days", "3 Days", ">3 Days"]
REASON_OPTIONS      = ["Quality Issue", "Weight Shortage", "Damages", "Item Missing",
                       "Late Delivery", "Price Issue", "Random Returns","Wrongly punched", "Others"]

# City → table mapping
CITY_TABLES = {
    "Bengaluru": {"base": "FnV_Adhoc_Base",     "sale": "FnV_Adhoc_Sale"},
    "Chennai":   {"base": "FnV_Adhoc_Base_Chn", "sale": "FnV_Adhoc_Sale_Chn"},
}

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
def get_delivery_dates(base_table):
    df = run_query(
        f"SELECT DISTINCT DeliveryDate FROM {base_table} ORDER BY DeliveryDate DESC"
    )
    return [r for r in df["DeliveryDate"]]


@st.cache_data(ttl=60)
def get_facilities(delivery_date, base_table):
    df = run_query(
        f"SELECT DISTINCT Facility FROM {base_table} "
        "WHERE DeliveryDate = %s AND Facility IS NOT NULL ORDER BY Facility",
        params=(delivery_date,),
    )
    return df["Facility"].tolist()


@st.cache_data(ttl=60)
def get_skus(delivery_date, facility, base_table):
    df = run_query(
        f"SELECT DISTINCT Sku FROM {base_table} "
        "WHERE DeliveryDate = %s AND Facility = %s AND Sku IS NOT NULL ORDER BY Sku",
        params=(delivery_date, facility),
    )
    return df["Sku"].tolist()


def get_base_row(delivery_date, facility, sku, base_table):
    df = run_query(
        f"SELECT ReturnKg, ReturnValue FROM {base_table} "
        "WHERE DeliveryDate = %s AND Facility = %s AND Sku = %s LIMIT 1",
        params=(delivery_date, facility, sku),
    )
    if df.empty:
        return None, None
    return float(df["ReturnKg"].iloc[0] or 0), float(df["ReturnValue"].iloc[0] or 0)


def get_already_liquidated(delivery_date, facility, sku, sale_table):
    df = run_query(
        f"SELECT COALESCE(SUM(LiqudationKg), 0) AS used_kg FROM {sale_table} "
        "WHERE DeliveryDate = %s AND Facility = %s AND Sku = %s",
        params=(delivery_date, facility, sku),
    )
    return float(df["used_kg"].iloc[0])


@st.cache_data(ttl=60)
def get_customers(sale_table):
    df = run_query(
        f"SELECT DISTINCT Customer FROM {sale_table} "
        "WHERE Customer IS NOT NULL AND Customer != '' ORDER BY Customer"
    )
    return df["Customer"].tolist()


def get_sale_records(sale_table, delivery_date=None):
    where = "WHERE 1=1"
    params = []
    if delivery_date:
        where += " AND DeliveryDate = %s"
        params.append(delivery_date)
    df = run_query(
        f"SELECT * FROM {sale_table} {where} ORDER BY Id DESC",
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
                st.session_state["allowed_cities"] = USERS[username]["cities"]
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
        allowed_cities = st.session_state.get("allowed_cities", [])
        if len(allowed_cities) == 1:
            city = allowed_cities[0]
            st.markdown(f"🏙️ **City:** {city}")
        else:
            city = st.selectbox("🏙️ City", allowed_cities)
        base_table = CITY_TABLES[city]["base"]
        sale_table = CITY_TABLES[city]["sale"]
        st.divider()
        page = st.radio("Navigation", ["📋 Record Sale", "📊 View Records"])
        st.divider()
        if st.button("🚪 Logout"):
            st.session_state.clear()
            st.rerun()

    if page == "📋 Record Sale":
        show_record_sale(base_table, sale_table)
    else:
        show_view_records(base_table, sale_table)


# ─────────────────────────────────────────────
# RECORD SALE PAGE
# ─────────────────────────────────────────────
def show_record_sale(base_table, sale_table):
    st.title("📋 Record Adhoc Liquidation Sale")
    st.markdown("Select the Facility and SKU to view available return stock, then record the liquidation entry.")
    st.divider()

    # ── Delivery Date ──
    delivery_dates = get_delivery_dates(base_table)
    if not delivery_dates:
        st.warning("No delivery data found.")
        return
    delivery_date = st.selectbox(
        "📅 Delivery Date",
        delivery_dates,
        format_func=lambda d: d.strftime("%d %b %Y") if hasattr(d, "strftime") else str(d),
    )

    # ── Facility ──
    facilities = get_facilities(delivery_date, base_table)
    if not facilities:
        st.warning("No facilities found for the selected delivery date.")
        return
    facility = st.selectbox("🏭 Facility", facilities)

    # ── SKU ──
    skus = get_skus(delivery_date, facility, base_table)
    if not skus:
        st.warning("No SKUs found for the selected facility and date.")
        return
    sku = st.selectbox("🛒 SKU", skus)

    # ── Sale Date ──
    sale_date = st.date_input("📅 Sale Date", value=date.today(), min_value=delivery_date)

    # ── Customer ──
    customers = get_customers(sale_table)
    customer_options = (customers or []) + ["➕ Add New Customer"]
    selected_customer = st.selectbox("👤 Customer", customer_options)
    if selected_customer == "➕ Add New Customer":
        customer = st.text_input("Enter New Customer Name", placeholder="Type customer name here")
        if not customer.strip():
            st.warning("Please enter a customer name.")
            return
    else:
        customer = selected_customer

    # ── Customer Nature, Sale Type & Reason ──
    col1, col2, col3 = st.columns(3)
    with col1:
        customer_nature = st.selectbox("🏷️ Customer Nature", CUSTOMER_NATURE)
    with col2:
        sale_type = st.selectbox("📦 Sale Type", SALE_TYPE_OPTIONS)
    with col3:
        reason = st.selectbox("❓ Reason for Return", REASON_OPTIONS)

    # ── Return stock availability ──
    base_kg, base_value = get_base_row(delivery_date, facility, sku, base_table)
    already_used_kg = get_already_liquidated(delivery_date, facility, sku, sale_table)

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
                    f"""INSERT INTO {sale_table}
                       (DeliveryDate, SaleDate, Customer, CustomerNature, SaleType,
                        Facility, Sku, ReturnKg, ReturnValue, LiqudationKg,
                        LiqudationValue, PaymentType, CreditDuration,
                        CreatedBy, CreatedAt, UpdatedBy, UpdatedAt, Reason)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    params=(
                        delivery_date, sale_date, customer, customer_nature, sale_type,
                        facility, sku,
                        round(available_kg, 4), round(available_value, 4),
                        round(liq_kg, 4), round(liq_value, 4),
                        payment_type, credit_duration,
                        st.session_state["username"], datetime.now(),
                        st.session_state["username"], datetime.now(),
                        reason,
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
def show_view_records(base_table, sale_table):
    st.title("📊 Liquidation Sale Records")
    st.divider()

    # ── Filters ──
    col1, col2, col3, col4 = st.columns([2, 2, 2, 1])

    with col1:
        delivery_dates = get_delivery_dates(base_table)
        filter_date = st.selectbox(
            "📅 Delivery Date",
            [None] + delivery_dates,
            format_func=lambda d: "All Dates" if d is None else (
                d.strftime("%d %b %Y") if hasattr(d, "strftime") else str(d)
            ),
        )

    # Load all records first so we can populate Facility/SKU filters
    df_all = get_sale_records(sale_table, filter_date)

    with col2:
        facility_options = ["All Facilities"] + sorted(df_all["Facility"].dropna().unique().tolist())
        filter_facility = st.selectbox("🏭 Facility", facility_options)

    with col3:
        if filter_facility != "All Facilities":
            sku_options = ["All SKUs"] + sorted(
                df_all[df_all["Facility"] == filter_facility]["Sku"].dropna().unique().tolist()
            )
        else:
            sku_options = ["All SKUs"] + sorted(df_all["Sku"].dropna().unique().tolist())
        filter_sku = st.selectbox("🛒 SKU", sku_options)

    with col4:
        st.markdown("")
        st.markdown("")
        if st.button("🔄 Refresh"):
            get_sale_records.clear() if hasattr(get_sale_records, "clear") else None
            st.rerun()

    # ── Apply filters ──
    df = df_all.copy()
    if filter_facility != "All Facilities":
        df = df[df["Facility"] == filter_facility]
    if filter_sku != "All SKUs":
        df = df[df["Sku"] == filter_sku]

    if df_all.empty:
        st.info("No sale records found.")
        return

    # ── Summary metrics from FnV_Adhoc_Base for selected filters ──
    st.divider()
    st.markdown("#### 📦 Stock Summary")

    # Build base query matching current filters
    base_where = "WHERE 1=1"
    base_params = []
    if filter_date:
        base_where += " AND DeliveryDate = %s"
        base_params.append(filter_date)
    if filter_facility != "All Facilities":
        base_where += " AND Facility = %s"
        base_params.append(filter_facility)
    if filter_sku != "All SKUs":
        base_where += " AND Sku = %s"
        base_params.append(filter_sku)

    df_base = run_query(
        f"SELECT COALESCE(SUM(ReturnKg),0) AS total_return_kg, "
        f"COALESCE(SUM(ReturnValue),0) AS total_return_value "
        f"FROM {base_table} {base_where}",
        params=base_params if base_params else None,
    )
    total_return_kg    = float(df_base["total_return_kg"].iloc[0])
    total_return_value = float(df_base["total_return_value"].iloc[0])
    total_liq_kg       = float(df["LiqudationKg"].sum())
    total_liq_value    = float(df["LiqudationValue"].sum())
    available_kg       = max(0.0, total_return_kg - total_liq_kg)
    available_value    = max(0.0, total_return_value - total_liq_value)

    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("Total Return Qty",    f"{total_return_kg:,.2f} Kg")
    m2.metric("Total Return Value",  f"₹{total_return_value:,.2f}")
    m3.metric("Liquidated Qty",      f"{total_liq_kg:,.2f} Kg")
    m4.metric("Liquidated Value",    f"₹{total_liq_value:,.2f}")
    m5.metric("Available Qty",       f"{available_kg:,.2f} Kg",
              delta=f"-{total_liq_kg:,.2f}" if total_liq_kg > 0 else None, delta_color="inverse")
    m6.metric("Available Value",     f"₹{available_value:,.2f}",
              delta=f"-₹{total_liq_value:,.2f}" if total_liq_value > 0 else None, delta_color="inverse")

    st.divider()

    # ── Records table ──
    st.markdown(f"#### 🗒️ Records ({len(df)})")

    if df.empty:
        st.info("No records match the selected filters.")
    else:
        display_df = df.rename(columns={
            "Id": "ID",
            "DeliveryDate": "Delivery Date",
            "SaleDate": "Sale Date",
            "LiqudationKg": "Liq. Qty (Kg)",
            "LiqudationValue": "Liq. Value (₹)",
            "ReturnKg": "Return Qty (Kg)",
            "ReturnValue": "Return Value (₹)",
            "PaymentType": "Payment",
            "CreditDuration": "Credit",
            "CustomerNature": "Cust. Nature",
            "SaleType": "Sale Type",
        })
        st.dataframe(display_df, use_container_width=True, hide_index=True)

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
