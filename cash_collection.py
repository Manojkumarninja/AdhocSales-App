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

TABLE = "FnV_CashCollection_Chn"

USERS = {
    "admin@ninjacart.com":          {"password": "Admin@123", "name": "Admin"},
    "abishanbarasan@ninjacart.com": {"password": "123456",    "name": "Abishan Barasan"},
    "pradeep.es@ninjacart.com":     {"password": "123456",    "name": "Pradeep ES"},
}

PAYMENT_STATUS_OPTIONS      = ["Paid", "Partially Paid", "Fully Paid"]
PAYMENT_MODE_OPTIONS        = ["Cash", "UPI", "Bank Transfer", "Cheque", "Credit"]
CREDIT_DURATION_OPT         = ["0 Days", "1 Days", "2 Days", "3 Days", ">3 Days"]
COLLECTION_TIME_WINDOW_OPT  = ["While Delivery", "Before 5 PM", "5 PM to 8:30 PM", "After 8:30 PM", "Next Day"]

# ─────────────────────────────────────────────
# DATABASE HELPERS
# ─────────────────────────────────────────────
def get_connection():
    cfg = {**DB_CONFIG}
    if not cfg.get("host"):
        cfg = {
            "host": st.secrets["DB_HOST"],
            "port": int(st.secrets["DB_PORT"]),
            "user": st.secrets["DB_USER"],
            "password": st.secrets["DB_PASSWORD"],
            "database": st.secrets["DB_NAME"],
            "charset": "utf8mb4",
        }
    return pymysql.connect(**cfg)


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
    df = run_query(f"SELECT DISTINCT DeliveryDate FROM {TABLE} ORDER BY DeliveryDate DESC")
    return df["DeliveryDate"].tolist()


@st.cache_data(ttl=60)
def get_facilities(delivery_date):
    df = run_query(
        f"SELECT DISTINCT FacilityId, Facility FROM {TABLE} "
        f"WHERE DeliveryDate = %s ORDER BY Facility",
        params=(delivery_date,),
    )
    return df


def get_customers(delivery_date, facility_id):
    df = run_query(
        f"SELECT Id, CustomerId, Customer, SaleOrderId, OrderKg, BilledKg, "
        f"FulfilledKg, ReturnKg, TotalInvoiceValue, PaymentStatus, OutstandingAmount, "
        f"PaymentMode, CreditDuration, CollectionTimeWindow "
        f"FROM {TABLE} "
        f"WHERE DeliveryDate = %s AND FacilityId = %s "
        f"ORDER BY Customer",
        params=(delivery_date, facility_id),
    )
    return df


# ─────────────────────────────────────────────
# LOGIN PAGE
# ─────────────────────────────────────────────
def show_login():
    st.set_page_config(page_title="Cash Collection", page_icon="💰", layout="centered")
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("## 💰 Cash Collection Portal")
        st.markdown("##### Ops Executive Login")
        st.divider()
        email = st.text_input("Mail ID", placeholder="Enter your email")
        password = st.text_input("Password", type="password", placeholder="Enter your password")
        if st.button("Login", use_container_width=True, type="primary"):
            if email in USERS and USERS[email]["password"] == password:
                st.session_state["logged_in"] = True
                st.session_state["username"] = email
                st.session_state["display_name"] = USERS[email]["name"]
                st.rerun()
            else:
                st.error("Invalid email or password.")


# ─────────────────────────────────────────────
# MAIN APP
# ─────────────────────────────────────────────
def show_app():
    st.set_page_config(page_title="Cash Collection", page_icon="💰", layout="wide")

    with st.sidebar:
        st.markdown(f"### 👤 {st.session_state['display_name']}")
        st.divider()
        page = st.radio("Navigation", ["📝 Update Payment", "📊 View Records"])
        st.divider()
        if st.button("🚪 Logout"):
            st.session_state.clear()
            st.rerun()

    if page == "📝 Update Payment":
        show_update_payment()
    else:
        show_view_records()


# ─────────────────────────────────────────────
# UPDATE PAYMENT PAGE
# ─────────────────────────────────────────────
def show_update_payment():
    st.title("📝 Update Payment Status")
    st.markdown("Select Delivery Date and Facility to view customers, then update their payment details.")
    st.divider()

    # ── Filters ──
    col1, col2 = st.columns(2)
    with col1:
        delivery_dates = get_delivery_dates()
        if not delivery_dates:
            st.warning("No data found in the table.")
            return
        delivery_date = st.selectbox(
            "📅 Delivery Date",
            delivery_dates,
            format_func=lambda d: d.strftime("%d %b %Y") if hasattr(d, "strftime") else str(d),
        )

    with col2:
        facilities_df = get_facilities(delivery_date)
        if facilities_df.empty:
            st.warning("No facilities found for selected date.")
            return

        # Build display options: "FacilityId — FacilityName"
        facility_options = {
            row["FacilityId"]: f"{row['FacilityId']} — {row['Facility']}"
            for _, row in facilities_df.iterrows()
        }
        selected_facility_id = st.selectbox(
            "🏭 Facility",
            options=list(facility_options.keys()),
            format_func=lambda fid: facility_options[fid],
        )
        selected_facility_name = facilities_df[
            facilities_df["FacilityId"] == selected_facility_id
        ]["Facility"].iloc[0]

    # ── Customer List ──
    customers_df = get_customers(delivery_date, selected_facility_id)
    if customers_df.empty:
        st.warning("No customers found for the selected facility and date.")
        return

    total = len(customers_df)
    filled = customers_df["PaymentStatus"].notna().sum()
    pending = total - filled

    st.divider()
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Customers", total)
    m2.metric("Updated", int(filled))
    m3.metric("Pending", int(pending))
    m4.metric("Total Invoice Value", f"₹{customers_df['TotalInvoiceValue'].sum():,.2f}")

    st.divider()

    # ── Customer Selector ──
    customer_options = {
        row["Id"]: f"{int(row['CustomerId'])} — {row['Customer']}"
        for _, row in customers_df.iterrows()
    }
    selected_row_id = st.selectbox(
        "👤 Select Customer (ID — Name)",
        options=list(customer_options.keys()),
        format_func=lambda rid: customer_options[rid],
    )

    selected = customers_df[customers_df["Id"] == selected_row_id].iloc[0]

    # ── Order Details (read-only) ──
    st.markdown("#### 📦 Order Details")
    d1, d2, d3, d4, d5 = st.columns(5)
    d1.metric("Order Kg",     f"{selected['OrderKg']:.2f}")
    d2.metric("Billed Kg",    f"{selected['BilledKg']:.2f}")
    d3.metric("Fulfilled Kg", f"{selected['FulfilledKg']:.2f}")
    d4.metric("Return Kg",    f"{selected['ReturnKg']:.2f}")
    d5.metric("Invoice Value",f"₹{selected['TotalInvoiceValue']:,.2f}")

    # ── Payment Form ──
    st.divider()
    st.markdown("#### 💳 Payment Details")

    col1, col2 = st.columns(2)
    with col1:
        current_status = selected["PaymentStatus"] if pd.notna(selected["PaymentStatus"]) else PAYMENT_STATUS_OPTIONS[0]
        status_idx = PAYMENT_STATUS_OPTIONS.index(current_status) if current_status in PAYMENT_STATUS_OPTIONS else 0
        payment_status = st.selectbox("Payment Status", PAYMENT_STATUS_OPTIONS, index=status_idx)

    with col2:
        current_mode = selected["PaymentMode"] if pd.notna(selected["PaymentMode"]) else PAYMENT_MODE_OPTIONS[0]
        mode_idx = PAYMENT_MODE_OPTIONS.index(current_mode) if current_mode in PAYMENT_MODE_OPTIONS else 0
        payment_mode = st.selectbox("Payment Mode", PAYMENT_MODE_OPTIONS, index=mode_idx)

    col1, col2 = st.columns(2)
    with col1:
        current_outstanding = str(selected["OutstandingAmount"]) if pd.notna(selected["OutstandingAmount"]) else ""
        outstanding_input = st.text_input(
            "Outstanding Amount (₹)",
            value=current_outstanding,
            placeholder="Enter outstanding amount",
        )

    with col2:
        if payment_mode == "Credit":
            current_credit = selected["CreditDuration"] if pd.notna(selected["CreditDuration"]) else CREDIT_DURATION_OPT[0]
            credit_idx = CREDIT_DURATION_OPT.index(current_credit) if current_credit in CREDIT_DURATION_OPT else 0
            credit_duration = st.selectbox("⏳ Credit Duration", CREDIT_DURATION_OPT, index=credit_idx)
        else:
            credit_duration = None

    current_ctw = selected["CollectionTimeWindow"] if pd.notna(selected["CollectionTimeWindow"]) else COLLECTION_TIME_WINDOW_OPT[0]
    ctw_idx = COLLECTION_TIME_WINDOW_OPT.index(current_ctw) if current_ctw in COLLECTION_TIME_WINDOW_OPT else 0
    collection_time_window = st.selectbox("🕐 Collection Time Window", COLLECTION_TIME_WINDOW_OPT, index=ctw_idx)

    # ── Submit ──
    st.divider()
    if st.button("✅ Save Payment Details", type="primary", use_container_width=True):
        errors = []
        try:
            outstanding = float(outstanding_input) if outstanding_input.strip() else None
            if outstanding is not None and outstanding < 0:
                errors.append("Outstanding amount cannot be negative.")
        except ValueError:
            errors.append("Outstanding amount must be a valid number.")
            outstanding = None

        if errors:
            for e in errors:
                st.error(e)
        else:
            try:
                run_write(
                    f"UPDATE {TABLE} SET PaymentStatus=%s, OutstandingAmount=%s, "
                    f"PaymentMode=%s, CreditDuration=%s, CollectionTimeWindow=%s WHERE Id=%s",
                    params=(payment_status, outstanding, payment_mode, credit_duration, collection_time_window, selected_row_id),
                )
                st.success(
                    f"✅ Payment updated for **{selected['Customer']}** — "
                    f"{payment_status} | {payment_mode}"
                    + (f" | Credit: {credit_duration}" if credit_duration else "")
                )
                get_delivery_dates.clear()
                get_facilities.clear()
                st.rerun()
            except Exception as ex:
                st.error(f"Database error: {ex}")

    # ── Customer Summary Table ──
    st.divider()
    st.markdown("#### 📋 All Customers — This Facility")
    display_df = customers_df[[
        "CustomerId", "Customer", "SaleOrderId", "TotalInvoiceValue",
        "PaymentStatus", "OutstandingAmount", "PaymentMode", "CreditDuration"
    ]].copy()
    display_df.columns = [
        "Customer ID", "Customer", "Order ID", "Invoice (₹)",
        "Payment Status", "Outstanding (₹)", "Payment Mode", "Credit Duration"
    ]
    st.dataframe(
        display_df.style.apply(
            lambda row: ["background-color: #d4edda" if row["Payment Status"] == "Paid"
                         else "background-color: #fff3cd" if row["Payment Status"] in ["Partial", "Credit"]
                         else "background-color: #f8d7da" if pd.isna(row["Payment Status"])
                         else "" for _ in row],
            axis=1,
        ),
        use_container_width=True,
        hide_index=True,
    )


# ─────────────────────────────────────────────
# VIEW RECORDS PAGE
# ─────────────────────────────────────────────
def show_view_records():
    st.title("📊 Cash Collection Records")
    st.divider()

    col1, col2, col3 = st.columns([2, 2, 1])
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
        if filter_date:
            facilities_df = get_facilities(filter_date)
            facility_options = {None: "All Facilities"}
            facility_options.update({
                row["FacilityId"]: row["Facility"]
                for _, row in facilities_df.iterrows()
            })
            filter_facility = st.selectbox(
                "Filter by Facility",
                options=list(facility_options.keys()),
                format_func=lambda k: facility_options[k],
            )
        else:
            filter_facility = None
            st.selectbox("Filter by Facility", ["All Facilities"], disabled=True)
    with col3:
        st.markdown("")
        st.markdown("")
        if st.button("🔄 Refresh"):
            get_delivery_dates.clear()
            get_facilities.clear()
            st.rerun()

    # Build query
    where_clauses = ["1=1"]
    params = []
    if filter_date:
        where_clauses.append("DeliveryDate = %s")
        params.append(filter_date)
    if filter_facility:
        where_clauses.append("FacilityId = %s")
        params.append(filter_facility)

    df = run_query(
        f"SELECT * FROM {TABLE} WHERE {' AND '.join(where_clauses)} ORDER BY Id DESC",
        params=params if params else None,
    )

    if df.empty:
        st.info("No records found.")
        return

    # Summary metrics
    total_invoice = df["TotalInvoiceValue"].sum()
    total_outstanding = df["OutstandingAmount"].sum()
    collected = total_invoice - total_outstanding if total_outstanding else total_invoice
    paid_count = df[df["PaymentStatus"] == "Paid"].shape[0]

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Total Orders",       len(df))
    m2.metric("Paid",               paid_count)
    m3.metric("Pending Update",     df["PaymentStatus"].isna().sum())
    m4.metric("Total Invoice (₹)",  f"₹{total_invoice:,.2f}")
    m5.metric("Outstanding (₹)",    f"₹{total_outstanding:,.2f}")

    st.divider()

    display_df = df.rename(columns={
        "Id": "ID", "DeliveryDate": "Delivery Date", "FacilityId": "Facility ID",
        "CustomerId": "Customer ID", "SaleOrderId": "Order ID",
        "OrderKg": "Order Kg", "BilledKg": "Billed Kg",
        "FulfilledKg": "Fulfilled Kg", "ReturnKg": "Return Kg",
        "TotalInvoiceValue": "Invoice (₹)", "PaymentStatus": "Payment Status",
        "OutstandingAmount": "Outstanding (₹)", "PaymentMode": "Payment Mode",
        "CreditDuration": "Credit Duration",
    })
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Download CSV", data=csv, file_name="cash_collection.csv", mime="text/csv")


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__" or True:
    if not st.session_state.get("logged_in"):
        show_login()
    else:
        show_app()
