import streamlit as st
import pymysql
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
BASE_TABLE        = "FnV_CashCollection_Base"
TRANSACTION_TABLE = "FnV_CashCollection_TransactionBase"

USERS = {
    "admin@ninjacart.com":          {"password": "Admin@123", "name": "Admin"},
    "abishanbarasan@ninjacart.com": {"password": "123456",    "name": "Abishan Barasan"},
    "pradeep.es@ninjacart.com":     {"password": "123456",    "name": "Pradeep ES"},
}

PAYMENT_MODE_OPTIONS = ["Cash", "UPI", "Net Banking", "Cheque", "Credit"]


# ─────────────────────────────────────────────
# DATABASE HELPERS
# ─────────────────────────────────────────────
def get_connection():
    cfg = {
        "host":     os.getenv("DB_HOST"),
        "port":     int(os.getenv("DB_PORT", 3306)),
        "user":     os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "database": os.getenv("DB_NAME"),
        "charset":  "utf8mb4",
    }
    if not cfg["host"]:
        cfg = {
            "host":     st.secrets["DB_HOST"],
            "port":     int(st.secrets["DB_PORT"]),
            "user":     st.secrets["DB_USER"],
            "password": st.secrets["DB_PASSWORD"],
            "database": st.secrets["DB_NAME"],
            "charset":  "utf8mb4",
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
    df = run_query(f"SELECT DISTINCT DeliveryDate FROM {BASE_TABLE} ORDER BY DeliveryDate DESC")
    return df["DeliveryDate"].tolist()


@st.cache_data(ttl=60)
def get_facilities(delivery_date):
    df = run_query(
        f"SELECT DISTINCT FacilityId, Facility FROM {BASE_TABLE} "
        f"WHERE DeliveryDate = %s ORDER BY Facility",
        params=(delivery_date,),
    )
    return df


@st.cache_data(ttl=60)
def get_drivers(delivery_date, facility_id, mode):
    df = run_query(
        f"SELECT DISTINCT DriverId, Driver FROM {BASE_TABLE} "
        f"WHERE DeliveryDate = %s AND FacilityId = %s AND OrderMode = %s "
        f"AND Driver IS NOT NULL ORDER BY Driver",
        params=(delivery_date, facility_id, mode),
    )
    return df


def get_customers(delivery_date, facility_id, mode, driver_id=None):
    """Returns customers with their pending cash amount."""
    query = f"""
        SELECT
            b.SaleOrderId,
            b.CustomerId,
            b.Customer,
            b.InvoiceAmount,
            b.UPIAmount,
            b.CashAmount,
            COALESCE(SUM(t.AmountPaid), 0)                       AS TotalPaid,
            b.CashAmount - COALESCE(SUM(t.AmountPaid), 0)        AS PendingAmount,
            COALESCE(COUNT(t.TransactionId), 0)                  AS TransactionCount
        FROM {BASE_TABLE} b
        LEFT JOIN {TRANSACTION_TABLE} t ON b.SaleOrderId = t.SaleOrderId
        WHERE b.DeliveryDate = %s AND b.FacilityId = %s AND b.OrderMode = %s
    """
    params = [delivery_date, facility_id, mode]
    if driver_id:
        query += " AND b.DriverId = %s"
        params.append(driver_id)
    query += " GROUP BY b.SaleOrderId, b.CustomerId, b.Customer, b.InvoiceAmount, b.UPIAmount, b.CashAmount"
    return run_query(query, params=params)


def get_transactions_for_order(sale_order_id):
    return run_query(
        f"SELECT * FROM {TRANSACTION_TABLE} WHERE SaleOrderId = %s ORDER BY TransactionId",
        params=(sale_order_id,),
    )


def get_next_transaction_id(sale_order_id):
    df = run_query(
        f"SELECT COALESCE(MAX(TransactionId), 0) + 1 AS next_id "
        f"FROM {TRANSACTION_TABLE} WHERE SaleOrderId = %s",
        params=(sale_order_id,),
    )
    return int(df["next_id"].iloc[0])


# ─────────────────────────────────────────────
# LOGIN PAGE
# ─────────────────────────────────────────────
def show_login():
    st.set_page_config(page_title="Cash Collection", page_icon="💵", layout="centered")
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("## 💵 Cash Collection")
        st.markdown("##### Ops Executive Login")
        st.divider()
        email    = st.text_input("Mail ID", placeholder="Enter your email")
        password = st.text_input("Password", type="password", placeholder="Enter your password")
        if st.button("Login", use_container_width=True, type="primary"):
            if email in USERS and USERS[email]["password"] == password:
                st.session_state["logged_in"]    = True
                st.session_state["username"]     = email
                st.session_state["display_name"] = USERS[email]["name"]
                st.rerun()
            else:
                st.error("Invalid email or password.")


# ─────────────────────────────────────────────
# MAIN APP
# ─────────────────────────────────────────────
def show_app():
    st.set_page_config(page_title="Cash Collection", page_icon="💵", layout="wide")

    with st.sidebar:
        st.markdown(f"### 👤 {st.session_state['display_name']}")
        st.divider()
        page = st.radio("Navigation", ["💳 Record Transaction", "📊 View Records"])
        st.divider()
        if st.button("🚪 Logout"):
            st.session_state.clear()
            st.rerun()

    if page == "💳 Record Transaction":
        show_record_transaction()
    else:
        show_view_records()


# ─────────────────────────────────────────────
# RECORD TRANSACTION PAGE
# ─────────────────────────────────────────────
def show_record_transaction():
    st.title("💳 Record Cash Transaction")
    st.divider()

    # ── Step 1: Filters ──
    col1, col2 = st.columns(2)
    with col1:
        delivery_dates = get_delivery_dates()
        if not delivery_dates:
            st.warning("No data available.")
            return
        delivery_date = st.selectbox(
            "📅 Delivery Date",
            delivery_dates,
            format_func=lambda d: d.strftime("%d %b %Y") if hasattr(d, "strftime") else str(d),
        )
    with col2:
        facilities_df = get_facilities(delivery_date)
        if facilities_df.empty:
            st.warning("No facilities found.")
            return
        facility_map = {row["FacilityId"]: f"{row['FacilityId']} — {row['Facility']}"
                        for _, row in facilities_df.iterrows()}
        facility_id = st.selectbox(
            "🏭 Facility",
            options=list(facility_map.keys()),
            format_func=lambda fid: facility_map[fid],
        )

    col1, col2 = st.columns(2)
    with col1:
        mode = st.selectbox("🚚 Mode", ["Delivery", "Pickup"])
    with col2:
        if mode == "Delivery":
            drivers_df = get_drivers(delivery_date, facility_id, mode)
            if drivers_df.empty:
                st.info("No drivers found for this selection.")
                driver_id = None
            else:
                driver_map = {row["DriverId"]: row["Driver"].strip()
                              for _, row in drivers_df.iterrows()}
                driver_map_with_all = {None: "All Drivers", **driver_map}
                driver_id = st.selectbox(
                    "🧑‍✈️ Driver",
                    options=list(driver_map_with_all.keys()),
                    format_func=lambda did: driver_map_with_all[did],
                )
        else:
            driver_id = None
            st.text_input("🧑‍✈️ Driver", value="N/A (Pickup)", disabled=True)

    # ── Customer List Summary ──
    customers_df = get_customers(delivery_date, facility_id, mode, driver_id)
    if customers_df.empty:
        st.warning("No customers found for this selection.")
        return

    total_customers  = len(customers_df)
    pending_customers = customers_df[customers_df["PendingAmount"] > 0].shape[0]
    fully_paid        = total_customers - pending_customers

    st.divider()
    st.markdown(f"""
    <div style="display:flex;gap:12px;flex-wrap:nowrap;overflow-x:auto;padding:8px 0">
        <div style="flex:1;min-width:100px;background:#f0f2f6;border-radius:8px;padding:12px;text-align:center">
            <div style="font-size:11px;color:#666">Total Customers</div>
            <div style="font-size:22px;font-weight:700">{total_customers}</div>
        </div>
        <div style="flex:1;min-width:100px;background:#fff3cd;border-radius:8px;padding:12px;text-align:center">
            <div style="font-size:11px;color:#666">Pending</div>
            <div style="font-size:22px;font-weight:700;color:#856404">{pending_customers}</div>
        </div>
        <div style="flex:1;min-width:100px;background:#d4edda;border-radius:8px;padding:12px;text-align:center">
            <div style="font-size:11px;color:#666">Fully Paid</div>
            <div style="font-size:22px;font-weight:700;color:#155724">{fully_paid}</div>
        </div>
        <div style="flex:1;min-width:100px;background:#f0f2f6;border-radius:8px;padding:12px;text-align:center">
            <div style="font-size:11px;color:#666">Cash Expected</div>
            <div style="font-size:18px;font-weight:700">₹{customers_df['CashAmount'].sum():,.0f}</div>
        </div>
        <div style="flex:1;min-width:100px;background:#f8d7da;border-radius:8px;padding:12px;text-align:center">
            <div style="font-size:11px;color:#666">Total Pending</div>
            <div style="font-size:18px;font-weight:700;color:#721c24">₹{customers_df['PendingAmount'].sum():,.0f}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Step 2: Customer Selection ──
    st.divider()
    st.markdown("#### 👤 Select Customer")

    # Build options showing pending amount
    pending_df = customers_df[customers_df["PendingAmount"] > 0].copy()
    all_df     = customers_df.copy()

    filter_pending = st.checkbox("Show only pending customers", value=True)
    display_df_sel = pending_df if filter_pending else all_df

    if display_df_sel.empty:
        st.success("✅ All customers are fully paid!")
        return

    customer_options = {
        int(row["SaleOrderId"]): f"{int(row['CustomerId'])} — {row['Customer'].strip()} | "
                                  f"Pending: ₹{row['PendingAmount']:,.2f}"
        for _, row in display_df_sel.iterrows()
    }

    selected_order_id = st.selectbox(
        "Customer (ID — Name | Pending Amount)",
        options=list(customer_options.keys()),
        format_func=lambda sid: customer_options[sid],
    )

    selected = customers_df[customers_df["SaleOrderId"] == selected_order_id].iloc[0]

    # ── Order Details (Read-only) ──
    st.divider()
    st.markdown("#### 📦 Order Details")
    upi_val = f"₹{selected['UPIAmount']:,.0f}" if pd.notna(selected["UPIAmount"]) and selected["UPIAmount"] else "₹0"
    st.markdown(f"""
    <div style="display:flex;gap:10px;flex-wrap:nowrap;overflow-x:auto;padding:8px 0">
        <div style="flex:1;min-width:90px;background:#f0f2f6;border-radius:8px;padding:10px;text-align:center">
            <div style="font-size:10px;color:#666">Invoice (₹)</div>
            <div style="font-size:16px;font-weight:700">₹{selected['InvoiceAmount']:,.0f}</div>
        </div>
        <div style="flex:1;min-width:90px;background:#f0f2f6;border-radius:8px;padding:10px;text-align:center">
            <div style="font-size:10px;color:#666">UPI/Wallet (₹)</div>
            <div style="font-size:16px;font-weight:700">{upi_val}</div>
        </div>
        <div style="flex:1;min-width:90px;background:#f0f2f6;border-radius:8px;padding:10px;text-align:center">
            <div style="font-size:10px;color:#666">Cash (₹)</div>
            <div style="font-size:16px;font-weight:700">₹{selected['CashAmount']:,.0f}</div>
        </div>
        <div style="flex:1;min-width:90px;background:#d4edda;border-radius:8px;padding:10px;text-align:center">
            <div style="font-size:10px;color:#666">Paid So Far (₹)</div>
            <div style="font-size:16px;font-weight:700;color:#155724">₹{selected['TotalPaid']:,.0f}</div>
        </div>
        <div style="flex:1;min-width:90px;background:#f8d7da;border-radius:8px;padding:10px;text-align:center">
            <div style="font-size:10px;color:#666">Pending (₹)</div>
            <div style="font-size:16px;font-weight:700;color:#721c24">₹{selected['PendingAmount']:,.0f}</div>
        </div>
        <div style="flex:1;min-width:70px;background:#f0f2f6;border-radius:8px;padding:10px;text-align:center">
            <div style="font-size:10px;color:#666">Txn #</div>
            <div style="font-size:16px;font-weight:700">{int(selected['TransactionCount']) + 1}</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Existing Transactions ──
    existing_txns = get_transactions_for_order(selected_order_id)
    if not existing_txns.empty:
        with st.expander(f"📋 Previous Transactions ({len(existing_txns)})"):
            st.dataframe(existing_txns[["TransactionId", "AmountPaid", "PaymentMode",
                                        "OustandingAmount", "PaymentStatus"]],
                         use_container_width=True, hide_index=True)

    if selected["PendingAmount"] <= 0:
        st.success("✅ This order is fully paid.")
        return

    # ── Transaction Entry ──
    st.divider()
    st.markdown("#### ✏️ New Transaction")

    next_txn_id = get_next_transaction_id(selected_order_id)
    pending_amt = float(selected["PendingAmount"])

    col1, col2 = st.columns(2)
    with col1:
        st.text_input("Transaction ID", value=str(next_txn_id), disabled=True)
    with col2:
        payment_mode = st.selectbox("💳 Payment Mode", PAYMENT_MODE_OPTIONS)

    amount_paid_input = st.text_input(
        f"Amount Paid (₹) *",
        placeholder=f"Max pending: ₹{pending_amt:,.2f}",
    )

    # Live outstanding preview
    try:
        amount_paid_preview = float(amount_paid_input) if amount_paid_input.strip() else 0.0
        outstanding_preview = max(0.0, pending_amt - amount_paid_preview)
        if amount_paid_preview >= pending_amt:
            status_preview = "Fully Paid"
        elif amount_paid_preview > 0:
            status_preview = "Partially Paid"
        else:
            status_preview = "—"
    except ValueError:
        outstanding_preview = pending_amt
        status_preview = "—"

    col1, col2 = st.columns(2)
    with col1:
        st.text_input("Outstanding After This Transaction (₹)",
                      value=f"₹{outstanding_preview:,.2f}", disabled=True)
    with col2:
        st.text_input("Payment Status", value=status_preview, disabled=True)

    # ── Submit ──
    st.divider()
    if st.button("✅ Save Transaction", type="primary", use_container_width=True):
        errors = []
        try:
            amount_paid = float(amount_paid_input)
            if amount_paid <= 0:
                errors.append("Amount Paid must be greater than 0.")
            elif amount_paid > pending_amt:
                errors.append(f"Amount Paid (₹{amount_paid:,.2f}) exceeds pending amount (₹{pending_amt:,.2f}).")
        except (ValueError, AttributeError):
            errors.append("Amount Paid must be a valid number.")
            amount_paid = None

        if errors:
            for e in errors:
                st.error(e)
        else:
            outstanding = round(max(0.0, pending_amt - amount_paid), 4)
            if amount_paid >= pending_amt:
                pay_status = "Fully Paid"
            else:
                pay_status = "Partially Paid"

            try:
                run_write(
                    f"""INSERT INTO {TRANSACTION_TABLE}
                        (SaleOrderId, TransactionId, AmountPaid, PaymentMode,
                         OustandingAmount, PaymentStatus, CreatedBy, UpdatedBy, CreatedAt, UpdatedAt)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    params=(
                        int(selected_order_id), next_txn_id,
                        round(amount_paid, 4), payment_mode,
                        outstanding, pay_status,
                        st.session_state["username"], st.session_state["username"],
                        datetime.now(), datetime.now(),
                    ),
                )
                st.success(
                    f"✅ Transaction #{next_txn_id} saved — "
                    f"₹{amount_paid:,.2f} via {payment_mode} | "
                    f"Status: **{pay_status}** | Outstanding: ₹{outstanding:,.2f}"
                )
                get_delivery_dates.clear()
                get_facilities.clear()
                get_drivers.clear()
                st.balloons()
                st.rerun()
            except Exception as ex:
                st.error(f"Database error: {ex}")


# ─────────────────────────────────────────────
# VIEW RECORDS PAGE
# ─────────────────────────────────────────────
def show_view_records():
    st.title("📊 Transaction Records")
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
            fac_map = {None: "All Facilities"}
            fac_map.update({row["FacilityId"]: row["Facility"]
                            for _, row in facilities_df.iterrows()})
            filter_facility = st.selectbox(
                "Filter by Facility",
                options=list(fac_map.keys()),
                format_func=lambda k: fac_map[k],
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
            get_drivers.clear()
            st.rerun()

    # Join base + transactions
    where = ["1=1"]
    params = []
    if filter_date:
        where.append("b.DeliveryDate = %s")
        params.append(filter_date)
    if filter_facility:
        where.append("b.FacilityId = %s")
        params.append(filter_facility)

    df = run_query(f"""
        SELECT
            b.DeliveryDate, b.FacilityId, b.Facility, b.CustomerId, b.Customer,
            b.SaleOrderId, b.OrderMode, b.Driver,
            b.InvoiceAmount, b.UPIAmount, b.CashAmount,
            t.TransactionId, t.AmountPaid, t.PaymentMode,
            t.OustandingAmount, t.PaymentStatus,
            t.CreatedBy, t.CreatedAt
        FROM {BASE_TABLE} b
        LEFT JOIN {TRANSACTION_TABLE} t ON b.SaleOrderId = t.SaleOrderId
        WHERE {' AND '.join(where)}
        ORDER BY b.DeliveryDate DESC, b.Facility, b.Customer, t.TransactionId
    """, params=params if params else None)

    if df.empty:
        st.info("No records found.")
        return

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Orders",       df["SaleOrderId"].nunique())
    m2.metric("Total Transactions", df["TransactionId"].notna().sum())
    m3.metric("Total Collected (₹)",f"₹{df['AmountPaid'].sum():,.2f}")
    m4.metric("Total Outstanding (₹)", f"₹{df['OustandingAmount'].sum():,.2f}")

    st.divider()
    st.dataframe(df.rename(columns={
        "DeliveryDate": "Date", "FacilityId": "Fac. ID",
        "CustomerId": "Cust. ID", "SaleOrderId": "Order ID",
        "OrderMode": "Mode", "InvoiceAmount": "Invoice (₹)",
        "UPIAmount": "UPI (₹)", "CashAmount": "Cash (₹)",
        "TransactionId": "Txn #", "AmountPaid": "Paid (₹)",
        "PaymentMode": "Pay Mode", "OustandingAmount": "Outstanding (₹)",
        "PaymentStatus": "Status", "CreatedBy": "Recorded By", "CreatedAt": "Recorded At",
    }), use_container_width=True, hide_index=True)

    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Download CSV", data=csv,
                       file_name="cash_transactions.csv", mime="text/csv")


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__" or True:
    if not st.session_state.get("logged_in"):
        show_login()
    else:
        show_app()
