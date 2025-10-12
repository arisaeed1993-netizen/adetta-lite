# adetta_lite.py
# Ein extrem einfacher, einzelner Streamlit‑Prototyp für Adetta (ohne komplizierte Flask-Routing/HTML).
# Ziele: schneller Start, einfache Bedienung, 1 Datei. DB = SQLite. Mehrbenutzer via Browser im gleichen Netz
# (oder später Cloud-Deploy über Streamlit Community Cloud / VPS). Auth optional (einfacher Pin).
#
# Start unter Windows:
#   python -m venv .venv
#   .venv\Scripts\Activate.ps1
#   pip install streamlit pandas sqlalchemy
#   streamlit run adetta_lite.py
#
# Features:
# - Produkte (Bestand, Mindestbestand, Preis/Karton)
# - Kunden (Zahlungsziel)
# - Lieferungen buchen (Bestand automatisch reduzieren, Rechnung anlegen)
# - Rechnungen & Zahlungen (offen/teilweise/bezahlt)
# - Dashboard (Low-Stock, offene Posten je Kunde, Umsätze letzte 30 Tage)

import os
from datetime import datetime, date, timedelta
from decimal import Decimal

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

DB_URL = os.environ.get("ADETTA_DB", "sqlite:///adetta_lite.db")
ENGINE = create_engine(DB_URL, future=True)

# ------------------ DB INIT ------------------
# ---- DDL: SQLite vs. Postgres kompatibel -----------------
DIALECT = ENGINE.url.get_backend_name()

def make_ddl(dialect: str):
    # SQLite nutzt AUTOINCREMENT, Postgres nutzt SERIAL
    id_col = "SERIAL PRIMARY KEY" if dialect.startswith("postgresql") else "INTEGER PRIMARY KEY AUTOINCREMENT"
    return [
        f"""
        CREATE TABLE IF NOT EXISTS products (
            id {id_col},
            name TEXT NOT NULL,
            sku TEXT UNIQUE NOT NULL,
            price NUMERIC DEFAULT 0,
            stock INTEGER DEFAULT 0,
            min_stock INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS customers (
            id {id_col},
            name TEXT NOT NULL,
            address TEXT,
            contact TEXT,
            terms INTEGER DEFAULT 30,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS deliveries (
            id {id_col},
            ddate DATE NOT NULL,
            customer_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            qty INTEGER NOT NULL,
            unit_price NUMERIC NOT NULL,
            note TEXT,
            CONSTRAINT fk_cust FOREIGN KEY(customer_id) REFERENCES customers(id),
            CONSTRAINT fk_prod FOREIGN KEY(product_id) REFERENCES products(id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS invoices (
            id {id_col},
            delivery_id INTEGER NOT NULL,
            total NUMERIC NOT NULL,
            issued_at DATE NOT NULL,
            due_at DATE NOT NULL,
            status TEXT DEFAULT 'open',
            CONSTRAINT fk_deliv FOREIGN KEY(delivery_id) REFERENCES deliveries(id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS payments (
            id {id_col},
            invoice_id INTEGER NOT NULL,
            amount NUMERIC NOT NULL,
            paid_at DATE NOT NULL,
            method TEXT DEFAULT 'cash',
            note TEXT,
            CONSTRAINT fk_inv FOREIGN KEY(invoice_id) REFERENCES invoices(id)
        )
        """,
    ]

DDL = make_ddl(DIALECT)
with ENGINE.begin() as conn:
    for ddl in DDL:
        conn.execute(text(ddl))

# ----------------- Helpers ------------------
@st.cache_data(ttl=2)
def load_df(query, **params):
    try:
        with ENGINE.begin() as conn:
            return pd.read_sql_query(text(query), conn, params=params)
    except Exception as e:
        # Zeige die echte Fehlermeldung in der UI statt die App abstürzen zu lassen
        st.error("SQL-Fehler in Abfrage:")
        st.code(query, language="sql")
        st.exception(e)
        return pd.DataFrame()

def execute(sql, **params):
    with ENGINE.begin() as conn:
        conn.execute(text(sql), params)

# Compute invoice status
@st.cache_data(ttl=2)
def invoice_status(inv_id: int):
    inv = load_df("SELECT id,total FROM invoices WHERE id=:i", i=inv_id)
    if inv.empty:
        return "open"
    total = Decimal(str(inv.iloc[0]["total"]))
    paid = load_df("SELECT COALESCE(SUM(amount),0) AS s FROM payments WHERE invoice_id=:i", i=inv_id).iloc[0]["s"] or 0
    paid = Decimal(str(paid))
    if paid == 0:
        return "open"
    if paid < total:
        return "partial"
    return "paid"

# Update all invoice statuses (called after mutations)
def refresh_invoice_statuses():
    invs = load_df("SELECT id FROM invoices")
    for i in invs["id"].tolist():
        s = invoice_status(i)
        execute("UPDATE invoices SET status=:s WHERE id=:i", s=s, i=i)

# ----------------- UI -----------------------
st.set_page_config(page_title="Adetta Lite", page_icon="🧴", layout="wide")

# Optional: einfacher PIN-Schutz
PIN = os.environ.get("ADETTA_PIN", "")
if PIN:
    pin_ok = st.session_state.get("_pin_ok", False)
    if not pin_ok:
        st.title("🔐 Adetta Lite – Login")
        pin_try = st.text_input("PIN eingeben", type="password")
        if st.button("Login"):
            if pin_try == PIN:
                st.session_state["_pin_ok"] = True
                st.rerun()
            else:
                st.error("Falscher PIN")
        st.stop()

st.title("Adetta Lite 🧴")

TABS = st.tabs(["📊 Dashboard", "📦 Produkte", "🧑‍🤝‍🧑 Kunden", "🚚 Lieferungen", "🧾 Rechnungen & Zahlungen"])  

# ------------- Dashboard -------------
with TABS[0]:
    col1, col2 = st.columns([2, 1])
    # Lager
    with col1:
        st.subheader("Lagerbestand")
        dfp = load_df("SELECT id,name,sku,price,stock,min_stock FROM products ORDER BY name")
        st.dataframe(dfp, use_container_width=True)
        low = dfp[dfp["stock"] <= dfp["min_stock"]]
        if not low.empty:
            st.warning("Niedriger Bestand bei: " + ", ".join(low["name"].tolist()))
        else:
            st.success("Keine Low-Stock-Warnungen.")
    # Offene Posten
    with col2:
        st.subheader("Offene Posten je Kunde")
        q = """
        SELECT c.name AS kunde,
               SUM(i.total - IFNULL(paid.sum_paid,0)) AS offen
        FROM invoices i
        JOIN deliveries d ON d.id = i.delivery_id
        JOIN customers c ON c.id = d.customer_id
        LEFT JOIN (
            SELECT invoice_id, SUM(amount) AS sum_paid FROM payments GROUP BY invoice_id
        ) paid ON paid.invoice_id = i.id
        WHERE i.status != 'paid'
        GROUP BY c.name
        ORDER BY offen DESC
        """
        dfb = load_df(q)
        st.dataframe(dfb, use_container_width=True)

    st.divider()
    st.subheader("Umsatz (letzte 30 Tage)")
    q30 = "SELECT SUM(total) AS revenue_30 FROM invoices WHERE issued_at >= :d"
    since = (date.today() - timedelta(days=30)).isoformat()
    rev = load_df(q30, d=since).iloc[0]["revenue_30"] or 0
    st.metric("Umsatz 30 Tage", f"{rev:,.2f}")

# ------------- Produkte -------------
with TABS[1]:
    st.subheader("Produkt anlegen")
    with st.form("prod_add", clear_on_submit=True):
        c1, c2, c3 = st.columns(3)
        name = c1.text_input("Name", key="pname")
        sku = c2.text_input("SKU", key="psku")
        price = c3.number_input("Preis/Karton", min_value=0.0, step=0.01, key="pprice")
        c4, c5 = st.columns(2)
        stock = c4.number_input("Startbestand (Kartons)", min_value=0, step=1, key="pstock")
        min_stock = c5.number_input("Mindestbestand", min_value=0, step=1, key="pmin")
        submitted = st.form_submit_button("Hinzufügen")
        if submitted:
            if name and sku:
                execute(
                    "INSERT INTO products(name,sku,price,stock,min_stock) VALUES (:n,:s,:p,:st,:ms)",
                    n=name, s=sku, p=price, st=int(stock), ms=int(min_stock)
                )
                st.success("Produkt angelegt")
                st.cache_data.clear()
            else:
                st.error("Name und SKU sind erforderlich")

    st.subheader("Produkte")
    st.dataframe(load_df("SELECT id,name,sku,price,stock,min_stock,created_at FROM products ORDER BY name"), use_container_width=True)

# -------------- Kunden --------------
with TABS[2]:
    st.subheader("Kunde anlegen")
    with st.form("cust_add", clear_on_submit=True):
        c1, c2 = st.columns(2)
        cname = c1.text_input("Name")
        caddr = c1.text_input("Adresse")
        ccontact = c1.text_input("Kontakt")
        cterms = c2.number_input("Zahlungsziel (Tage)", min_value=0, step=1, value=30)
        ok = st.form_submit_button("Hinzufügen")
        if ok:
            execute("INSERT INTO customers(name,address,contact,terms) VALUES (:n,:a,:c,:t)", n=cname, a=caddr, c=ccontact, t=int(cterms))
            st.success("Kunde angelegt")
            st.cache_data.clear()

    st.subheader("Kunden")
    st.dataframe(load_df("SELECT id,name,address,contact,terms,created_at FROM customers ORDER BY name"), use_container_width=True)

# ------------- Lieferungen -------------
with TABS[3]:
    st.subheader("Lieferung buchen")
    dfc = load_df("SELECT id,name FROM customers ORDER BY name")
    dfp = load_df("SELECT id,name,stock FROM products ORDER BY name")
    if dfc.empty or dfp.empty:
        st.info("Bitte zuerst Kunden und Produkte anlegen.")
    else:
        with st.form("deliv_form", clear_on_submit=True):
            c1, c2 = st.columns(2)
            cust = c1.selectbox("Kunde", dfc["name"].tolist())
            prod = c2.selectbox("Produkt", [f"{r.name} (Lager: {r.stock})" for r in dfp.itertuples()])
            qty = st.number_input("Kartons", min_value=1, step=1)
            unit_price = st.number_input("Preis/Karton", min_value=0.0, step=0.01)
            ddate = st.date_input("Datum", value=date.today())
            note = st.text_input("Notiz", value="")
            submit = st.form_submit_button("Buchen")

        if submit:
            cust_id = int(dfc[dfc["name"]==cust].iloc[0]["id"])
            # Produkt-ID aus Auswahl extrahieren (Name vor ' (Lager:')
            prod_name = prod.split(" (Lager:")[0]
            prod_row = dfp[dfp["name"]==prod_name].iloc[0]
            prod_id = int(prod_row["id"])
            stock_now = int(prod_row["stock"])
            if qty > stock_now:
                st.error(f"Nicht genug Bestand. Verfügbar: {stock_now}")
            else:
                # Lieferung speichern
                execute(
                    "INSERT INTO deliveries(ddate,customer_id,product_id,qty,unit_price,note) VALUES (:d,:c,:p,:q,:u,:n)",
                    d=ddate.isoformat(), c=cust_id, p=prod_id, q=int(qty), u=unit_price, n=note
                )
                # Bestand reduzieren
                execute("UPDATE products SET stock = stock - :q WHERE id=:pid", q=int(qty), pid=prod_id)
                # Rechnung erzeugen
                cust_terms = int(load_df("SELECT terms FROM customers WHERE id=:i", i=cust_id).iloc[0]["terms"])
                total = Decimal(str(unit_price)) * Decimal(str(qty))
                issued = ddate
                due = ddate + timedelta(days=cust_terms)
                execute(
                    "INSERT INTO invoices(delivery_id,total,issued_at,due_at,status) VALUES ((SELECT MAX(id) FROM deliveries), :t, :i, :du, 'open')",
                    t=float(total), i=issued.isoformat(), du=due.isoformat()
                )
                refresh_invoice_statuses()
                st.success("Lieferung & Rechnung erstellt")
                st.cache_data.clear()

    st.subheader("Letzte Lieferungen")
    q = """
    SELECT d.id, d.ddate, c.name AS kunde, p.name AS produkt, d.qty, d.unit_price,
           (d.qty*d.unit_price) AS total,
           i.id AS invoice_id, i.status
    FROM deliveries d
    JOIN customers c ON c.id = d.customer_id
    JOIN products p ON p.id = d.product_id
    JOIN invoices i ON i.delivery_id = d.id
    ORDER BY d.id DESC LIMIT 50
    """
    st.dataframe(load_df(q), use_container_width=True)

# --------- Rechnungen & Zahlungen ---------
with TABS[4]:
    st.subheader("Rechnungen")
    q = """
    SELECT i.id AS rechnung, i.issued_at, i.due_at, i.total, i.status,
           c.name AS kunde, p.name AS produkt, d.qty, d.unit_price,
           COALESCE(pay.sum_paid, 0) AS bezahlt,
           i.total - COALESCE(pay.sum_paid, 0) AS offen
    FROM invoices i
    JOIN deliveries d ON d.id = i.delivery_id
    JOIN customers c ON c.id = d.customer_id
    JOIN products p ON p.id = d.product_id
    LEFT JOIN (
        SELECT invoice_id, SUM(amount) AS sum_paid FROM payments GROUP BY invoice_id
    ) pay ON pay.invoice_id = i.id
    ORDER BY i.id DESC
    """
    dfi = load_df(q)
    st.dataframe(dfi, use_container_width=True)

    st.subheader("Zahlung buchen")
    if dfi.empty:
        st.info("Keine Rechnungen vorhanden.")
    else:
        with st.form("pay_form", clear_on_submit=True):
            inv_id = st.selectbox("Rechnung #", dfi["rechnung"].astype(int).tolist())
            amount = st.number_input("Betrag", min_value=0.0, step=0.01)
            paid_at = st.date_input("Datum", value=date.today())
            method = st.selectbox("Methode", ["cash","bank","card"])
            note = st.text_input("Notiz", value="")
            ok = st.form_submit_button("Buchen")
        if ok:
            execute(
                "INSERT INTO payments(invoice_id,amount,paid_at,method,note) VALUES (:i,:a,:p,:m,:n)",
                i=int(inv_id), a=float(amount), p=paid_at.isoformat(), m=method, n=note
            )
            refresh_invoice_statuses()
            st.success("Zahlung verbucht")
            st.cache_data.clear()

st.caption("Adetta Lite v0.2 — Streamlit One‑File. Nächste Schritte: PDF‑Rechnungen, Multi‑Positionen, User‑Rollen, Cloud‑Deploy.")

