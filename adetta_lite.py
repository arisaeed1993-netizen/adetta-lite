# adetta_lite.py
# Ein einfacher, einzelner Streamlit-Prototyp für Adetta.
# DB = SQLite oder Postgres über ADETTA_DB.
# Start lokal:
#   pip install streamlit pandas sqlalchemy
#   streamlit run adetta_lite.py

import os
from datetime import date, timedelta
from decimal import Decimal

import pandas as pd
import streamlit as st
from sqlalchemy import bindparam, create_engine, text

try:
    import folium
    from streamlit_folium import st_folium
except Exception:
    folium = None
    st_folium = None

# ------------------ DB ------------------
DB_URL = os.environ.get("ADETTA_DB", "sqlite:///adetta_lite.db")
ENGINE = create_engine(DB_URL, future=True)
DIALECT = ENGINE.url.get_backend_name()


def make_ddl(dialect: str):
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
            latitude NUMERIC,
            longitude NUMERIC,
            map_address TEXT,
            area_name TEXT,
            terms INTEGER DEFAULT 30,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS delivery_headers (
            id {id_col},
            ddate DATE NOT NULL,
            customer_id INTEGER NOT NULL,
            note TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT fk_delivery_header_cust FOREIGN KEY(customer_id) REFERENCES customers(id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS deliveries (
            id {id_col},
            header_id INTEGER,
            ddate DATE NOT NULL,
            customer_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            qty INTEGER NOT NULL,
            unit_price NUMERIC NOT NULL,
            invoice_id INTEGER,
            note TEXT,
            CONSTRAINT fk_cust FOREIGN KEY(customer_id) REFERENCES customers(id),
            CONSTRAINT fk_prod FOREIGN KEY(product_id) REFERENCES products(id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS invoices (
            id {id_col},
            invoice_no INTEGER,
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
        f"""
        CREATE TABLE IF NOT EXISTS expenses (
            id {id_col},
            edate DATE NOT NULL,
            category TEXT NOT NULL,
            amount NUMERIC NOT NULL,
            customer_id INTEGER,
            note TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT fk_exp_cust FOREIGN KEY(customer_id) REFERENCES customers(id)
        )
        """,
    ]


with ENGINE.begin() as conn:
    for ddl in make_ddl(DIALECT):
        conn.execute(text(ddl))

    # Migration für ältere Datenbanken:
    # - invoice_id verbindet mehrere Produktpositionen mit einer Rechnung.
    # - header_id verbindet mehrere Produktpositionen mit einer einzigen sichtbaren Liefer-ID.
    try:
        if DIALECT.startswith("postgresql"):
            conn.execute(text("ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS invoice_id INTEGER"))
            conn.execute(text("ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS header_id INTEGER"))
            conn.execute(text("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS invoice_no INTEGER"))
            conn.execute(text("ALTER TABLE customers ADD COLUMN IF NOT EXISTS latitude NUMERIC"))
            conn.execute(text("ALTER TABLE customers ADD COLUMN IF NOT EXISTS longitude NUMERIC"))
            conn.execute(text("ALTER TABLE customers ADD COLUMN IF NOT EXISTS map_address TEXT"))
            conn.execute(text("ALTER TABLE customers ADD COLUMN IF NOT EXISTS area_name TEXT"))
        else:
            cust_cols = [row[1] for row in conn.execute(text("PRAGMA table_info(customers)")).fetchall()]
            if "latitude" not in cust_cols:
                conn.execute(text("ALTER TABLE customers ADD COLUMN latitude NUMERIC"))
            if "longitude" not in cust_cols:
                conn.execute(text("ALTER TABLE customers ADD COLUMN longitude NUMERIC"))
            if "map_address" not in cust_cols:
                conn.execute(text("ALTER TABLE customers ADD COLUMN map_address TEXT"))
            if "area_name" not in cust_cols:
                conn.execute(text("ALTER TABLE customers ADD COLUMN area_name TEXT"))
            cols = [row[1] for row in conn.execute(text("PRAGMA table_info(deliveries)")).fetchall()]
            if "invoice_id" not in cols:
                conn.execute(text("ALTER TABLE deliveries ADD COLUMN invoice_id INTEGER"))
            if "header_id" not in cols:
                conn.execute(text("ALTER TABLE deliveries ADD COLUMN header_id INTEGER"))
            inv_cols = [row[1] for row in conn.execute(text("PRAGMA table_info(invoices)")).fetchall()]
            if "invoice_no" not in inv_cols:
                conn.execute(text("ALTER TABLE invoices ADD COLUMN invoice_no INTEGER"))
    except Exception:
        pass

    # Alte Daten auffüllen: bisher hatte jede Rechnung genau eine delivery_id.
    conn.execute(text("""
        UPDATE deliveries
        SET invoice_id = (
            SELECT invoices.id FROM invoices WHERE invoices.delivery_id = deliveries.id LIMIT 1
        )
        WHERE invoice_id IS NULL
    """))

    # Für vorhandene Lieferpositionen ohne Header wird je Rechnung ein Lieferkopf erzeugt.
    # Wenn keine Rechnung existiert, bekommt jede einzelne Position einen eigenen Lieferkopf.
    orphan_rows = conn.execute(text("""
        SELECT d.id, d.ddate, d.customer_id, d.note, d.invoice_id
        FROM deliveries d
        WHERE d.header_id IS NULL
        ORDER BY COALESCE(d.invoice_id, d.id), d.id
    """)).mappings().fetchall()
    created_headers = {}
    for row in orphan_rows:
        group_key = row["invoice_id"] if row["invoice_id"] is not None else f"delivery-{row['id']}"
        if group_key not in created_headers:
            conn.execute(text("""
                INSERT INTO delivery_headers(ddate, customer_id, note)
                VALUES (:d, :c, :n)
            """), {"d": row["ddate"], "c": row["customer_id"], "n": row["note"]})
            if DIALECT.startswith("postgresql"):
                header_id = conn.execute(text("SELECT currval(pg_get_serial_sequence('delivery_headers','id'))")).scalar()
            else:
                header_id = conn.execute(text("SELECT last_insert_rowid()")).scalar()
            created_headers[group_key] = int(header_id)
        conn.execute(text("UPDATE deliveries SET header_id=:h WHERE id=:id"), {
            "h": created_headers[group_key],
            "id": row["id"],
        })

    # Sichtbare Rechnungsnummer: standardmäßig dieselbe Nummer wie die sichtbare Liefer-ID.
    # Die technische invoices.id bleibt intern unabhängig und kann daher höher/niedriger sein.
    conn.execute(text("""
        UPDATE invoices
        SET invoice_no = (
            SELECT d.header_id
            FROM deliveries d
            WHERE d.id = invoices.delivery_id
            LIMIT 1
        )
        WHERE invoice_no IS NULL
    """))


# ------------------ Helper ------------------
@st.cache_data(ttl=2)
def load_df(query, **params):
    try:
        with ENGINE.begin() as conn:
            return pd.read_sql_query(text(query), conn, params=params)
    except Exception as e:
        st.error("SQL-Fehler in Abfrage:")
        st.code(query, language="sql")
        st.exception(e)
        return pd.DataFrame()


def execute(sql, **params):
    with ENGINE.begin() as conn:
        conn.execute(text(sql), params)


def execute_scalar(sql, **params):
    with ENGINE.begin() as conn:
        return conn.execute(text(sql), params).scalar()


def get_last_insert_id(conn, table_name: str):
    if DIALECT.startswith("postgresql"):
        return int(conn.execute(text(f"SELECT currval(pg_get_serial_sequence('{table_name}','id'))")).scalar())
    return int(conn.execute(text("SELECT last_insert_rowid()")).scalar())


def get_invoice_status(inv_id: int):
    row = load_df("""
        SELECT i.total, COALESCE(pay.sum_paid, 0) AS paid
        FROM invoices i
        LEFT JOIN (
            SELECT invoice_id, SUM(amount) AS sum_paid
            FROM payments
            GROUP BY invoice_id
        ) pay ON pay.invoice_id = i.id
        WHERE i.id = :i
    """, i=int(inv_id))
    if row.empty:
        return "open"
    total = Decimal(str(row["total"].iloc[0] or 0))
    paid = Decimal(str(row["paid"].iloc[0] or 0))
    if paid == 0:
        return "open"
    if paid < total:
        return "partial"
    return "paid"


def update_invoice_status(inv_id: int):
    # Nur eine Rechnung aktualisieren statt nach jeder Buchung alle Rechnungen neu zu berechnen.
    status = get_invoice_status(int(inv_id))
    execute("UPDATE invoices SET status=:s WHERE id=:i", s=status, i=int(inv_id))


def refresh_invoice_statuses():
    # Notfallfunktion für alte Daten; nicht mehr nach jeder Buchung verwenden.
    invs = load_df("SELECT id FROM invoices")
    for i in invs["id"].tolist():
        update_invoice_status(int(i))


def money(value):
    return f"{float(value or 0):,.2f}"


def parse_google_maps_input(value: str):
    """Einfache Hilfe: akzeptiert '35.55, 45.43' oder Google-Maps-Links mit @lat,lng."""
    if not value:
        return None, None
    txt = value.strip()
    import re

    match = re.search(r"@(-?\d+(?:\.\d+)?),(-?\d+(?:\.\d+)?)", txt)
    if match:
        return float(match.group(1)), float(match.group(2))

    match = re.search(r"(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)", txt)
    if match:
        lat = float(match.group(1))
        lon = float(match.group(2))
        if -90 <= lat <= 90 and -180 <= lon <= 180:
            return lat, lon
    return None, None


# ------------------ Seiten ------------------
def render_dashboard():
    st.subheader("Lagerbestand")
    dfp = load_df("SELECT id,name,sku,price,stock,min_stock FROM products ORDER BY name")
    st.dataframe(dfp, use_container_width=True)

    if not dfp.empty:
        low = dfp[dfp["stock"] <= dfp["min_stock"]]
        if not low.empty:
            st.warning("Niedriger Bestand bei: " + ", ".join(low["name"].tolist()))
        else:
            st.success("Keine Low-Stock-Warnungen.")
    else:
        st.info("Noch keine Produkte angelegt.")

    st.divider()
    st.subheader("Zahlungsübersicht")
    df_totals = load_df("""
        SELECT
            COALESCE(SUM(i.total), 0) AS rechnungen_gesamt,
            COALESCE(SUM(pay.sum_paid), 0) AS bezahlt_gesamt,
            COALESCE(SUM(i.total), 0) - COALESCE(SUM(pay.sum_paid), 0) AS offen_gesamt
        FROM invoices i
        LEFT JOIN (
            SELECT invoice_id, SUM(amount) AS sum_paid
            FROM payments
            GROUP BY invoice_id
        ) pay ON pay.invoice_id = i.id
    """)

    if not df_totals.empty:
        c1, c2, c3 = st.columns(3)
        c1.metric("Rechnungen gesamt", money(df_totals["rechnungen_gesamt"].iloc[0]))
        c2.metric("Bezahlt gesamt", money(df_totals["bezahlt_gesamt"].iloc[0]))
        c3.metric("Offen gesamt", money(df_totals["offen_gesamt"].iloc[0]))

    st.divider()
    st.subheader("Umsatz")
    period = st.selectbox("Zeitraum", ["30 Tage", "90 Tage", "365 Tage", "Alle"], index=0, key="period_dashboard")
    since = None if period == "Alle" else (date.today() - timedelta(days=int(period.split()[0]))).isoformat()

    if since:
        rev_total = load_df("SELECT COALESCE(SUM(total),0) AS s FROM invoices WHERE issued_at >= :d", d=since).iloc[0]["s"] or 0
    else:
        rev_total = load_df("SELECT COALESCE(SUM(total),0) AS s FROM invoices").iloc[0]["s"] or 0
    st.metric("Gesamtumsatz", money(rev_total))

    q = """
        SELECT c.name AS supermarkt, SUM(i.total) AS umsatz
        FROM invoices i
        JOIN deliveries d ON d.id = i.delivery_id
        JOIN customers c ON c.id = d.customer_id
        {where}
        GROUP BY c.name
        ORDER BY umsatz DESC
    """
    if since:
        df_rev = load_df(q.format(where="WHERE i.issued_at >= :d"), d=since)
    else:
        df_rev = load_df(q.format(where=""))
    st.dataframe(df_rev, use_container_width=True)


def render_products():
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
                n=name, s=sku, p=float(price), st=int(stock), ms=int(min_stock)
            )
            st.success("Produkt angelegt")
            st.cache_data.clear()
            st.rerun()
        else:
            st.error("Name und SKU sind erforderlich")

    st.subheader("Produkte")
    st.dataframe(
        load_df("SELECT id,name,sku,price,stock,min_stock,created_at FROM products ORDER BY name"),
        use_container_width=True
    )


def render_customers():
    st.subheader("Kunde anlegen")
    with st.form("cust_add", clear_on_submit=True):
        c1, c2 = st.columns(2)
        cname = c1.text_input("Name", key="cust_name_add")
        caddr = c1.text_input("Adresse", key="cust_addr_add")
        ccontact = c1.text_input("Kontakt", key="cust_contact_add")
        cterms = c2.number_input("Zahlungsziel (Tage)", min_value=0, step=1, value=30, key="cust_terms_add")
        c2.markdown("**Karte (optional)**")
        cmap_address = c2.text_input("Adresse für Karte", key="cust_map_addr_add")
        carea_name = c2.text_input("Stadtteil / Gebiet", key="cust_area_add")
        clat = c2.number_input("Latitude", value=0.0, format="%.6f", key="cust_lat_add")
        clon = c2.number_input("Longitude", value=0.0, format="%.6f", key="cust_lon_add")
        ok = st.form_submit_button("Hinzufügen")

    if ok:
        if not cname.strip():
            st.error("Bitte Kundennamen eingeben.")
        else:
            execute(
                """
                INSERT INTO customers(name,address,contact,terms,latitude,longitude,map_address,area_name)
                VALUES (:n,:a,:c,:t,:lat,:lon,:ma,:area)
                """,
                n=cname, a=caddr, c=ccontact, t=int(cterms),
                lat=float(clat) if float(clat) != 0.0 else None,
                lon=float(clon) if float(clon) != 0.0 else None,
                ma=cmap_address, area=carea_name
            )
            st.success("Kunde angelegt")
            st.cache_data.clear()
            st.rerun()

    st.subheader("Kunden")
    dfc_view = load_df("SELECT id,name,address,contact,latitude,longitude,map_address,area_name,terms,created_at FROM customers ORDER BY name")
    st.dataframe(dfc_view, use_container_width=True)

    if dfc_view.empty:
        st.info("Noch keine Kunden angelegt.")
        return

    st.divider()
    st.subheader("Kunde bearbeiten")
    edit_customer_id = st.selectbox(
        "Kunde auswählen",
        dfc_view["id"].astype(int).tolist(),
        format_func=lambda cid: dfc_view[dfc_view["id"].astype(int) == int(cid)]["name"].iloc[0],
        key="edit_customer_select"
    )
    row = dfc_view[dfc_view["id"].astype(int) == int(edit_customer_id)].iloc[0]

    with st.form("edit_customer_form"):
        new_name = st.text_input("Name", value=row["name"] or "", key="edit_customer_name")
        new_address = st.text_input("Adresse", value=row["address"] or "", key="edit_customer_address")
        new_contact = st.text_input("Kontakt", value=row["contact"] or "", key="edit_customer_contact")
        new_terms = st.number_input("Zahlungsziel Tage", min_value=0, step=1, value=int(row["terms"] or 0), key="edit_customer_terms")
        st.markdown("**Karte / Standort**")
        map_input = st.text_input("Google-Maps-Link oder Koordinaten einfügen", value="", key="edit_customer_map_input")
        parsed_lat, parsed_lon = parse_google_maps_input(map_input)
        base_lat = float(row["latitude"]) if pd.notna(row.get("latitude")) else 0.0
        base_lon = float(row["longitude"]) if pd.notna(row.get("longitude")) else 0.0
        new_lat = st.number_input("Latitude", value=float(parsed_lat) if parsed_lat is not None else base_lat, format="%.6f", key="edit_customer_lat")
        new_lon = st.number_input("Longitude", value=float(parsed_lon) if parsed_lon is not None else base_lon, format="%.6f", key="edit_customer_lon")
        new_map_address = st.text_input("Adresse für Karte", value=row["map_address"] or row["address"] or "", key="edit_customer_map_address")
        new_area = st.text_input("Stadtteil / Gebiet", value=row["area_name"] or "", key="edit_customer_area")
        save_customer = st.form_submit_button("Kunden speichern")

    if save_customer:
        execute(
            """
            UPDATE customers
            SET name=:n, address=:a, contact=:c, terms=:t,
                latitude=:lat, longitude=:lon, map_address=:ma, area_name=:area
            WHERE id=:id
            """,
            n=new_name, a=new_address, c=new_contact, t=int(new_terms),
            lat=float(new_lat) if float(new_lat) != 0.0 else None,
            lon=float(new_lon) if float(new_lon) != 0.0 else None,
            ma=new_map_address, area=new_area, id=int(edit_customer_id)
        )
        st.success("Kunde wurde aktualisiert.")
        st.cache_data.clear()
        st.rerun()

    st.divider()
    st.subheader("Kundendetails: Schulden & gelieferte Ware")
    detail_customer_id = st.selectbox(
        "Supermarkt auswählen",
        dfc_view["id"].astype(int).tolist(),
        format_func=lambda cid: dfc_view[dfc_view["id"].astype(int) == int(cid)]["name"].iloc[0],
        key="customer_detail"
    )

    q_open = """
        SELECT
            COALESCE(SUM(i.total), 0) AS gesamt_rechnung,
            COALESCE(SUM(pay.sum_paid), 0) AS gesamt_bezahlt,
            COALESCE(SUM(i.total), 0) - COALESCE(SUM(pay.sum_paid), 0) AS offen
        FROM invoices i
        JOIN deliveries d ON d.id = i.delivery_id
        LEFT JOIN (
            SELECT invoice_id, SUM(amount) AS sum_paid
            FROM payments
            GROUP BY invoice_id
        ) pay ON pay.invoice_id = i.id
        WHERE d.customer_id = :cid
    """
    df_amt = load_df(q_open, cid=int(detail_customer_id))
    gesamt = float(df_amt["gesamt_rechnung"].iloc[0] or 0) if not df_amt.empty else 0.0
    bezahlt = float(df_amt["gesamt_bezahlt"].iloc[0] or 0) if not df_amt.empty else 0.0
    offen = float(df_amt["offen"].iloc[0] or 0) if not df_amt.empty else 0.0

    c1, c2, c3 = st.columns(3)
    c1.metric("Gesamt in Rechnung gestellt", money(gesamt))
    c2.metric("Bisher bezahlt", money(bezahlt))
    c3.metric("Offen (Schulden)", money(offen))

    q_deliv_sum = """
        SELECT
            COALESCE(SUM(qty), 0) AS gesamt_kartons,
            COALESCE(SUM(qty * unit_price), 0) AS gesamt_warenwert
        FROM deliveries
        WHERE customer_id = :cid
    """
    df_deliv_sum = load_df(q_deliv_sum, cid=int(detail_customer_id))
    ges_kartons = int(df_deliv_sum["gesamt_kartons"].iloc[0] or 0) if not df_deliv_sum.empty else 0
    ges_wert = float(df_deliv_sum["gesamt_warenwert"].iloc[0] or 0) if not df_deliv_sum.empty else 0.0

    c4, c5 = st.columns(2)
    c4.metric("Gelieferte Kartons gesamt", f"{ges_kartons}")
    c5.metric("Warenwert gesamt", money(ges_wert))

    st.markdown("#### Gelieferte Produkte im Detail")
    df_deliv_detail = load_df("""
        SELECT p.name AS produkt, SUM(d.qty) AS kartons, SUM(d.qty * d.unit_price) AS warenwert
        FROM deliveries d
        JOIN products p ON p.id = d.product_id
        WHERE d.customer_id = :cid
        GROUP BY p.name
        ORDER BY kartons DESC
    """, cid=int(detail_customer_id))
    st.dataframe(df_deliv_detail, use_container_width=True)


def render_deliveries():
    st.subheader("Lieferung buchen")
    dfc = load_df("SELECT id,name,terms FROM customers ORDER BY name")
    dfp = load_df("SELECT id,name,stock,price FROM products ORDER BY name")

    if dfc.empty or dfp.empty:
        st.info("Bitte zuerst Kunden und Produkte anlegen.")
    else:
        c1, c2 = st.columns(2)
        cust_id = c1.selectbox(
            "Kunde",
            dfc["id"].astype(int).tolist(),
            format_func=lambda cid: dfc[dfc["id"].astype(int) == int(cid)]["name"].iloc[0],
            key="deliv_customer"
        )
        ddate = c2.date_input("Datum", value=date.today(), key="deliv_date")
        note = st.text_input("Notiz", value="", key="deliv_note")

        selected_ids = st.multiselect(
            "Produkte",
            dfp["id"].astype(int).tolist(),
            format_func=lambda pid: dfp[dfp["id"].astype(int) == int(pid)]["name"].iloc[0],
            key="deliv_products"
        )

        quantities = {}
        if selected_ids:
            st.markdown("**Mengen je Produkt (Kartons)**")
            for prod_id in selected_ids:
                row = dfp[dfp["id"].astype(int) == int(prod_id)].iloc[0]
                stock_now = int(row["stock"] or 0)
                price_now = float(row["price"] or 0)
                col1, col2, col3 = st.columns([2, 1, 1])
                col1.write(f"**{row['name']}**")
                col2.write(f"Lager: {stock_now}")
                col3.write(f"Preis/Karton: {price_now:,.2f}")
                quantities[int(prod_id)] = st.number_input(
                    f"Kartons für {row['name']}",
                    min_value=0,
                    step=1,
                    key=f"qty_{int(prod_id)}"
                )

        submit = st.button("Lieferung buchen", type="primary", key="submit_delivery")
        if submit:
            if not selected_ids:
                st.error("Bitte mindestens ein Produkt auswählen.")
            else:
                cust_terms = int(dfc[dfc["id"].astype(int) == int(cust_id)]["terms"].iloc[0] or 0)
                lines = []
                error = False
                for prod_id in selected_ids:
                    qty = int(quantities.get(int(prod_id), 0) or 0)
                    if qty <= 0:
                        continue
                    row = dfp[dfp["id"].astype(int) == int(prod_id)].iloc[0]
                    stock_now = int(row["stock"] or 0)
                    unit_price = float(row["price"] or 0)
                    pname = row["name"]

                    if unit_price <= 0:
                        st.error(f"Kein Preis für Produkt '{pname}' hinterlegt.")
                        error = True
                        break
                    if qty > stock_now:
                        st.error(f"Nicht genug Bestand für '{pname}'. Verfügbar: {stock_now}, angefragt: {qty}.")
                        error = True
                        break
                    lines.append({"prod_id": int(prod_id), "qty": qty, "unit_price": unit_price})

                if not lines and not error:
                    st.error("Es wurden keine Mengen größer als 0 eingetragen.")
                    error = True

                if not error:
                    # Eine Buchung = ein Lieferkopf + mehrere Produktpositionen + eine Rechnung.
                    due = ddate + timedelta(days=cust_terms)
                    invoice_total = sum(
                        Decimal(str(line["unit_price"])) * Decimal(str(line["qty"]))
                        for line in lines
                    )

                    with ENGINE.begin() as conn:
                        conn.execute(text(
                            "INSERT INTO delivery_headers(ddate,customer_id,note) VALUES (:d,:c,:n)"
                        ), {"d": ddate.isoformat(), "c": int(cust_id), "n": note})
                        header_id = get_last_insert_id(conn, "delivery_headers")

                        delivery_line_ids = []
                        for line in lines:
                            conn.execute(text(
                                "INSERT INTO deliveries(header_id,ddate,customer_id,product_id,qty,unit_price,note) "
                                "VALUES (:h,:d,:c,:p,:q,:u,:n)"
                            ), {
                                "h": header_id,
                                "d": ddate.isoformat(),
                                "c": int(cust_id),
                                "p": line["prod_id"],
                                "q": line["qty"],
                                "u": line["unit_price"],
                                "n": note,
                            })
                            delivery_line_id = get_last_insert_id(conn, "deliveries")
                            delivery_line_ids.append(int(delivery_line_id))
                            conn.execute(text("UPDATE products SET stock = stock - :q WHERE id=:pid"), {
                                "q": line["qty"], "pid": line["prod_id"]
                            })

                        # Für Kompatibilität bleibt invoices.delivery_id auf der ersten Produktposition.
                        first_delivery_line_id = delivery_line_ids[0]
                        conn.execute(text(
                            "INSERT INTO invoices(invoice_no,delivery_id,total,issued_at,due_at,status) "
                            "VALUES (:invoice_no, :delivery_id, :t, :i, :du, 'open')"
                        ), {
                            "invoice_no": int(header_id),
                            "delivery_id": first_delivery_line_id,
                            "t": float(invoice_total),
                            "i": ddate.isoformat(),
                            "du": due.isoformat()
                        })
                        invoice_id = get_last_insert_id(conn, "invoices")

                        conn.execute(
                            text("UPDATE deliveries SET invoice_id=:inv WHERE id IN :ids")
                            .bindparams(bindparam("ids", expanding=True)),
                            {"inv": int(invoice_id), "ids": delivery_line_ids}
                        )

                    st.success(
                        f"Lieferung #{header_id} mit {len(lines)} Produktposition(en) und 1 Rechnung erstellt. "
                        f"Gesamt: {money(invoice_total)}"
                    )
                    st.cache_data.clear()
                    st.rerun()

    st.divider()
    st.subheader("Letzte Lieferungen")
    product_summary = (
        "STRING_AGG(p.name || ' x ' || d.qty::TEXT, ', ' ORDER BY d.id)"
        if DIALECT.startswith("postgresql")
        else "GROUP_CONCAT(p.name || ' x ' || d.qty, ', ')"
    )
    df_last = load_df(f"""
        SELECT h.id AS lieferung_id,
               h.ddate,
               c.name AS kunde,
               {product_summary} AS produkte,
               SUM(d.qty) AS kartons,
               SUM(d.qty * d.unit_price) AS total,
               MIN(i.id) AS interne_invoice_id,
               MIN(COALESCE(i.invoice_no, i.id)) AS rechnungsnr,
               MIN(i.status) AS status
        FROM delivery_headers h
        JOIN customers c ON c.id = h.customer_id
        JOIN deliveries d ON d.header_id = h.id
        JOIN products p ON p.id = d.product_id
        LEFT JOIN invoices i ON i.id = d.invoice_id
        GROUP BY h.id, h.ddate, c.name
        ORDER BY h.id DESC
        LIMIT 50
    """)
    st.dataframe(df_last, use_container_width=True, hide_index=True)

    st.subheader("Lieferung löschen")
    if df_last.empty:
        st.caption("Keine Lieferungen vorhanden.")
    else:
        header_id = st.selectbox(
            "Lieferung auswählen",
            df_last["lieferung_id"].astype(int).tolist(),
            format_func=lambda hid: (
                f"Lieferung #{hid} - "
                f"{df_last[df_last['lieferung_id'].astype(int) == int(hid)]['kunde'].iloc[0]} - "
                f"{df_last[df_last['lieferung_id'].astype(int) == int(hid)]['produkte'].iloc[0]}"
            ),
            key="delete_delivery_header_id"
        )
        if st.button("Ausgewählte Lieferung löschen", type="secondary", key="delete_delivery_btn"):
            lines_df = load_df("SELECT * FROM deliveries WHERE header_id=:h", h=int(header_id))
            if lines_df.empty:
                st.error("Lieferung nicht gefunden.")
            else:
                inv_ids = [int(x) for x in lines_df["invoice_id"].dropna().unique().tolist()]
                line_ids = [int(x) for x in lines_df["id"].astype(int).tolist()]
                with ENGINE.begin() as conn:
                    for _, row in lines_df.iterrows():
                        conn.execute(
                            text("UPDATE products SET stock = stock + :q WHERE id=:pid"),
                            {"q": int(row["qty"]), "pid": int(row["product_id"])}
                        )
                    for inv_id in inv_ids:
                        conn.execute(text("DELETE FROM payments WHERE invoice_id=:i"), {"i": inv_id})
                        conn.execute(text("DELETE FROM invoices WHERE id=:i"), {"i": inv_id})
                    conn.execute(
                        text("DELETE FROM deliveries WHERE id IN :ids").bindparams(bindparam("ids", expanding=True)),
                        {"ids": line_ids}
                    )
                    conn.execute(text("DELETE FROM delivery_headers WHERE id=:h"), {"h": int(header_id)})

                st.success(f"Lieferung #{header_id} wurde vollständig gelöscht, inklusive Rechnung/Zahlungen und Lagerkorrektur.")
                st.cache_data.clear()
                st.rerun()

    st.divider()
    st.subheader("Produktposition einer Lieferung korrigieren")
    df_edit_del = load_df("""
        SELECT h.id AS lieferung_id, d.id AS position_id, d.qty, d.unit_price,
               d.product_id, d.customer_id, d.invoice_id,
               p.name AS produkt, c.name AS kunde
        FROM deliveries d
        JOIN delivery_headers h ON h.id = d.header_id
        JOIN products p ON p.id = d.product_id
        JOIN customers c ON c.id = d.customer_id
        ORDER BY h.id DESC, d.id ASC
    """)

    if df_edit_del.empty:
        st.caption("Keine Lieferungen zum Korrigieren vorhanden.")
    else:
        pos_choices = df_edit_del["position_id"].astype(int).tolist()
        selected_pos_id = st.selectbox(
            "Lieferung / Produktposition auswählen",
            pos_choices,
            format_func=lambda pid: (
                f"Lieferung #{int(df_edit_del[df_edit_del['position_id'].astype(int) == int(pid)]['lieferung_id'].iloc[0])} - "
                f"{df_edit_del[df_edit_del['position_id'].astype(int) == int(pid)]['kunde'].iloc[0]} - "
                f"{df_edit_del[df_edit_del['position_id'].astype(int) == int(pid)]['produkt'].iloc[0]} - "
                f"{int(df_edit_del[df_edit_del['position_id'].astype(int) == int(pid)]['qty'].iloc[0])} Kartons"
            ),
            key="edit_delivery_position_select"
        )
        drow = df_edit_del[df_edit_del["position_id"].astype(int) == int(selected_pos_id)].iloc[0]
        with st.form("edit_delivery_position_form"):
            new_qty = st.number_input("Neue Anzahl Kartons", min_value=1, step=1, value=int(drow["qty"]), key="edit_delivery_qty")
            save_del = st.form_submit_button("Produktposition korrigieren")

        if save_del:
            old_qty = int(drow["qty"])
            diff = int(new_qty) - old_qty
            product_id = int(drow["product_id"])
            current_stock = int(load_df("SELECT stock FROM products WHERE id=:id", id=product_id).iloc[0]["stock"] or 0)
            if diff > current_stock:
                st.error("Nicht genug Lagerbestand für diese Korrektur.")
            else:
                execute("UPDATE deliveries SET qty=:q WHERE id=:id", q=int(new_qty), id=int(selected_pos_id))
                execute("UPDATE products SET stock = stock - :diff WHERE id=:pid", diff=diff, pid=product_id)
                inv_id = int(drow["invoice_id"]) if pd.notna(drow["invoice_id"]) else None
                if inv_id:
                    new_total = execute_scalar("SELECT COALESCE(SUM(qty * unit_price), 0) FROM deliveries WHERE invoice_id=:i", i=int(inv_id)) or 0
                    execute("UPDATE invoices SET total=:t WHERE id=:i", t=float(new_total), i=int(inv_id))
                    update_invoice_status(int(inv_id))
                st.success(f"Lieferung #{int(drow['lieferung_id'])} wurde korrigiert.")
                st.cache_data.clear()
                st.rerun()

def render_invoices_payments():
    st.subheader("Rechnungen")
    product_summary = (
        "STRING_AGG(p.name || ' x ' || d.qty::TEXT, ', ' ORDER BY d.id)"
        if DIALECT.startswith("postgresql")
        else "GROUP_CONCAT(p.name || ' x ' || d.qty, ', ')"
    )
    dfi = load_df(f"""
        SELECT i.id AS invoice_id,
               COALESCE(i.invoice_no, MIN(d.header_id), i.id) AS rechnung,
               MIN(d.header_id) AS lieferung_id,
               i.issued_at, i.due_at, i.total, i.status,
               c.name AS kunde,
               {product_summary} AS produkte,
               SUM(d.qty) AS kartons,
               COALESCE(pay.sum_paid, 0) AS bezahlt,
               i.total - COALESCE(pay.sum_paid, 0) AS offen
        FROM invoices i
        JOIN deliveries d ON (d.invoice_id = i.id OR (d.invoice_id IS NULL AND d.id = i.delivery_id))
        JOIN customers c ON c.id = d.customer_id
        JOIN products p ON p.id = d.product_id
        LEFT JOIN (
            SELECT invoice_id, SUM(amount) AS sum_paid FROM payments GROUP BY invoice_id
        ) pay ON pay.invoice_id = i.id
        GROUP BY i.id, i.invoice_no, i.issued_at, i.due_at, i.total, i.status, c.name, pay.sum_paid
        ORDER BY rechnung DESC
    """)
    st.dataframe(dfi, use_container_width=True)

    st.subheader("Zahlung buchen")
    if dfi.empty:
        st.info("Keine Rechnungen vorhanden.")
    else:
        left, right = st.columns([1, 1])
        with left:
            inv_choices = dfi["invoice_id"].astype(int).tolist()
            inv_id = st.selectbox(
                "Rechnung #",
                inv_choices,
                format_func=lambda iid: (
                    f"Rechnung #{int(dfi[dfi['invoice_id'].astype(int) == int(iid)]['rechnung'].iloc[0])} "
                    f"(Lieferung #{int(dfi[dfi['invoice_id'].astype(int) == int(iid)]['lieferung_id'].iloc[0])})"
                ),
                key="inv_select"
            )
            open_amt = float(dfi[dfi["invoice_id"].astype(int) == int(inv_id)]["offen"].iloc[0])
            paid_amt = float(dfi[dfi["invoice_id"].astype(int) == int(inv_id)]["bezahlt"].iloc[0])
            st.metric("Offen", money(open_amt))
            st.metric("Bereits bezahlt", money(paid_amt))
        with right:
            st.markdown("**Zahlungsverlauf**")
            hist = load_df(
                "SELECT id, paid_at AS datum, amount AS betrag, method AS methode, COALESCE(note,'') AS notiz "
                "FROM payments WHERE invoice_id=:i ORDER BY paid_at ASC, id ASC",
                i=int(inv_id)
            )
            if hist.empty:
                st.caption("Noch keine Zahlungen erfasst.")
            else:
                st.dataframe(hist, use_container_width=True, hide_index=True)

        with st.form("pay_form", clear_on_submit=True):
            c1, c2 = st.columns(2)
            default_amount = max(float(open_amt), 0.01)
            amount = c1.number_input("Betrag", min_value=0.01, step=0.01, value=default_amount, key="pay_amount")
            paid_at = c2.date_input("Datum", value=date.today(), key="pay_date")
            c3, c4 = st.columns(2)
            method = c3.selectbox("Methode", ["cash", "bank", "card"], key="pay_method")
            note = c4.text_input("Notiz", value="", key="pay_note")
            ok = st.form_submit_button("Zahlung buchen")

        if ok:
            if amount > open_amt + 1e-6:
                st.error(f"Der Betrag ({amount:,.2f}) ist höher als der offene Betrag ({open_amt:,.2f}).")
            else:
                execute(
                    "INSERT INTO payments(invoice_id,amount,paid_at,method,note) VALUES (:i,:a,:p,:m,:n)",
                    i=int(inv_id), a=float(amount), p=paid_at.isoformat(), m=method, n=note
                )
                update_invoice_status(int(inv_id))
                st.success(f"Zahlung verbucht. Rest offen: {max(open_amt - float(amount), 0.0):,.2f}")
                st.cache_data.clear()
                st.rerun()

    st.divider()
    st.subheader("Zahlung korrigieren oder löschen")
    df_payments_edit = load_df("""
        SELECT p.id, p.invoice_id, COALESCE(i.invoice_no, i.id) AS rechnung_nr,
               p.amount, p.paid_at, p.method, COALESCE(p.note,'') AS note
        FROM payments p
        JOIN invoices i ON i.id = p.invoice_id
        ORDER BY p.paid_at DESC, p.id DESC
    """)

    if df_payments_edit.empty:
        st.caption("Keine Zahlungen vorhanden.")
    else:
        pay_choices = df_payments_edit["id"].astype(int).tolist()
        pay_id = st.selectbox(
            "Zahlung auswählen",
            pay_choices,
            format_func=lambda pid: (
                f"#{pid} - Rechnung "
                f"{int(df_payments_edit[df_payments_edit['id'].astype(int) == int(pid)]['rechnung_nr'].iloc[0])} - "
                f"{df_payments_edit[df_payments_edit['id'].astype(int) == int(pid)]['amount'].iloc[0]} - "
                f"{df_payments_edit[df_payments_edit['id'].astype(int) == int(pid)]['paid_at'].iloc[0]}"
            ),
            key="edit_payment_select"
        )
        prow = df_payments_edit[df_payments_edit["id"].astype(int) == int(pay_id)].iloc[0]

        with st.form("edit_payment_form"):
            new_amount = st.number_input("Betrag", min_value=0.01, step=0.01, value=float(prow["amount"]), key="edit_payment_amount")
            new_paid_at = st.date_input("Datum", value=pd.to_datetime(prow["paid_at"]).date(), key="edit_payment_date")
            methods = ["cash", "bank", "card"]
            current_method = prow["method"] if prow["method"] in methods else "cash"
            new_method = st.selectbox("Methode", methods, index=methods.index(current_method), key="edit_payment_method")
            new_note = st.text_input("Notiz", value=prow["note"], key="edit_payment_note")
            save_payment = st.form_submit_button("Zahlung speichern")

        c1, c2 = st.columns(2)
        if save_payment:
            execute(
                """
                UPDATE payments
                SET amount=:a, paid_at=:p, method=:m, note=:n
                WHERE id=:id
                """,
                a=float(new_amount), p=new_paid_at.isoformat(), m=new_method, n=new_note, id=int(pay_id)
            )
            update_invoice_status(int(prow["invoice_id"]))
            st.success("Zahlung wurde aktualisiert.")
            st.cache_data.clear()
            st.rerun()

        if c2.button("Zahlung löschen", key="delete_payment_btn"):
            execute("DELETE FROM payments WHERE id=:id", id=int(pay_id))
            update_invoice_status(int(prow["invoice_id"]))
            st.success("Zahlung wurde gelöscht.")
            st.cache_data.clear()
            st.rerun()


def render_map():
    st.subheader("Karte / Gebietsabdeckung")

    if folium is None or st_folium is None:
        st.error("Für die Kartenansicht fehlen Pakete: folium und streamlit-folium.")
        st.code("pip install folium streamlit-folium", language="bash")
        st.info("Auf Streamlit Cloud bitte auch requirements.txt entsprechend erweitern.")
        return

    df = load_df("""
        SELECT c.id, c.name, c.address, c.map_address, c.area_name,
               c.latitude, c.longitude,
               COALESCE(SUM(i.total), 0) AS umsatz,
               COALESCE(SUM(pay.sum_paid), 0) AS bezahlt,
               COALESCE(SUM(i.total), 0) - COALESCE(SUM(pay.sum_paid), 0) AS offen,
               MAX(h.ddate) AS letzte_lieferung
        FROM customers c
        LEFT JOIN delivery_headers h ON h.customer_id = c.id
        LEFT JOIN deliveries d ON d.header_id = h.id
        LEFT JOIN invoices i ON i.id = d.invoice_id
        LEFT JOIN (
            SELECT invoice_id, SUM(amount) AS sum_paid
            FROM payments
            GROUP BY invoice_id
        ) pay ON pay.invoice_id = i.id
        GROUP BY c.id, c.name, c.address, c.map_address, c.area_name, c.latitude, c.longitude
        ORDER BY c.name
    """)

    if df.empty:
        st.info("Noch keine Kunden/Supermärkte angelegt.")
        return

    df_geo = df[pd.notna(df["latitude"]) & pd.notna(df["longitude"])].copy()
    missing_geo = df[pd.isna(df["latitude"]) | pd.isna(df["longitude"])].copy()

    c1, c2, c3 = st.columns(3)
    c1.metric("Supermärkte gesamt", len(df))
    c2.metric("Auf Karte lokalisiert", len(df_geo))
    c3.metric("Ohne Koordinaten", len(missing_geo))

    st.markdown("### Koordinaten schnell eintragen")
    selected_customer_id = st.selectbox(
        "Supermarkt auswählen",
        df["id"].astype(int).tolist(),
        format_func=lambda cid: df[df["id"].astype(int) == int(cid)]["name"].iloc[0],
        key="map_customer_select"
    )
    selected = df[df["id"].astype(int) == int(selected_customer_id)].iloc[0]

    with st.form("map_location_form"):
        st.caption("Du kannst aus Google Maps entweder Koordinaten wie 35.56, 45.43 oder einen Link mit @35.56,45.43 einfügen.")
        map_text = st.text_input("Google-Maps-Link oder Koordinaten", key="map_parse_input")
        parsed_lat, parsed_lon = parse_google_maps_input(map_text)
        current_lat = float(selected["latitude"]) if pd.notna(selected["latitude"]) else 0.0
        current_lon = float(selected["longitude"]) if pd.notna(selected["longitude"]) else 0.0
        lat = st.number_input("Latitude", value=float(parsed_lat) if parsed_lat is not None else current_lat, format="%.6f", key="map_lat")
        lon = st.number_input("Longitude", value=float(parsed_lon) if parsed_lon is not None else current_lon, format="%.6f", key="map_lon")
        area = st.text_input("Stadtteil / Gebiet", value=selected["area_name"] or "", key="map_area")
        map_address = st.text_input("Adresse für Karte", value=selected["map_address"] or selected["address"] or "", key="map_address")
        save_loc = st.form_submit_button("Standort speichern")

    if save_loc:
        execute(
            """
            UPDATE customers
            SET latitude=:lat, longitude=:lon, map_address=:ma, area_name=:area
            WHERE id=:id
            """,
            lat=float(lat) if float(lat) != 0.0 else None,
            lon=float(lon) if float(lon) != 0.0 else None,
            ma=map_address,
            area=area,
            id=int(selected_customer_id),
        )
        st.success("Standort gespeichert.")
        st.cache_data.clear()
        st.rerun()

    st.divider()
    st.markdown("### Kartenansicht")
    if df_geo.empty:
        st.warning("Noch kein Supermarkt hat Koordinaten. Trage zuerst Latitude/Longitude ein.")
    else:
        center_lat = float(df_geo["latitude"].mean())
        center_lon = float(df_geo["longitude"].mean())
        m = folium.Map(location=[center_lat, center_lon], zoom_start=12, control_scale=True)

        show_radius = st.checkbox("500-m-Abdeckungsradius anzeigen", value=True, key="show_radius")
        for _, row in df_geo.iterrows():
            lat = float(row["latitude"])
            lon = float(row["longitude"])
            offen = float(row["offen"] or 0)
            umsatz = float(row["umsatz"] or 0)
            letzte = row["letzte_lieferung"] or "-"
            popup_html = f"""
            <b>{row['name']}</b><br>
            Gebiet: {row['area_name'] or '-'}<br>
            Adresse: {row['map_address'] or row['address'] or '-'}<br>
            Umsatz: {umsatz:,.2f}<br>
            Offen: {offen:,.2f}<br>
            Letzte Lieferung: {letzte}
            """
            folium.Marker(
                location=[lat, lon],
                popup=folium.Popup(popup_html, max_width=300),
                tooltip=row["name"],
            ).add_to(m)
            if show_radius:
                folium.Circle(
                    location=[lat, lon],
                    radius=500,
                    fill=True,
                    fill_opacity=0.08,
                    weight=1,
                ).add_to(m)

        st_folium(m, width=None, height=650)

    if not missing_geo.empty:
        st.markdown("### Supermärkte ohne Koordinaten")
        st.dataframe(
            missing_geo[["id", "name", "address", "map_address", "area_name"]],
            use_container_width=True,
            hide_index=True,
        )


def render_expenses():
    st.subheader("Ausgaben erfassen")
    cat_options = ["Lohn", "Lagerung", "Transport", "Werbung", "Standkosten"]
    dfc = load_df("SELECT id,name FROM customers ORDER BY name")
    with st.form("exp_add", clear_on_submit=True):
        c1, c2 = st.columns(2)
        edate = c1.date_input("Datum", value=date.today(), key="exp_date")
        category = c2.selectbox("Kategorie", cat_options, key="exp_category")
        amount = st.number_input("Betrag", min_value=0.01, step=0.01, key="exp_amount")
        note = st.text_input("Notiz", value="", key="exp_note")
        cust_id = None
        if category == "Standkosten":
            if dfc.empty:
                st.info("Für Standkosten bitte zuerst Kunden anlegen.")
            else:
                cust_id = st.selectbox(
                    "Supermarkt für Standkosten",
                    dfc["id"].astype(int).tolist(),
                    format_func=lambda cid: dfc[dfc["id"].astype(int) == int(cid)]["name"].iloc[0],
                    key="stand_customer"
                )
        ok = st.form_submit_button("Ausgabe speichern")

    if ok:
        execute(
            "INSERT INTO expenses(edate,category,amount,customer_id,note) VALUES (:d,:c,:a,:cid,:n)",
            d=edate.isoformat(), c=category, a=float(amount), cid=int(cust_id) if cust_id else None, n=note
        )
        st.success("Ausgabe gespeichert")
        st.cache_data.clear()
        st.rerun()

    st.subheader("Ausgaben-Übersicht")
    period_e = st.selectbox("Zeitraum", ["30 Tage", "90 Tage", "365 Tage", "Alle"], index=0, key="period_expenses")
    since = None if period_e == "Alle" else (date.today() - timedelta(days=int(period_e.split()[0]))).isoformat()

    if since:
        dfe = load_df(
            "SELECT e.id, e.edate, e.category, e.amount, COALESCE(c.name,'') AS kunde, e.note "
            "FROM expenses e LEFT JOIN customers c ON c.id = e.customer_id "
            "WHERE e.edate >= :d ORDER BY e.edate DESC",
            d=since
        )
        dsum = load_df("SELECT category, SUM(amount) AS summe FROM expenses WHERE edate >= :d GROUP BY category ORDER BY summe DESC", d=since)
    else:
        dfe = load_df(
            "SELECT e.id, e.edate, e.category, e.amount, COALESCE(c.name,'') AS kunde, e.note "
            "FROM expenses e LEFT JOIN customers c ON c.id = e.customer_id "
            "ORDER BY e.edate DESC"
        )
        dsum = load_df("SELECT category, SUM(amount) AS summe FROM expenses GROUP BY category ORDER BY summe DESC")

    st.dataframe(dfe, use_container_width=True)
    st.subheader("Summen je Kategorie")
    st.dataframe(dsum, use_container_width=True)


# ------------------ App ------------------
def main():
    st.set_page_config(page_title="Adetta", page_icon="🧴", layout="wide")

    pin = os.environ.get("ADETTA_PIN", "")
    if pin:
        pin_ok = st.session_state.get("_pin_ok", False)
        if not pin_ok:
            st.title("🔐 Adetta Lite – Login")
            pin_try = st.text_input("PIN eingeben", type="password")
            if st.button("Login"):
                if pin_try == pin:
                    st.session_state["_pin_ok"] = True
                    st.rerun()
                else:
                    st.error("Falscher PIN")
            st.stop()

    st.title("Adetta Lite")

    # Wichtig für Geschwindigkeit:
    # st.tabs() berechnet in Streamlit alle Tabs bei jedem Refresh.
    # Mit dieser Navigation wird nur die aktuell ausgewählte Seite geladen.
    page = st.sidebar.radio(
        "Bereich auswählen",
        [
            "📊 Dashboard",
            "📦 Produkte",
            "🧑‍🤝‍🧑 Kunden",
            "🚚 Lieferungen",
            "🧾 Rechnungen & Zahlungen",
            "🗺️ Karte",
            "💸 Ausgaben",
        ],
        key="main_page"
    )

    if page == "📊 Dashboard":
        render_dashboard()
    elif page == "📦 Produkte":
        render_products()
    elif page == "🧑‍🤝‍🧑 Kunden":
        render_customers()
    elif page == "🚚 Lieferungen":
        render_deliveries()
    elif page == "🧾 Rechnungen & Zahlungen":
        render_invoices_payments()
    elif page == "🗺️ Karte":
        render_map()
    elif page == "💸 Ausgaben":
        render_expenses()

    st.caption("Adetta Lite v1.0 — Kartenmodul mit Supermarkt-Standorten und 500-m-Abdeckungsradius.")


if __name__ == "__main__":
    main()
