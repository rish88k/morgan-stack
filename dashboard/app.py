"""
Banking Analytics Dashboard
Schema:   BANKING_DB.PUBLIC_AGGREGATIONS
Tables:   ACCOUNTS, CUSTOMERS, TRANS_AMOUNT, TRANS_COUNT, TRANS_TYPE
Connects via connections.toml — connection name: "myconnection"
"""

import os, random
from datetime import datetime, timedelta
from flask import Flask, jsonify, render_template
from flask_cors import CORS

try:
    import snowflake.connector
    SF_AVAILABLE = True
except ImportError:
    SF_AVAILABLE = False

app = Flask(__name__)
CORS(app)

DATABASE = "BANKING_DB"
SCHEMA   = "PUBLIC_AGGREGATIONS"
FQN      = f"{DATABASE}.{SCHEMA}"

CONNECTION_NAME = "my_connection"   # must match [myconnection] in connections.toml


def _conn():
    """Open a Snowflake connection via connections.toml. Returns None on failure."""
    if not SF_AVAILABLE:
        app.logger.warning("snowflake-connector-python not installed")
        return None
    try:
        # Do NOT use 'with' here — we need the connection to stay open
        # for run_query to use. Closed in run_query's finally block.
        conn = snowflake.connector.connect(connection_name=CONNECTION_NAME)
        return conn
    except Exception as e:
        app.logger.warning(f"Snowflake connection failed: {e}")
        return None


def run_query(sql):
    """Run a SQL query and return list of dicts, or None on failure."""
    conn = _conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute("USE WAREHOUSE AIRFLOW_WH")
        cur.execute(sql)
        cols = [d[0].lower() for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as e:
        app.logger.warning(f"Query failed: {e}")
        return None
    finally:
        conn.close()   # always close after query, even if it crashes


def j(v, p=0.05):
    return v * (1 + random.uniform(-p, p))


# ── Simulated data ────────────────────────────────────────────────────────────

def sim_accounts():
    categories = [
        {"poorness": "broke",         "no_of_accounts": int(j(3820,  0.03))},
        {"poorness": "BrokeByMonday", "no_of_accounts": int(j(8940,  0.03))},
        {"poorness": "homeless",      "no_of_accounts": int(j(14280, 0.03))},
    ]
    total = sum(c["no_of_accounts"] for c in categories)
    for c in categories:
        c["pct"] = round(c["no_of_accounts"] / total * 100, 1)
    return {"rows": categories, "total": total}

def sim_customers():
    base = [
        ("India", 4820), ("United States", 3910), ("United Kingdom", 2140),
        ("Germany", 1680), ("Brazil", 1420), ("Canada", 980),
        ("Australia", 870), ("France", 740), ("Singapore", 620), ("UAE", 510),
    ]
    return [{"country": c, "no_of_customers": int(j(n, 0.04))} for c, n in base]

def sim_trans_type():
    base = [
        ("PURCHASE", 38400), ("TRANSFER", 22100), ("WITHDRAWAL", 14600),
        ("DEPOSIT",  13200), ("PAYMENT",   9100), ("REFUND",      3800),
    ]
    rows = [{"transaction_type": t, "no_of_transactions": int(j(n, 0.04))} for t, n in base]
    total = sum(r["no_of_transactions"] for r in rows)
    for r in rows:
        r["pct"] = round(r["no_of_transactions"] / total * 100, 1)
    return {"rows": rows, "total": total}

def sim_trans_count():
    today = datetime.utcnow().date()
    rows, base = [], 3200
    for i in range(29, -1, -1):
        d = today - timedelta(days=i)
        vol = int(j(base * (0.65 if d.weekday() >= 5 else 1.0), 0.12))
        rows.append({"transaction_date": str(d), "no_of_transactions": vol})
    return rows

def sim_trans_amount():
    base = [("rainbet", 28400), ("grocers", 31200), ("poker", 9800)]
    rows = [{"category": c, "no_of_transactions": int(j(n, 0.04))} for c, n in base]
    total = sum(r["no_of_transactions"] for r in rows)
    for r in rows:
        r["pct"] = round(r["no_of_transactions"] / total * 100, 1)
    return {"rows": rows, "total": total}

def sim_summary():
    acc     = sim_accounts()
    cust    = sim_customers()
    ttype   = sim_trans_type()
    tcount  = sim_trans_count()
    tamount = sim_trans_amount()
    total_txns   = ttype["total"]
    total_acc    = acc["total"]
    total_cust   = sum(c["no_of_customers"] for c in cust)
    today_txns   = tcount[-1]["no_of_transactions"]
    yesterday    = tcount[-2]["no_of_transactions"]
    pct_change   = round((today_txns - yesterday) / yesterday * 100, 1)
    top_country  = cust[0]
    top_type     = ttype["rows"][0]
    high_val_pct = next(r["pct"] for r in tamount["rows"] if r["category"] == "poker")
    # check if real snowflake is reachable to set data_source label
    test_conn = _conn()
    is_live   = test_conn is not None
    if test_conn:
        test_conn.close()
    return {
        "total_accounts":     total_acc,
        "total_customers":    total_cust,
        "total_transactions": total_txns,
        "today_transactions": today_txns,
        "txn_change_pct":     pct_change,
        "top_country":        top_country["country"],
        "top_country_count":  top_country["no_of_customers"],
        "top_txn_type":       top_type["transaction_type"],
        "top_txn_type_pct":   top_type["pct"],
        "high_value_txn_pct": high_val_pct,
        "homeless_accounts":  next(r["no_of_accounts"] for r in acc["rows"] if r["poorness"] == "homeless"),
        "broke_accounts":     next(r["no_of_accounts"] for r in acc["rows"] if r["poorness"] == "broke"),
        "last_refreshed":     datetime.utcnow().isoformat() + "Z",
        "data_source":        "SNOWFLAKE" if is_live else "DEMO",
    }


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/summary")
def api_summary():
    return jsonify(sim_summary())

@app.route("/api/accounts")
def api_accounts():
    rows = run_query(f"SELECT no_of_accounts, poorness FROM {FQN}.ACCOUNTS ORDER BY no_of_accounts DESC")
    if rows:
        total = sum(r["no_of_accounts"] for r in rows)
        for r in rows:
            r["pct"] = round(r["no_of_accounts"] / total * 100, 1)
        return jsonify({"rows": rows, "total": total})
    return jsonify(sim_accounts())

@app.route("/api/customers")
def api_customers():
    rows = run_query(f"SELECT no_of_customers, country FROM {FQN}.CUSTOMERS ORDER BY no_of_customers DESC")
    return jsonify(rows if rows else sim_customers())

@app.route("/api/trans_type")
def api_trans_type():
    rows = run_query(f"SELECT no_of_transactions, transaction_type FROM {FQN}.TRANS_TYPE ORDER BY no_of_transactions DESC")
    if rows:
        total = sum(r["no_of_transactions"] for r in rows)
        for r in rows:
            r["pct"] = round(r["no_of_transactions"] / total * 100, 1)
        return jsonify({"rows": rows, "total": total})
    return jsonify(sim_trans_type())

@app.route("/api/trans_count")
def api_trans_count():
    rows = run_query(f"SELECT no_of_transactions, transaction_date FROM {FQN}.TRANS_COUNT ORDER BY transaction_date ASC")
    return jsonify(rows if rows else sim_trans_count())

@app.route("/api/trans_amount")
def api_trans_amount():
    rows = run_query(f"SELECT no_of_transactions, category FROM {FQN}.TRANS_AMOUNT ORDER BY no_of_transactions DESC")
    if rows:
        total = sum(r["no_of_transactions"] for r in rows)
        for r in rows:
            r["pct"] = round(r["no_of_transactions"] / total * 100, 1)
        return jsonify({"rows": rows, "total": total})
    return jsonify(sim_trans_amount())

@app.route("/health")
def health():
    return jsonify({"status": "ok", "ts": datetime.utcnow().isoformat()})

if __name__ == "__main__":
    port = int(os.getenv("PORT", 6969))
    app.run(host="0.0.0.0", port=port, debug=False)