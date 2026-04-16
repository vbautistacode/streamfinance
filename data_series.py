# data_series.py
from sqlalchemy import text
import pandas as pd
from db import engine
from datetime import datetime

DDL = """
CREATE TABLE IF NOT EXISTS monthly_series (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  owner TEXT NOT NULL,            -- ex: 'Paula Casale' ou 'Adolfo Pacheco'
  period DATE NOT NULL,           -- use first day do mês: YYYY-MM-01
  patrimonio NUMERIC,
  cdi NUMERIC,
  ipca NUMERIC,
  ibov NUMERIC,
  usd NUMERIC,
  carteira NUMERIC,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_owner_period ON monthly_series(owner, period);
"""

def ensure_table():
    dialect = engine.dialect.name.lower()
    if dialect == "sqlite":
        raw = engine.raw_connection()
        try:
            cur = raw.cursor()
            cur.executescript(DDL)
            raw.commit()
        finally:
            cur.close()
            raw.close()
    else:
        statements = [s.strip() for s in DDL.split(";") if s.strip()]
        with engine.begin() as conn:
            for stmt in statements:
                conn.execute(text(stmt))

def upsert_entry(owner: str, period: str, patrimonio: float=None, cdi: float=None,
                 ipca: float=None, ibov: float=None, usd: float=None, carteira: float=None):
    """
    period: 'YYYY-MM' or 'YYYY-MM-01' accepted. Will store as YYYY-MM-01.
    """
    ensure_table()
    # normalize period to first day
    if len(period) == 7:
        period = period + "-01"
    period_dt = period
    with engine.begin() as conn:
        # try update, else insert
        res = conn.execute(text("""
            SELECT id FROM monthly_series WHERE owner=:owner AND period=:period
        """), {"owner": owner, "period": period_dt}).fetchone()
        if res:
            conn.execute(text("""
                UPDATE monthly_series
                SET patrimonio = COALESCE(:patrimonio, patrimonio),
                    cdi = COALESCE(:cdi, cdi),
                    ipca = COALESCE(:ipca, ipca),
                    ibov = COALESCE(:ibov, ibov),
                    usd = COALESCE(:usd, usd),
                    carteira = COALESCE(:carteira, carteira),
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = :id
            """), {"patrimonio": patrimonio, "cdi": cdi, "ipca": ipca, "ibov": ibov, "usd": usd, "carteira": carteira, "id": res[0]})
        else:
            conn.execute(text("""
                INSERT INTO monthly_series (owner, period, patrimonio, cdi, ipca, ibov, usd, carteira)
                VALUES (:owner, :period, :patrimonio, :cdi, :ipca, :ibov, :usd, :carteira)
            """), {"owner": owner, "period": period_dt, "patrimonio": patrimonio, "cdi": cdi, "ipca": ipca, "ibov": ibov, "usd": usd, "carteira": carteira})

def list_entries(owner: str = None):
    ensure_table()
    q = "SELECT * FROM monthly_series"
    params = {}
    if owner:
        q += " WHERE owner = :owner"
        params["owner"] = owner
    q += " ORDER BY period"
    try:
        with engine.connect() as conn:
            return pd.read_sql(q, conn, params=params)
    except Exception:
        return pd.DataFrame()

def aggregate_total(period_from: str = None, period_to: str = None):
    """
    Return aggregated (sum) patrimonio and average of indices per period across all owners.
    """
    ensure_table()
    q = """
    SELECT period,
           SUM(COALESCE(patrimonio,0)) AS patrimonio,
           AVG(cdi) AS cdi,
           AVG(ipca) AS ipca,
           AVG(ibov) AS ibov,
           AVG(usd) AS usd,
           AVG(carteira) AS carteira
    FROM monthly_series
    """
    params = {}
    if period_from or period_to:
        q += " WHERE 1=1"
        if period_from:
            q += " AND period >= :pf"
            params["pf"] = period_from if len(period_from)>7 else period_from + "-01"
        if period_to:
            q += " AND period <= :pt"
            params["pt"] = period_to if len(period_to)>7 else period_to + "-01"
    q += " GROUP BY period ORDER BY period"
    try:
        with engine.connect() as conn:
            return pd.read_sql(q, conn, params=params)
    except Exception:
        return pd.DataFrame()