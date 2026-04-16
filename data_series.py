# data_series.py
from sqlalchemy import text
import pandas as pd
from db import engine
from datetime import datetime

DDL = """
CREATE TABLE IF NOT EXISTS monthly_series (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  owner TEXT NOT NULL,
  period DATE NOT NULL,
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
    """
    Ensure the monthly_series table exists. Safe to call repeatedly.
    """
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

def _normalize_period(period: str) -> str:
    """
    Accept 'YYYY-MM' or 'YYYY-MM-01' and return 'YYYY-MM-01'.
    """
    if period is None:
        return None
    period = str(period).strip()
    if len(period) == 7:
        return period + "-01"
    if len(period) == 10:
        return period
    # try parse
    try:
        dt = pd.to_datetime(period, dayfirst=False, errors='coerce')
        if pd.isna(dt):
            return None
        return dt.strftime("%Y-%m-01")
    except Exception:
        return None

def upsert_entry(owner: str, period: str, patrimonio: float=None, cdi: float=None,
                 ipca: float=None, ibov: float=None, usd: float=None, carteira: float=None):
    """
    Insert or update a monthly_series entry. period accepts 'YYYY-MM' or 'YYYY-MM-01'.
    """
    ensure_table()
    period_dt = _normalize_period(period)
    if not period_dt:
        raise ValueError("Período inválido. Use 'YYYY-MM' ou 'YYYY-MM-DD'.")
    with engine.begin() as conn:
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

def list_entries(owner: str = None) -> pd.DataFrame:
    """
    Return rows for an owner (or all owners if owner is None) ordered by period.
    Period column is returned as datetime.
    """
    ensure_table()
    q = "SELECT * FROM monthly_series"
    params = {}
    if owner:
        q += " WHERE owner = :owner"
        params["owner"] = owner
    q += " ORDER BY period"
    try:
        with engine.connect() as conn:
            df = pd.read_sql(q, conn, params=params)
            if not df.empty and 'period' in df.columns:
                df['period'] = pd.to_datetime(df['period'])
            return df
    except Exception:
        return pd.DataFrame()

def aggregate_total(period_from: str = None, period_to: str = None) -> pd.DataFrame:
    """
    Return aggregated (sum) patrimonio and average of indices per period across all owners.
    Optional period_from/period_to accept 'YYYY-MM' or 'YYYY-MM-01'.
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
            pf = _normalize_period(period_from)
            q += " AND period >= :pf"
            params["pf"] = pf
        if period_to:
            pt = _normalize_period(period_to)
            q += " AND period <= :pt"
            params["pt"] = pt
    q += " GROUP BY period ORDER BY period"
    try:
        with engine.connect() as conn:
            df = pd.read_sql(q, conn, params=params)
            if not df.empty and 'period' in df.columns:
                df['period'] = pd.to_datetime(df['period'])
            return df
    except Exception:
        return pd.DataFrame()