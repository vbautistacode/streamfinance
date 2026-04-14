# ips.py
from sqlalchemy import text
import pandas as pd
from db import engine

DDL_IPS = """
CREATE TABLE IF NOT EXISTS ips (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT,
  content TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
"""

def ensure_ips_table():
    """Create ips table if not exists."""
    dialect = engine.dialect.name.lower()
    if dialect == "sqlite":
        raw = engine.raw_connection()
        try:
            cur = raw.cursor()
            cur.executescript(DDL_IPS)
            raw.commit()
        finally:
            cur.close()
            raw.close()
    else:
        statements = [s.strip() for s in DDL_IPS.split(";") if s.strip()]
        with engine.begin() as conn:
            for stmt in statements:
                conn.execute(text(stmt))

def save_ips(title: str, content: str):
    """Insert a new IPS version."""
    ensure_ips_table()
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO ips (title, content) VALUES (:t, :c)"),
            {"t": title, "c": content}
        )

def load_latest_ips():
    """Return the latest IPS row as dict or None."""
    ensure_ips_table()
    try:
        with engine.connect() as conn:
            df = pd.read_sql("SELECT * FROM ips ORDER BY id DESC LIMIT 1", conn)
            if df.empty:
                return None
            return df.iloc[0].to_dict()
    except Exception:
        return None

def list_ips(limit: int = 50):
    """Return a DataFrame with recent IPS versions."""
    ensure_ips_table()
    try:
        with engine.connect() as conn:
            return pd.read_sql(f"SELECT * FROM ips ORDER BY id DESC LIMIT {int(limit)}", conn)
    except Exception:
        return pd.DataFrame()

def get_ips_by_id(ips_id: int):
    """Return a single IPS row by id or None."""
    ensure_ips_table()
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text("SELECT * FROM ips WHERE id = :id"), conn.bind, params={"id": ips_id})
            if df.empty:
                return None
            return df.iloc[0].to_dict()
    except Exception:
        return None

def delete_ips(ips_id: int):
    """Delete an IPS row by id."""
    ensure_ips_table()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM ips WHERE id = :id"), {"id": ips_id})