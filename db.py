# db.py
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Read DATABASE_URL from environment for easy deployment (fallback to local sqlite)
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///personal_finance.db")

# Create engine with sqlite-specific connect_args when appropriate
if DATABASE_URL.startswith("sqlite"):
    engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
else:
    engine = create_engine(DATABASE_URL)

DDL_SCRIPT = """
CREATE TABLE IF NOT EXISTS accounts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT,
  type TEXT,
  currency TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transactions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  account_id INTEGER,
  date DATE,
  amount NUMERIC,
  type TEXT,
  category TEXT,
  description TEXT,
  description_hash TEXT,
  import_batch_id TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS holdings (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  account_id INTEGER,
  asset_symbol TEXT,
  quantity NUMERIC,
  avg_cost NUMERIC,
  market_value NUMERIC,
  category TEXT,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS uploads (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  import_batch_id TEXT,
  file_name TEXT,
  source TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS upload_errors (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  import_batch_id TEXT,
  file_name TEXT,
  error_code TEXT,
  error_message TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS upload_results (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  import_batch_id TEXT,
  file_name TEXT,
  rows_staged INTEGER,
  rows_promoted INTEGER,
  status TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS assets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  categoria TEXT,
  descricao TEXT,
  valor NUMERIC,
  currency TEXT DEFAULT 'BRL',
  source TEXT,
  import_batch_id TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS liabilities (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  categoria TEXT,
  descricao TEXT,
  valor NUMERIC,
  currency TEXT DEFAULT 'BRL',
  source TEXT,
  import_batch_id TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS prices (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  asset_symbol TEXT,
  date DATE,
  close_price NUMERIC,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS net_worth_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  snapshot_date DATE,
  total_cash NUMERIC,
  total_investments NUMERIC,
  total_assets NUMERIC,
  total_liabilities NUMERIC,
  net_worth NUMERIC,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS mappings (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_name TEXT,
  source_column TEXT,
  target_column TEXT,
  transform TEXT,
  required BOOLEAN DEFAULT 0,
  notes TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Investment Policy Statement (IPS) table
CREATE TABLE IF NOT EXISTS ips (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT,
  content TEXT,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
"""

def init_db(engine: Engine = engine):
    """
    Initialize database schema.
    - For SQLite use executescript via raw_connection to run multiple statements.
    - For other dialects execute statements one by one.
    """
    dialect_name = engine.dialect.name.lower()
    if dialect_name == "sqlite":
        raw_conn = engine.raw_connection()
        try:
            cursor = raw_conn.cursor()
            cursor.executescript(DDL_SCRIPT)
            raw_conn.commit()
        finally:
            cursor.close()
            raw_conn.close()
    else:
        # split by semicolon and execute each non-empty statement
        statements = [s.strip() for s in DDL_SCRIPT.split(";") if s.strip()]
        with engine.begin() as conn:
            for stmt in statements:
                conn.execute(text(stmt))

# Initialize on import
init_db()