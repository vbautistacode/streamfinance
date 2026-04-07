# db.py
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Ajuste a URL se migrar para Postgres/Supabase
DATABASE_URL = "sqlite:///personal_finance.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})

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

-- Tabela para bens/ativos inseridos manualmente (imóveis, bens móveis, empresas, novos negócios)
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

-- Tabela para passivos inseridos manualmente (financiamentos, empréstimos)
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

-- Preço histórico para ativos (opcional)
CREATE TABLE IF NOT EXISTS prices (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  asset_symbol TEXT,
  date DATE,
  close_price NUMERIC,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Snapshots diários do patrimônio líquido
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

-- Tabela de mappings (opcional) para gerenciar via DB
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
"""

def init_db(engine: Engine = engine):
    """
    Inicializa o schema de forma compatível com SQLite e outros dialects.
    - Para SQLite usa executescript via raw_connection.
    - Para outros DBs executa cada statement separadamente.
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
        statements = [s.strip() for s in DDL_SCRIPT.split(";") if s.strip()]
        with engine.begin() as conn:
            for stmt in statements:
                conn.execute(text(stmt))

# inicializa ao importar
init_db()