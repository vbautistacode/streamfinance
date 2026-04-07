# etl/writer.py
import pandas as pd
from sqlalchemy import text
from db import engine
import logging

logger = logging.getLogger(__name__)

def write_staging(df: pd.DataFrame, stg_table: str, import_batch_id: str, file_name: str):
    """
    Grava DataFrame bruto em tabela de staging (append).
    """
    df2 = df.copy()
    df2["import_batch_id"] = import_batch_id
    df2.to_sql(stg_table, engine, if_exists="append", index=False)
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO uploads (import_batch_id, file_name, source) VALUES (:ib, :fn, :src)"),
                     {"ib": import_batch_id, "fn": file_name, "src": stg_table})
    logger.info("Staging gravado: %s rows=%s", stg_table, len(df2))
    return len(df2)

def promote_merge_sqlite(norm_df: pd.DataFrame, import_batch_id: str, source: str):
    """
    Implementação simples para SQLite:
    - agrupa por date e description_hash
    - insere ou atualiza na tabela transactions usando chave composta (date, source, description_hash)
    """
    if norm_df is None or norm_df.empty:
        return 0
    df = norm_df.copy()
    # agrupar por date e description_hash
    df_grouped = df.groupby(["date","description_hash"], dropna=False).agg({
        "cash_in":"sum",
        "cash_out":"sum",
        "description": lambda s: " | ".join(s.dropna().astype(str).unique())
    }).reset_index()
    # para cada linha, inserir ou atualizar na tabela transactions
    promoted = 0
    with engine.begin() as conn:
        for _, row in df_grouped.iterrows():
            date_val = row["date"].date() if pd.notna(row["date"]) else None
            cash_in = float(row["cash_in"] or 0.0)
            cash_out = float(row["cash_out"] or 0.0)
            description = row["description"]
            desc_hash = row["description_hash"]
            # checar existência
            q = text("""
            SELECT id FROM transactions
            WHERE date = :date AND description_hash = :desc_hash
            LIMIT 1
            """)
            res = conn.execute(q, {"date": date_val, "desc_hash": desc_hash}).fetchone()
            if res:
                # update
                conn.execute(text("""
                    UPDATE transactions
                    SET amount = :amount, description = :description, import_batch_id = :ib
                    WHERE id = :id
                """), {"amount": cash_in - cash_out, "description": description, "ib": import_batch_id, "id": res[0]})
            else:
                # insert
                conn.execute(text("""
                    INSERT INTO transactions (account_id, date, amount, type, category, description, description_hash, import_batch_id)
                    VALUES (NULL, :date, :amount, :type, NULL, :description, :desc_hash, :ib)
                """), {"date": date_val, "amount": cash_in - cash_out, "type": "import", "description": description, "desc_hash": desc_hash, "ib": import_batch_id})
            promoted += 1
    logger.info("Promovido %s linhas para transactions (import_batch_id=%s)", promoted, import_batch_id)
    return promoted

def record_upload_result(import_batch_id: str, file_name: str, rows_staged: int, rows_promoted: int, status: str):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO upload_results (import_batch_id, file_name, rows_staged, rows_promoted, status)
            VALUES (:ib, :fn, :rs, :rp, :st)
        """), {"ib": import_batch_id, "fn": file_name, "rs": rows_staged, "rp": rows_promoted, "st": status})

def record_upload_error(import_batch_id: str, file_name: str, error_code: str, error_message: str):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO upload_errors (import_batch_id, file_name, error_code, error_message)
            VALUES (:ib, :fn, :ec, :em)
        """), {"ib": import_batch_id, "fn": file_name, "ec": error_code, "em": error_message})
