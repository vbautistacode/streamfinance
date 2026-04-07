# etl/normalizers.py
import pandas as pd
import hashlib
from typing import Optional
from datetime import datetime

# Registry de transformações simples
def to_numeric_ptbr(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.replace(r"[^\d\-,\.]", "", regex=True).str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce")

def to_date(series: pd.Series) -> pd.Series:
    # tenta formatos comuns; dayfirst True para dd/mm/yyyy
    return pd.to_datetime(series, errors="coerce", dayfirst=True)

def strip(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip()

def lower(series: pd.Series) -> pd.Series:
    return series.astype(str).str.lower()

TRANSFORMS = {
    "to_numeric_ptbr": to_numeric_ptbr,
    "to_date": to_date,
    "to_date_iso": lambda s: pd.to_datetime(s, errors="coerce"),
    "to_date_dayfirst": to_date,
    "strip": strip,
    "lower": lower,
    "current_date": lambda s: pd.Series([pd.to_datetime(datetime.today().date())] * len(s), index=s.index)
}

import csv

def load_mapping(path: str = "mappings.csv") -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except Exception:
        # mapping vazio se não existir
        return pd.DataFrame(columns=["source_name","source_column","target_column","transform","required","notes"])

def apply_mapping(df_raw: pd.DataFrame, mapping_df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    """
    Aplica mapping declarativo ao DataFrame bruto e retorna DataFrame normalizado
    com colunas alvo (target_column). Não altera df_raw.
    """
    if df_raw is None:
        return pd.DataFrame()
    df = df_raw.copy()
    m = mapping_df[mapping_df["source_name"] == source_name]
    out = {}
    # se mapping vazio, tenta heurística simples (sinônimos)
    if m.empty:
        # heurística: procurar colunas comuns
        heur = {
            "date": ["date","data","dt","dia"],
            "cash_in": ["entradas","recebimentos","receita","valor_entrada","cash_in"],
            "cash_out": ["saidas","pagamentos","despesas","valor_saida","cash_out"],
            "description": ["descricao","historico","obs","description"]
        }
        for tgt, candidates in heur.items():
            found = None
            for c in df.columns:
                if c.lower() in candidates:
                    found = c
                    break
            if found:
                out[tgt] = df[found]
    else:
        for _, row in m.iterrows():
            src = row["source_column"]
            tgt = row["target_column"]
            transform = row.get("transform") if "transform" in row else None
            # se source_column vazio, aplicar transform global (ex: current_date)
            if pd.isna(src) or src == "":
                if transform in TRANSFORMS:
                    out[tgt] = TRANSFORMS[transform](pd.Series([None]*len(df)))
                else:
                    out[tgt] = pd.Series([None]*len(df))
                continue
            if src in df.columns:
                series = df[src]
                if isinstance(transform, str) and transform in TRANSFORMS:
                    try:
                        series = TRANSFORMS[transform](series)
                    except Exception:
                        series = series
                out[tgt] = series
            else:
                # coluna não encontrada: preencher com NaNs
                out[tgt] = pd.Series([None]*len(df))
    norm = pd.DataFrame(out, index=df.index)
    # garantir colunas numéricas e date coerentes
    for col in ["cash_in","cash_out"]:
        if col in norm.columns:
            norm[col] = to_numeric_ptbr(norm[col].fillna(0))
        else:
            norm[col] = 0.0
    if "date" in norm.columns:
        norm["date"] = pd.to_datetime(norm["date"], errors="coerce")
    else:
        norm["date"] = pd.NaT
    if "description" not in norm.columns:
        norm["description"] = ""
    norm["description"] = norm["description"].fillna("").astype(str)
    norm["description_hash"] = norm["description"].apply(lambda s: hashlib.md5(s.encode("utf-8")).hexdigest())
    return norm

def validate_required(norm_df: pd.DataFrame, mapping_df: pd.DataFrame, source_name: str):
    """
    Retorna lista de target_columns requeridas que estão faltando ou vazias.
    """
    required = mapping_df[(mapping_df["source_name"]==source_name) & (mapping_df["required"]==True)]
    missing = []
    for _, r in required.iterrows():
        tgt = r["target_column"]
        if tgt not in norm_df.columns:
            missing.append(tgt)
        else:
            # se coluna existe mas todos valores nulos
            if norm_df[tgt].dropna().empty:
                missing.append(tgt)
    return missing
