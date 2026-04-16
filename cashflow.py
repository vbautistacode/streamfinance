# cashflow.py
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
from db import engine
import plotly.graph_objects as go
import plotly.express as px
from utils import format_brl

from sqlalchemy import text

# Helper: tenta converter string de moeda "R$ 1.234,56" ou "1.234,56" para float
def parse_currency(x):
    if pd.isna(x):
        return np.nan
    if isinstance(x, (int, float, np.number)):
        return float(x)
    s = str(x).strip()
    s = s.replace("R$", "").replace("r$", "").replace(" ", "")
    # remover pontos de milhar e normalizar vírgula decimal
    # cuidado: primeiro remover pontos que são separadores de milhar
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        import re
        m = re.search(r"-?\d+(\.\d+)?", s)
        if m:
            try:
                return float(m.group(0))
            except Exception:
                return np.nan
        return np.nan

# Tenta ler transactions do DB como fallback
def read_transactions_from_db():
    try:
        with engine.connect() as conn:
            df = pd.read_sql("SELECT * FROM transactions", conn)
            # normalizar colunas esperadas
            if "date" in df.columns and "amount" in df.columns:
                df = df.rename(columns={"date": "data", "amount": "valor (R$)", "description": "lancamento"})
            return df
    except Exception:
        return pd.DataFrame()

def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    # Normaliza nomes de colunas para os esperados
    cols = {c.lower().strip(): c for c in df.columns}
    mapping = {}
    for k, orig in cols.items():
        if k == "data" or k.startswith("data"):
            mapping[orig] = "data"
        elif "lançamento" in k or "lancamento" in k or k.startswith("lanc"):
            mapping[orig] = "lancamento"
        elif "ag." in k or "ag/origem" in k or "origem" in k:
            mapping[orig] = "ag/origem"
        elif "valor" in k:
            mapping[orig] = "valor (R$)"
        elif "saldo" in k or "saldos" in k:
            mapping[orig] = "saldos (R$)"
        else:
            mapping[orig] = orig
    df = df.rename(columns=mapping)

    # converter data
    if "data" in df.columns:
        df["data"] = pd.to_datetime(df["data"], dayfirst=True, errors="coerce")

    # converter valores
    if "valor (R$)" in df.columns:
        df["valor (R$)"] = df["valor (R$)"].apply(parse_currency)
    else:
        # tentar detectar coluna de valores
        for c in df.columns:
            try:
                sample = df[c].dropna().astype(str).head(10).tolist()
                if any(any(ch.isdigit() for ch in s) for s in sample):
                    # não renomeia automaticamente aqui; será tratado depois
                    pass
            except Exception:
                pass

    if "saldos (R$)" in df.columns:
        df["saldos (R$)"] = df["saldos (R$)"].apply(parse_currency)

    return df

def save_cashflow_to_db(df: pd.DataFrame, table_name: str = "transactions") -> int:
    """
    Persiste lançamentos no banco na tabela `transactions`.
    Requer colunas: data (datetime), lancamento (str), valor (float).
    Retorna número de linhas inseridas.
    """
    if df.empty:
        return 0

    # preparar dataframe mínimo
    df2 = df.copy()
    # garantir colunas
    if "data" not in df2.columns or "valor (R$)" not in df2.columns:
        return 0
    df2 = df2.rename(columns={"lancamento": "description", "valor (R$)": "amount"})
    df2["date"] = pd.to_datetime(df2["data"], errors="coerce")
    df2["description"] = df2.get("description", "").astype(str)
    df2["amount"] = pd.to_numeric(df2["amount"], errors="coerce").fillna(0.0)

    # deduplicar localmente por (date, description, amount)
    df2["dup_key"] = df2["date"].dt.strftime("%Y-%m-%d %H:%M:%S").fillna("") + "|" + df2["description"].str.strip() + "|" + df2["amount"].astype(str)
    df2 = df2.drop_duplicates(subset=["dup_key"])

    inserted = 0
    with engine.begin() as conn:
        for _, r in df2.iterrows():
            try:
                # evitar inserir duplicata já existente: checar existência
                exists = conn.execute(text("""
                    SELECT 1 FROM transactions WHERE date = :date AND amount = :amount AND description = :desc LIMIT 1
                """), {"date": r["date"], "amount": float(r["amount"]), "desc": r["description"]}).fetchone()
                if exists:
                    continue
                conn.execute(text("""
                    INSERT INTO transactions (date, description, amount, created_at)
                    VALUES (:date, :description, :amount, CURRENT_TIMESTAMP)
                """), {"date": r["date"], "description": r["description"], "amount": float(r["amount"])})
                inserted += 1
            except Exception:
                # ignorar erros de linha individual para não abortar todo o batch
                continue
    return inserted

def render_cash_ui():
    st.header("Fluxo de Caixa")

    st.markdown(
        "Envie um arquivo **.xlsx** ou **.csv** com as colunas: "
        "**data, lançamento, ag./origem, valor (R$), saldos (R$)**. "
        "Se nenhum arquivo for enviado, o app tentará usar a tabela `transactions` do banco como fallback."
    )

    uploaded = st.file_uploader("Enviar planilha (.xlsx ou .csv)", type=["xlsx", "csv"], key="cashflow_upload")
    df = pd.DataFrame()

    if uploaded is not None:
        try:
            if str(uploaded.name).lower().endswith(".csv"):
                # tentar detectar separador automaticamente
                try:
                    df = pd.read_csv(uploaded, sep=None, engine="python")
                except Exception:
                    df = pd.read_csv(uploaded)
            else:
                df = pd.read_excel(uploaded, sheet_name=0)
            st.success(f"Arquivo '{uploaded.name}' carregado. Linhas: {len(df)}")
        except Exception as e:
            st.error(f"Falha ao ler o arquivo: {e}")
            df = pd.DataFrame()
    else:
        df = read_transactions_from_db()
        if not df.empty:
            st.info("Nenhum arquivo enviado — usando dados da tabela `transactions` do banco de dados como fallback.")

    if df.empty:
        st.info("Nenhum dado disponível. Envie um arquivo XLSX/CSV com as colunas esperadas ou carregue dados na tabela `transactions`.")
        return

    # Normalizar
    df = _normalize_df(df)

    # Verificar coluna de valor
    if "valor (R$)" not in df.columns:
        # tentar inferir coluna de valores
        numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]) or df[c].astype(str).str.contains(r"[\d\.,\-]").any()]
        if numeric_cols:
            sums = {c: df[c].apply(lambda v: parse_currency(v) if not pd.isna(v) else 0).abs().sum() for c in numeric_cols}
            best = max(sums, key=sums.get)
            df = df.rename(columns={best: "valor (R$)"})
            df["valor (R$)"] = df["valor (R$)"].apply(parse_currency)
        else:
            st.error("Não foi possível identificar a coluna de valores. Verifique o arquivo.")
            return

    # garantir data
    if "data" not in df.columns:
        date_cols = []
        for c in df.columns:
            try:
                parsed = pd.to_datetime(df[c], dayfirst=True, errors="coerce")
                if parsed.notna().sum() > 0:
                    date_cols.append(c)
            except Exception:
                pass
        if date_cols:
            df = df.rename(columns={date_cols[0]: "data"})
            df["data"] = pd.to_datetime(df["data"], dayfirst=True, errors="coerce")
        else:
            st.error("Não foi possível identificar a coluna de datas. Verifique o arquivo.")
            return

    # ordenar por data
    df = df.sort_values("data").reset_index(drop=True)

    # garantir tipos
    df["valor (R$)"] = df["valor (R$)"].astype(float)
    df["entrada"] = df["valor (R$)"].apply(lambda v: v if v > 0 else 0.0)
    df["saida"] = df["valor (R$)"].apply(lambda v: -v if v < 0 else 0.0)

    total_entradas = df["entrada"].sum()
    total_saidas = df["saida"].sum()

    if "saldos (R$)" in df.columns and df["saldos (R$)"].notna().any():
        last_saldo = df.loc[df["saldos (R$)"].notna(), "saldos (R$)"].iloc[-1]
        current_balance = last_saldo
    else:
        current_balance = df["valor (R$)"].sum()

    # KPIs
    k1, k2, k3 = st.columns(3)
    k1.metric("Receita total", format_brl(total_entradas))
    k2.metric("Despesa total", format_brl(total_saidas))
    k3.metric("Saldo atual", format_brl(current_balance))

    st.markdown("---")

    # Filtros por período
    st.subheader("Filtros")
    min_date = df["data"].min()
    max_date = df["data"].max()
    col_a, col_b = st.columns(2)
    with col_a:
        start = st.date_input("Data inicial", value=min_date.date() if pd.notna(min_date) else datetime.today().date(), min_value=min_date.date() if pd.notna(min_date) else None)
    with col_b:
        end = st.date_input("Data final", value=max_date.date() if pd.notna(max_date) else datetime.today().date(), max_value=max_date.date() if pd.notna(max_date) else None)

    mask = (df["data"].dt.date >= start) & (df["data"].dt.date <= end)
    df_filtered = df.loc[mask].copy()

    # Agregar por dia para gráfico
    df_daily = df_filtered.groupby("data", as_index=False).agg(
        entradas=("entrada", "sum"),
        saidas=("saida", "sum"),
        saldo_dia=("valor (R$)", "sum")
    ).sort_values("data")
    df_daily["saldo_acumulado"] = df_daily["saldo_dia"].cumsum()

    # Gráfico: barras (entradas / saídas) + linha saldo acumulado
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df_daily["data"],
        y=df_daily["entradas"],
        name="Entradas",
        marker_color="#2ca02c",
        hovertemplate="%{x|%Y-%m-%d}<br>Entradas: %{y:$,.2f}<extra></extra>"
    ))
    fig.add_trace(go.Bar(
        x=df_daily["data"],
        y=-df_daily["saidas"],
        name="Saídas",
        marker_color="#d62728",
        hovertemplate="%{x|%Y-%m-%d}<br>Saídas: %{y:$,.2f}<extra></extra>"
    ))
    fig.add_trace(go.Scatter(
        x=df_daily["data"],
        y=df_daily["saldo_acumulado"],
        mode="lines+markers",
        name="Saldo acumulado",
        line=dict(color="#1f77b4", width=3),
        hovertemplate="%{x|%Y-%m-%d}<br>Saldo acumulado: %{y:$,.2f}<extra></extra>"
    ))

    fig.update_layout(
        barmode="relative",
        title="Fluxo de Caixa (período filtrado)",
        xaxis_title="Data",
        yaxis_title="Valor (R$)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=420,
        margin=dict(t=40, b=40, l=40, r=40)
    )

    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

        # Mostrar tabela com preview e totais
    st.subheader("Detalhe de lançamentos (período filtrado)")
    df_display = df_filtered.copy()
    df_display["data"] = df_display["data"].dt.strftime("%Y-%m-%d")
    if "saldos (R$)" in df_display.columns:
        df_display["saldos (R$)"] = df_display["saldos (R$)"].apply(lambda v: format_brl(v) if not pd.isna(v) else "")
    df_display["valor (R$)"] = df_display["valor (R$)"].apply(lambda v: format_brl(v))
    df_display["entrada"] = df_display["entrada"].apply(lambda v: format_brl(v) if v > 0 else "")
    df_display["saida"] = df_display["saida"].apply(lambda v: format_brl(v) if v > 0 else "")

    # montar colunas exibidas com segurança
    cols_show = ["data"]
    if "lancamento" in df_display.columns:
        cols_show.append("lancamento")
    elif "description" in df_display.columns:
        cols_show.append("description")
    if "ag/origem" in df_display.columns:
        cols_show.append("ag/origem")
    cols_show += ["valor (R$)", "entrada", "saida"]
    if "saldos (R$)" in df_display.columns:
        cols_show.append("saldos (R$)")

    # garantir que todas as colunas existem antes de exibir
    cols_show = [c for c in cols_show if c in df_display.columns]

    st.dataframe(df_display[cols_show], use_container_width=True)

    st.markdown("---")

    # Botão para salvar no banco
    st.subheader("Persistir lançamentos")
    st.markdown("Se desejar, salve os lançamentos filtrados na tabela `transactions`. O processo evita duplicatas por data+descrição+valor.")
    if st.button("Salvar lançamentos no banco"):
        try:
            n = save_cashflow_to_db(df_filtered)
            if n > 0:
                st.success(f"{n} lançamentos salvos no banco.")
            else:
                st.info("Nenhum lançamento novo foi salvo (possíveis duplicatas ou nenhum registro válido).")
        except Exception as e:
            st.error(f"Erro ao salvar no banco: {e}")

    st.markdown("---")