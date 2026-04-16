# cashflow.py
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
from db import engine
import plotly.graph_objects as go
import plotly.express as px
from utils import format_brl

# Helper: tenta converter string de moeda "R$ 1.234,56" ou "1.234,56" para float
def parse_currency(x):
    if pd.isna(x):
        return np.nan
    if isinstance(x, (int, float, np.number)):
        return float(x)
    s = str(x).strip()
    # remover R$, espaços e pontos de milhar; trocar vírgula por ponto decimal
    s = s.replace("R$", "").replace("r$", "").replace(" ", "").replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        # tentar extrair números com regex
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
                df = df.rename(columns={"date": "data", "amount": "valor (R$)"})
            return df
    except Exception:
        return pd.DataFrame()

def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    # Normaliza nomes de colunas para os esperados
    cols = {c.lower().strip(): c for c in df.columns}
    mapping = {}
    # mapear colunas comuns
    for k, orig in cols.items():
        if "data" == k or k.startswith("data"):
            mapping[orig] = "data"
        elif "lançamento" in k or "lancamento" in k or "lanc" in k:
            mapping[orig] = "lancamento"
        elif "ag." in k or "ag/origem" in k or "origem" in k:
            mapping[orig] = "ag/origem"
        elif "valor" in k:
            mapping[orig] = "valor (R$)"
        elif "saldo" in k or "saldos" in k:
            mapping[orig] = "saldos (R$)"
        else:
            # manter outras colunas
            mapping[orig] = orig
    df = df.rename(columns=mapping)
    # garantir colunas essenciais
    if "data" in df.columns:
        # converter para datetime.date
        df["data"] = pd.to_datetime(df["data"], dayfirst=True, errors="coerce")
    if "valor (R$)" in df.columns:
        df["valor (R$)"] = df["valor (R$)"].apply(parse_currency)
    else:
        # tentar detectar coluna numeric com valores
        for c in df.columns:
            if df[c].dtype in [float, int] or df[c].astype(str).str.contains(r"[\d\.,]").any():
                # não sobrescrever se já mapeado
                pass
    if "saldos (R$)" in df.columns:
        df["saldos (R$)"] = df["saldos (R$)"].apply(parse_currency)
    return df

def render_cash_ui():
    st.header("Fluxo de Caixa")

    st.markdown("Envie um arquivo XLSX/CSV com as colunas: **data, lançamento, ag./origem, valor (R$), saldos (R$)**. Se nenhum arquivo for enviado, tentaremos usar a tabela `transactions` do banco de dados como fallback.")

    uploaded = st.file_uploader("Enviar planilha (.xlsx ou .csv)", type=["xlsx", "csv"], key="cashflow_upload")
    df = pd.DataFrame()

    if uploaded is not None:
        try:
            if str(uploaded.name).lower().endswith(".csv"):
                df = pd.read_csv(uploaded)
            else:
                # ler primeira sheet
                df = pd.read_excel(uploaded, sheet_name=0)
            st.success(f"Arquivo '{uploaded.name}' carregado. Linhas: {len(df)}")
        except Exception as e:
            st.error(f"Falha ao ler o arquivo: {e}")
            df = pd.DataFrame()
    else:
        # tentar ler do DB
        df = read_transactions_from_db()
        if not df.empty:
            st.info("Nenhum arquivo enviado — usando dados da tabela `transactions` do banco de dados como fallback.")

    if df.empty:
        st.info("Nenhum dado disponível. Envie um arquivo XLSX/CSV com as colunas esperadas.")
        return

    # Normalizar
    df = _normalize_df(df)

    # Verificar coluna de valor
    if "valor (R$)" not in df.columns:
        # tentar inferir coluna de valores
        numeric_cols = [c for c in df.columns if df[c].dtype in [float, int] or df[c].astype(str).str.contains(r"[\d\.,]").any()]
        if numeric_cols:
            # escolher a coluna com maior soma absoluta
            sums = {c: df[c].apply(lambda v: parse_currency(v) if not pd.isna(v) else 0).abs().sum() for c in numeric_cols}
            best = max(sums, key=sums.get)
            df = df.rename(columns={best: "valor (R$)"})
            df["valor (R$)"] = df["valor (R$)"].apply(parse_currency)
        else:
            st.error("Não foi possível identificar a coluna de valores. Verifique o arquivo.")
            return

    # garantir data
    if "data" not in df.columns:
        # tentar inferir por coluna com datas
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

    # calcular entradas e saídas
    df["valor (R$)"] = df["valor (R$)"].astype(float)
    df["entrada"] = df["valor (R$)"].apply(lambda v: v if v > 0 else 0.0)
    df["saida"] = df["valor (R$)"].apply(lambda v: -v if v < 0 else 0.0)

    total_entradas = df["entrada"].sum()
    total_saidas = df["saida"].sum()
    # saldo atual: preferir coluna saldos (R$) se existir, senão somar
    if "saldos (R$)" in df.columns and df["saldos (R$)"].notna().any():
        # pegar último saldo não-nulo
        last_saldo = df.loc[df["saldos (R$)"].notna(), "saldos (R$)"].iloc[-1]
        current_balance = last_saldo
    else:
        # soma cumulativa do valor
        current_balance = df["valor (R$)"].sum()

    # KPIs
    k1, k2, k3 = st.columns(3)
    k1.metric("Receita total", format_brl(total_entradas))
    k2.metric("Despesa total", format_brl(total_saidas))
    k3.metric("Saldo atual", format_brl(current_balance))

    st.markdown("---")

    # Agregar por dia para gráfico
    df_daily = df.groupby("data", as_index=False).agg(
        entradas=("entrada", "sum"),
        saidas=("saida", "sum"),
        saldo_dia=("valor (R$)", "sum")
    ).sort_values("data")
    # saldo acumulado
    df_daily["saldo_acumulado"] = df_daily["saldo_dia"].cumsum()

    # Gráfico: barras empilhadas (entradas / saídas) + linha saldo acumulado
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
        y=-df_daily["saidas"],  # plotar como negativo para visual separar
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
        title="Fluxo de Caixa Diário",
        xaxis_title="Data",
        yaxis_title="Valor (R$)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=420,
        margin=dict(t=40, b=40, l=40, r=40)
    )

    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    # Mostrar tabela com preview e totais
    st.subheader("Detalhe de lançamentos")
    # formatar colunas para exibição
    df_display = df.copy()
    df_display["data"] = df_display["data"].dt.strftime("%Y-%m-%d")
    if "saldos (R$)" in df_display.columns:
        df_display["saldos (R$)"] = df_display["saldos (R$)"].apply(lambda v: format_brl(v) if not pd.isna(v) else "")
    df_display["valor (R$)"] = df_display["valor (R$)"].apply(lambda v: format_brl(v))
    df_display["entrada"] = df_display["entrada"].apply(lambda v: format_brl(v) if v>0 else "")
    df_display["saida"] = df_display["saida"].apply(lambda v: format_brl(v) if v>0 else "")

    st.dataframe(df_display[["data", "lancamento" if "lancamento" in df_display.columns else df_display.columns[1], "ag/origem" if "ag/origem" in df_display.columns else None, "valor (R$)", "entrada", "saida", "saldos (R$)"]].drop(columns=[c for c in ["ag/origem"] if c not in df_display.columns]), use_container_width=True)

    st.markdown("---")
    st.info("Use o upload para atualizar os dados. Em produção, valide formatos e adicione autenticação antes de permitir uploads.")
