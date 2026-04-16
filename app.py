# app.py
import streamlit as st
import pandas as pd
import numpy as np
import time
import uuid
from datetime import datetime
from db import engine
from etl.normalizers import load_mapping, apply_mapping, validate_required
from etl.writer import write_staging, promote_merge_sqlite, record_upload_result, record_upload_error
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import text

# data series module (monthly series per owner)
from data_series import upsert_entry, list_entries, aggregate_total, ensure_table as ensure_series_table
ensure_series_table()

# Import UI renderers and data functions from modules
from cashflow import render_cash_ui
from ips import render_ips
from investment import (
    render_controle_ui,
    list_holdings, list_assets, list_liabilities,
    add_asset, update_asset, delete_asset,
    add_liability, update_liability, delete_liability,
    aggregate_assets_by_category, aggregate_liabilities_by_category
)

# ---------------- Utility functions ----------------
def format_brl(value):
    try:
        v = float(value)
    except Exception:
        return value
    s = f"{v:,.2f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"

# ---------------- Page config ----------------
st.set_page_config(page_title="StreamDash — Finanças Pessoais", layout="wide")
st.title("StreamDash — Finanças Pessoais")

# ---------------- Top navigation as tabs ----------------
tab_visao, tab_cash, tab_controle, tab_ips = st.tabs(["Início", "Fluxo de Caixa", "Controle de Investimentos", "IPS"])

# Sidebar: monthly series quick form and CSV import (kept in sidebar)
st.sidebar.markdown("---")
st.sidebar.subheader("Séries mensais (Paula / Adolfo)")
owner_sel = st.sidebar.selectbox("Investidor", ["Paula Casale", "Adolfo Pacheco"], key="series_owner")
period_input = st.sidebar.text_input("Período (YYYY-MM)", value=datetime.today().strftime("%Y-%m"), key="series_period")
pat_input = st.sidebar.number_input("Patrimônio (R$)", value=0.0, format="%.2f", key="series_patrimonio")
cdi_input = st.sidebar.number_input("CDI var. mensal (%)", value=0.0, format="%.4f", key="series_cdi")
ipca_input = st.sidebar.number_input("IPCA var. mensal (%)", value=0.0, format="%.4f", key="series_ipca")
ibov_input = st.sidebar.number_input("IBOV var. mensal (%)", value=0.0, format="%.4f", key="series_ibov")
usd_input = st.sidebar.number_input("Dólar var. mensal (%)", value=0.0, format="%.4f", key="series_usd")
carteira_input = st.sidebar.number_input("Carteira var. mensal (%)", value=0.0, format="%.4f", key="series_carteira")
if st.sidebar.button("Salvar série mensal", key="save_series"):
    upsert_entry(owner_sel, period_input, patrimonio=pat_input, cdi=cdi_input, ipca=ipca_input, ibov=ibov_input, usd=usd_input, carteira=carteira_input)
    st.sidebar.success("Registro salvo.")
    st.experimental_rerun()

st.sidebar.markdown("Upload CSV (owner,period,patrimonio,cdi,ipca,ibov,usd,carteira)")
csv_file = st.sidebar.file_uploader("Importar séries (CSV)", type=["csv"], key="csv_series")
if csv_file:
    try:
        df_csv = pd.read_csv(csv_file)
        for _, r in df_csv.iterrows():
            upsert_entry(r['owner'], str(r['period']), patrimonio=r.get('patrimonio'), cdi=r.get('cdi'),
                         ipca=r.get('ipca'), ibov=r.get('ibov'), usd=r.get('usd'), carteira=r.get('carteira'))
        st.sidebar.success("CSV importado.")
        st.experimental_rerun()
    except Exception as e:
        st.sidebar.error(f"Falha ao importar CSV: {e}")

# ---------------- Visão Geral tab ----------------
with tab_visao:
    st.subheader("Visão Geral")

    # Tentar ler transactions e snapshots do DB
    try:
        with engine.connect() as conn:
            df_transactions = pd.read_sql("SELECT * FROM transactions", conn)
    except Exception:
        df_transactions = pd.DataFrame()
    try:
        with engine.connect() as conn:
            df_snapshots = pd.read_sql("SELECT * FROM net_worth_snapshots", conn)
    except Exception:
        df_snapshots = pd.DataFrame()

    # Se houver snapshots, usar para evolução do patrimônio; senão, fallback mocks
    if not df_snapshots.empty:
        df_evol_real = df_snapshots.sort_values("snapshot_date").rename(columns={"snapshot_date":"date","net_worth":"Patrimônio"})
        df_evol_plot = df_evol_real[["date","Patrimônio"]].copy()
        base = float(df_evol_plot["Patrimônio"].iloc[0])
        n = len(df_evol_plot)
        cdi = base * (1 + np.cumsum(np.repeat(0.0003, n)))
        renda_fixa = base * (1 + np.cumsum(np.repeat(0.0005, n)))
        indj26 = base * (1 + np.cumsum(np.random.normal(0.0002, 0.001, n)))
        df_evol_plot["CDI"] = cdi
        df_evol_plot["Renda Fixa"] = renda_fixa
        df_evol_plot["INDJ26"] = indj26
        df_plot = df_evol_plot.melt(id_vars=["date"], var_name="serie", value_name="valor")
    else:
        np.random.seed(42)
        today = datetime.today().date()
        dates = pd.date_range(end=today, periods=180).to_pydatetime().tolist()
        patrimonio = np.cumsum(np.random.normal(loc=50, scale=200, size=len(dates))) + 100000
        cdi = 100000 * (1 + np.cumsum(np.repeat(0.0003, len(dates))))
        renda_fixa = 100000 * (1 + np.cumsum(np.repeat(0.0005, len(dates))))
        indj26 = 100000 * (1 + np.cumsum(np.random.normal(0.0002, 0.001, len(dates))))
        df_plot = pd.DataFrame({
            "date": dates,
            "Patrimônio": patrimonio,
            "CDI": cdi,
            "Renda Fixa": renda_fixa,
            "INDJ26": indj26
        }).melt(id_vars=["date"], var_name="serie", value_name="valor")

    # Plot 1: evolução do patrimônio (global)
    st.caption("1) Evolução do Patrimônio vs CDI / Renda Fixa / IBOV")
    fig1 = px.line(df_plot, x="date", y="valor", color="serie",
                   labels={"date":"Data","valor":"Valor (R$)","serie":"Série"})
    fig1.update_layout(height=420, legend_title_text="Séries", margin=dict(t=40, b=40, l=40, r=40))
    st.plotly_chart(fig1, use_container_width=True)
    st.markdown("---")

    # --- New: load monthly series for Paula and Adolfo and plot them ---
    df_paula = list_entries("Paula Casale")
    df_adolfo = list_entries("Adolfo Pacheco")
    df_total = aggregate_total()

    def _prepare(df):
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.copy()
        df['period'] = pd.to_datetime(df['period'])
        df['patrimonio'] = pd.to_numeric(df.get('patrimonio', 0), errors='coerce').fillna(0.0)
        for col in ['cdi','ipca','ibov','usd','carteira']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            else:
                df[col] = np.nan
        return df.sort_values('period')

    df_paula = _prepare(df_paula)
    df_adolfo = _prepare(df_adolfo)
    df_total = _prepare(df_total)

    # Plot: individual patrimonies + total
    st.caption("Evolução Patrimonial por Investidor")
    fig_personal = go.Figure()
    if not df_paula.empty:
        fig_personal.add_trace(go.Scatter(x=df_paula['period'], y=df_paula['patrimonio'],
                                          mode='lines+markers', name='Paula Casale',
                                          hovertemplate='%{x|%Y-%m}: %{y:$,.2f}<extra></extra>'))
    if not df_adolfo.empty:
        fig_personal.add_trace(go.Scatter(x=df_adolfo['period'], y=df_adolfo['patrimonio'],
                                          mode='lines+markers', name='Adolfo Pacheco',
                                          hovertemplate='%{x|%Y-%m}: %{y:$,.2f}<extra></extra>'))
    if not df_total.empty:
        fig_personal.add_trace(go.Scatter(x=df_total['period'], y=df_total['patrimonio'],
                                          mode='lines+markers', name='Total (soma)',
                                          line=dict(width=2, dash='dash'),
                                          hovertemplate='%{x|%Y-%m}: %{y:$,.2f}<extra></extra>'))
    if fig_personal.data:
        fig_personal.update_layout(xaxis_title="Período", yaxis_title="Patrimônio (R$)", height=420)
        st.plotly_chart(fig_personal, use_container_width=True)
    else:
        st.info("Nenhum dado mensal de patrimônio encontrado para Paula ou Adolfo.")

    st.markdown("---")

    # Plot: indices (CDI, IPCA, IBOV, USD, Carteira)
    st.caption("Variações mensais: CDI / IPCA / IBOV / USD / Carteira")
    fig_indices = go.Figure()
    def add_index_traces(df, owner_label, dash=None):
        if df.empty:
            return
        for col, label in [('cdi','CDI'), ('ipca','IPCA'), ('ibov','IBOV'), ('usd','USD'), ('carteira','Carteira')]:
            if col in df.columns and df[col].notna().any():
                fig_indices.add_trace(go.Scatter(
                    x=df['period'], y=df[col],
                    mode='lines+markers',
                    name=f"{label} - {owner_label}",
                    line=dict(dash=dash) if dash else None,
                    hovertemplate='%{x|%Y-%m}: %{y:.4f}<extra></extra>'
                ))

    add_index_traces(df_paula, "Paula Casale", dash=None)
    add_index_traces(df_adolfo, "Adolfo Pacheco", dash='dash')
    add_index_traces(df_total, "Média (Total)", dash='dot')

    if fig_indices.data:
        fig_indices.update_layout(xaxis_title="Período", yaxis_title="Variação (pct ou dec)", height=420)
        st.plotly_chart(fig_indices, use_container_width=True)
    else:
        st.info("Nenhum dado de variações mensais (CDI/IPCA/IBOV/USD/Carteira) encontrado.")

    st.markdown("---")
    # (rest of Visão Geral continues: fluxo, despesas, composição, KPIs)
    # For brevity the rest of the existing Visão Geral content is kept as before.

#----------------- Fluxo de Caixa tab ----------------
with tab_cash:
    render_cash_ui()  # This function should be defined in a separate module (e.g., cash.py) and imported at the top

# ---------------- Controle de Investimentos tab ----------------
with tab_controle:
    render_controle_ui()

# ---------------- IPS tab ----------------
with tab_ips:
    render_ips()

# ---------------- Visão Geral page ----------------
def render_visao_geral():
    st.subheader("Visão Geral")

    # Tentar ler transactions e snapshots do DB
    try:
        with engine.connect() as conn:
            df_transactions = pd.read_sql("SELECT * FROM transactions", conn)
    except Exception:
        df_transactions = pd.DataFrame()
    try:
        with engine.connect() as conn:
            df_snapshots = pd.read_sql("SELECT * FROM net_worth_snapshots", conn)
    except Exception:
        df_snapshots = pd.DataFrame()

    # Se houver snapshots, usar para evolução do patrimônio; senão, fallback mocks
    if not df_snapshots.empty:
        df_evol_real = df_snapshots.sort_values("snapshot_date").rename(columns={"snapshot_date":"date","net_worth":"Patrimônio"})
        df_evol_plot = df_evol_real[["date","Patrimônio"]].copy()
        base = float(df_evol_plot["Patrimônio"].iloc[0])
        n = len(df_evol_plot)
        cdi = base * (1 + np.cumsum(np.repeat(0.0003, n)))
        renda_fixa = base * (1 + np.cumsum(np.repeat(0.0005, n)))
        indj26 = base * (1 + np.cumsum(np.random.normal(0.0002, 0.001, n)))
        df_evol_plot["CDI"] = cdi
        df_evol_plot["Renda Fixa"] = renda_fixa
        df_evol_plot["Dólar"] = indj26
        df_plot = df_evol_plot.melt(id_vars=["date"], var_name="serie", value_name="valor")
    else:
        np.random.seed(42)
        today = datetime.today().date()
        dates = pd.date_range(end=today, periods=180).to_pydatetime().tolist()
        patrimonio = np.cumsum(np.random.normal(loc=50, scale=200, size=len(dates))) + 100000
        cdi = 100000 * (1 + np.cumsum(np.repeat(0.0003, len(dates))))
        renda_fixa = 100000 * (1 + np.cumsum(np.repeat(0.0005, len(dates))))
        indj26 = 100000 * (1 + np.cumsum(np.random.normal(0.0002, 0.001, len(dates))))
        df_plot = pd.DataFrame({
            "date": dates,
            "Patrimônio": patrimonio,
            "CDI": cdi,
            "Renda Fixa": renda_fixa,
            "INDJ26": indj26
        }).melt(id_vars=["date"], var_name="serie", value_name="valor")

    # Plot 1: evolução do patrimônio (global)
    st.caption("1) Evolução do Patrimônio vs CDI / Renda Fixa / IBOV")
    fig1 = px.line(df_plot, x="date", y="valor", color="serie",
                   labels={"date":"Data","valor":"Valor (R$)","serie":"Série"})
    fig1.update_layout(height=420, legend_title_text="Séries", margin=dict(t=40, b=40, l=40, r=40))
    st.plotly_chart(fig1, use_container_width=True)
    st.markdown("---")

    # --- New: load monthly series for Paula and Adolfo and plot them ---
    df_paula = list_entries("Paula Casale")
    df_adolfo = list_entries("Adolfo Pacheco")
    df_total = aggregate_total()

    def _prepare(df):
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.copy()
        df['period'] = pd.to_datetime(df['period'])
        df['patrimonio'] = pd.to_numeric(df.get('patrimonio', 0), errors='coerce').fillna(0.0)
        # ensure numeric for indices
        for col in ['cdi','ipca','ibov','usd','carteira']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            else:
                df[col] = np.nan
        return df.sort_values('period')

    df_paula = _prepare(df_paula)
    df_adolfo = _prepare(df_adolfo)
    df_total = _prepare(df_total)

    # Plot: individual patrimonies + total
    st.caption("Evolução Patrimonial por Investidor")
    fig_personal = go.Figure()
    if not df_paula.empty:
        fig_personal.add_trace(go.Scatter(x=df_paula['period'], y=df_paula['patrimonio'],
                                          mode='lines+markers', name='Paula Casale',
                                          hovertemplate='%{x|%Y-%m}: %{y:$,.2f}<extra></extra>'))
    if not df_adolfo.empty:
        fig_personal.add_trace(go.Scatter(x=df_adolfo['period'], y=df_adolfo['patrimonio'],
                                          mode='lines+markers', name='Adolfo Pacheco',
                                          hovertemplate='%{x|%Y-%m}: %{y:$,.2f}<extra></extra>'))
    if not df_total.empty:
        fig_personal.add_trace(go.Scatter(x=df_total['period'], y=df_total['patrimonio'],
                                          mode='lines+markers', name='Total (soma)',
                                          line=dict(width=2, dash='dash'),
                                          hovertemplate='%{x|%Y-%m}: %{y:$,.2f}<extra></extra>'))
    if fig_personal.data:
        fig_personal.update_layout(xaxis_title="Período", yaxis_title="Patrimônio (R$)", height=420)
        st.plotly_chart(fig_personal, use_container_width=True)
    else:
        st.info("Nenhum dado mensal de patrimônio encontrado para Paula ou Adolfo.")

    st.markdown("---")

    # Plot: indices (CDI, IPCA, IBOV, USD, Carteira) — compare Paula / Adolfo / Média (Total)
    st.caption("Variações mensais: CDI / IPCA / IBOV / USD / Carteira")
    fig_indices = go.Figure()
    # helper to add traces safely
    def add_index_traces(df, owner_label, dash=None):
        if df.empty:
            return
        for col, label in [('cdi','CDI'), ('ipca','IPCA'), ('ibov','IBOV'), ('usd','USD'), ('carteira','Carteira')]:
            if col in df.columns and df[col].notna().any():
                fig_indices.add_trace(go.Scatter(
                    x=df['period'], y=df[col],
                    mode='lines+markers',
                    name=f"{label} - {owner_label}",
                    line=dict(dash=dash) if dash else None,
                    hovertemplate='%{x|%Y-%m}: %{y:.4f}<extra></extra>'
                ))

    add_index_traces(df_paula, "Paula Casale", dash=None)
    add_index_traces(df_adolfo, "Adolfo Pacheco", dash='dash')
    add_index_traces(df_total, "Média (Total)", dash='dot')

    if fig_indices.data:
        fig_indices.update_layout(xaxis_title="Período", yaxis_title="Variação (pct ou dec)", height=420)
        st.plotly_chart(fig_indices, use_container_width=True)
    else:
        st.info("Nenhum dado de variações mensais (CDI/IPCA/IBOV/USD/Carteira) encontrado.")

    st.markdown("---")

    # Fluxo de caixa (existing)
    st.caption("2) Fluxo de Caixa Diário")
    if not df_transactions.empty:
        df_tx = df_transactions.copy()
        df_tx["date"] = pd.to_datetime(df_tx["date"], errors="coerce").dt.date
        df_daily = df_tx.groupby("date").agg(
            entradas=("amount", lambda s: s[s>0].sum() if not s[s>0].empty else 0.0),
            saidas=("amount", lambda s: -s[s<0].sum() if not s[s<0].empty else 0.0)
        ).reset_index()
        df_daily = df_daily.sort_values("date").tail(180)
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(x=df_daily["date"], y=df_daily["entradas"], name="Entradas", marker_color="#2ca02c",
                              text=df_daily["entradas"].apply(format_brl), textposition="auto"))
        fig2.add_trace(go.Bar(x=df_daily["date"], y=df_daily["saidas"], name="Saídas", marker_color="#d62728",
                              text=df_daily["saidas"].apply(format_brl), textposition="auto"))
        fig2.update_layout(barmode='group', xaxis_title="Data", yaxis_title="Valor (R$)", height=420)
        st.plotly_chart(fig2, use_container_width=True)
    else:
        fig2 = go.Figure()
        dates = pd.date_range(end=datetime.today().date(), periods=180).to_pydatetime().tolist()
        entradas = np.random.poisson(2, len(dates)) * np.random.uniform(50, 500, len(dates))
        saidas = np.random.poisson(3, len(dates)) * np.random.uniform(20, 400, len(dates))
        fig2.add_trace(go.Bar(x=dates, y=entradas, name="Entradas", marker_color="#2ca02c",
                              text=[format_brl(v) for v in entradas], textposition="auto"))
        fig2.add_trace(go.Bar(x=dates, y=saidas, name="Saídas", marker_color="#d62728",
                              text=[format_brl(v) for v in saidas], textposition="auto"))
        fig2.update_layout(barmode='group', xaxis_title="Data", yaxis_title="Valor (R$)", height=420)
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown("---")

    # Despesas por categoria (existing)
    st.caption("3) Principais despesas por categoria")
    if not df_transactions.empty and "category" in df_transactions.columns:
        df_tx = df_transactions.copy()
        df_tx["amount"] = pd.to_numeric(df_tx["amount"], errors="coerce").fillna(0.0)
        df_tx["expense"] = df_tx["amount"].apply(lambda x: -x if x < 0 else 0.0)
        df_cat = df_tx.groupby("category", as_index=False)["expense"].sum()
        df_cat = df_cat[df_cat["expense"] > 0].sort_values("expense", ascending=False).head(10)
        if df_cat.empty:
            st.info("Nenhuma despesa categorizada encontrada.")
        else:
            df_cat["expense_fmt"] = df_cat["expense"].apply(format_brl)
            fig3 = px.pie(df_cat, names="category", values="expense", hole=0.35)
            fig3.update_traces(textposition='inside', textinfo='percent+label',
                               customdata=df_cat[["expense_fmt"]].values,
                               hovertemplate="%{label}<br>%{customdata[0]}<extra></extra>")
            fig3.update_layout(height=420)
            st.plotly_chart(fig3, use_container_width=True)
    else:
        df_mock = pd.DataFrame({"categoria": ["Alimentação","Moradia","Transporte","Lazer","Saúde","Outros"], "valor": np.random.dirichlet(np.ones(6))*5000})
        df_mock["valor_fmt"] = df_mock["valor"].apply(format_brl)
        fig3 = px.pie(df_mock, names="categoria", values="valor", hole=0.35)
        fig3.update_traces(textposition='inside', textinfo='percent+label',
                           customdata=df_mock[["valor_fmt"]].values,
                           hovertemplate="%{label}<br>%{customdata[0]}<extra></extra>")
        fig3.update_layout(height=420)
        st.plotly_chart(fig3, use_container_width=True)

    st.markdown("---")

    # Pequena tabela resumo (robust)
    st.markdown("### Resumo")
    # Saldo total
    if not df_snapshots.empty:
        latest = df_snapshots.sort_values("snapshot_date").iloc[-1]
        saldo_total = float(latest.net_worth)
    else:
        try:
            saldo_total = float(df_plot[df_plot['serie'] == 'Patrimônio']['valor'].iloc[-1])
        except Exception:
            saldo_total = 0.0

    # Fluxo líquido últimos 30 dias
    if 'df_daily' in locals():
        try:
            fluxo_30 = (df_daily['entradas'] - df_daily['saidas']).tail(30).sum()
        except Exception:
            fluxo_30 = 0.0
    else:
        fluxo_30 = 0.0

    # Despesas últimos 30 dias
    if 'df_cat' in locals():
        try:
            despesas_30 = float(df_cat['expense'].sum())
        except Exception:
            despesas_30 = 0.0
    elif 'df_mock' in locals():
        try:
            despesas_30 = float(df_mock['valor'].sum())
        except Exception:
            despesas_30 = 0.0
    else:
        despesas_30 = 0.0

    col_a, col_b, col_c = st.columns(3)
    with col_a:
        st.metric("Saldo Total", format_brl(saldo_total))
    with col_b:
        st.metric("Fluxo Líquido (últimos 30d)", format_brl(fluxo_30))
    with col_c:
        st.metric("Despesas (últimos 30d)", format_brl(despesas_30))

    st.markdown("---")

    # ------------------ Composição Patrimonial ----------------
    st.markdown("### Composição Patrimonial | Bens e Investimentos")

    df_holdings = list_holdings()
    df_assets = list_assets()
    df_liab = list_liabilities()

    # Build assets from holdings + manual assets
    if df_holdings.empty and df_assets.empty and df_liab.empty:
        assets = [
            {"categoria": "Imóveis", "descricao": "Apartamento SP", "valor": 650000},
            {"categoria": "Imóveis", "descricao": "Casa de praia", "valor": 420000},
            {"categoria": "Bens Móveis", "descricao": "Carro - Sedan", "valor": 85000},
            {"categoria": "Empresas", "descricao": "Participação Startup A", "valor": 200000},
            {"categoria": "Novos Negócios", "descricao": "Projeto B (pré-receita)", "valor": 50000},
            {"categoria": "Investimentos", "descricao": "Carteira Ações", "valor": 180000},
            {"categoria": "Investimentos", "descricao": "Renda Fixa", "valor": 120000},
        ]
        liabilities = [
            {"categoria": "Financiamento Imobiliário", "descricao": "Saldo financiamento apto", "valor": 300000},
            {"categoria": "Empréstimo Pessoal", "descricao": "Empréstimo banco X", "valor": 25000},
        ]
        df_assets_display = pd.DataFrame(assets)
        df_liab_display = pd.DataFrame(liabilities)
    else:
        parts = []
        if not df_holdings.empty:
            df_h = df_holdings.copy()
            if "market_value" in df_h.columns:
                df_h["valor"] = df_h["market_value"].astype(float)
            elif "quantity" in df_h.columns and "avg_cost" in df_h.columns:
                df_h["valor"] = df_h["quantity"].astype(float) * df_h["avg_cost"].astype(float)
            else:
                df_h["valor"] = 0.0
            df_h["categoria"] = df_h.get("category", "Investimentos")
            df_h["descricao"] = df_h.get("asset_symbol", "")
            parts.append(df_h[["categoria","descricao","valor"]])
        if not df_assets.empty:
            parts.append(df_assets[["categoria","descricao","valor"]])
        df_assets_display = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=["categoria","descricao","valor"])
        df_liab_display = df_liab[["categoria","descricao","valor"]] if not df_liab.empty else pd.DataFrame(columns=["categoria","descricao","valor"])

    agg_assets = df_assets_display.groupby("categoria", as_index=False)["valor"].sum() if not df_assets_display.empty else pd.DataFrame(columns=["categoria","valor"])
    agg_liab = df_liab_display.groupby("categoria", as_index=False)["valor"].sum() if not df_liab_display.empty else pd.DataFrame(columns=["categoria","valor"])

    total_assets = float(agg_assets["valor"].sum()) if not agg_assets.empty else 0.0
    total_liab = float(agg_liab["valor"].sum()) if not agg_liab.empty else 0.0
    net_worth = total_assets - total_liab

    # KPIs
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Ativos Totais", format_brl(total_assets))
    with col2:
        st.metric("Passivos Totais", format_brl(total_liab))
    with col3:
        st.metric("Patrimônio Líquido", format_brl(net_worth))

    st.markdown("#### Distribuição de ativos por categoria")
    if not agg_assets.empty:
        fig_assets_pie = px.pie(agg_assets, names="categoria", values="valor", hole=0.35)
        fig_assets_pie.update_traces(textinfo="percent+label")
        st.plotly_chart(fig_assets_pie, use_container_width=True)
    else:
        st.info("Nenhum ativo registrado ainda.")

    st.markdown("#### Detalhe de ativos")
    if not df_assets_display.empty:
        df_display = df_assets_display.copy()
        df_display["valor"] = df_display["valor"].apply(format_brl)
        st.dataframe(df_display, use_container_width=True)
    else:
        st.write("Nenhum ativo detalhado disponível.")

    st.markdown("#### Distribuição de passivos por categoria")
    if not agg_liab.empty:
        fig_liab = px.bar(agg_liab, x="categoria", y="valor", labels={"valor":"Valor (R$)","categoria":"Categoria"}, text="valor")
        fig_liab.update_traces(texttemplate="R$ %{y:,.0f}")
        fig_liab.update_layout(yaxis_tickformat=",.0f")
        st.plotly_chart(fig_liab, use_container_width=True)
    else:
        st.info("Nenhum passivo registrado ainda.")

    st.markdown("#### Detalhe de passivos")
    if not df_liab_display.empty:
        df_display_l = df_liab_display.copy()
        df_display_l["valor"] = df_display_l["valor"].apply(format_brl)
        st.dataframe(df_display_l, use_container_width=True)
    else:
        st.write("Nenhum passivo detalhado disponível.")

    st.markdown("---")
    st.info("Formulários e controles de edição estão no sidebar. Para produção, adicione autenticação, validação e backups.")