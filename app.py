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
from data_series import upsert_entry, list_entries, aggregate_total, ensure_table
ensure_table()

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

# ---------------- Shared sidebar: upload + series (kept minimal) ----------------
st.sidebar.header("Adolfo Pacheco")
st.sidebar.subheader("Atualização de Investimentos")
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
    try:
        st.experimental_rerun()
    except Exception:
        st.info("Registro salvo. Atualize a página para ver as mudanças.")

st.sidebar.markdown("---")
st.sidebar.markdown("Upload CSV (owner,period,patrimonio,cdi,ipca,ibov,usd,carteira)")
csv_file = st.sidebar.file_uploader("Importar séries (CSV)", type=["csv"], key="csv_series")
if csv_file:
    try:
        df_csv = pd.read_csv(csv_file)
        for _, r in df_csv.iterrows():
            upsert_entry(r['owner'], str(r['period']), patrimonio=r.get('patrimonio'), cdi=r.get('cdi'),
                         ipca=r.get('ipca'), ibov=r.get('ibov'), usd=r.get('usd'), carteira=r.get('carteira'))
        st.sidebar.success("CSV importado.")
        try:
            st.experimental_rerun()
        except Exception:
            st.info("CSV importado. Atualize a página para ver as mudanças.")
    except Exception as e:
        st.sidebar.error(f"Falha ao importar CSV: {e}")

# ---------------- Helper: performance infographic ----------------
def render_performance_infographic(owner_a="Paula Casale", owner_b="Adolfo Pacheco", months=12):
    """
    Exibe tabela de rentabilidades dos últimos `months` meses e gráfico de rentabilidade acumulada
    comparando Carteira, CDI, IBOV, IPCA e Dólar.
    """
    df_a = list_entries(owner_a)
    df_b = list_entries(owner_b)
    df_total = aggregate_total()

    def prepare(df):
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.copy()
        df['period'] = pd.to_datetime(df['period'])
        if 'patrimonio' in df.columns:
            df['patrimonio'] = pd.to_numeric(df['patrimonio'], errors='coerce')
        for c in ['cdi','ipca','ibov','usd','carteira']:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce')
        return df.sort_values('period').reset_index(drop=True)

    df_a = prepare(df_a)
    df_b = prepare(df_b)
    df_total = prepare(df_total)

    ref = df_total if not df_total.empty else (df_a if not df_a.empty else df_b)
    if ref.empty:
        st.info("Nenhuma série mensal encontrada para gerar o infográfico.")
        return

    # robust selection of last_periods
    periods_series = pd.to_datetime(ref['period'].dropna(), errors='coerce')
    periods_series = periods_series[periods_series.notna()]
    if periods_series.empty:
        st.info("Nenhuma data válida encontrada nas séries.")
        return
    unique_sorted = pd.Series(periods_series.unique()).dropna().astype('datetime64[ns]')
    unique_sorted = unique_sorted.sort_values()
    last_periods = unique_sorted.iloc[-months:].tolist()

    def monthly_returns_from_pat(df, label):
        if df.empty:
            return pd.DataFrame()
        d = df[df['period'].isin(last_periods)][['period','patrimonio']].dropna().set_index('period').sort_index()
        if d.empty:
            return pd.DataFrame()
        d[label] = d['patrimonio'].pct_change().fillna(0.0)
        return d[[label]]

    paula_ret = monthly_returns_from_pat(df_a, "Paula (%)")
    adolfo_ret = monthly_returns_from_pat(df_b, "Adolfo (%)")

    def index_returns(df, col, label):
        if df.empty or col not in df.columns:
            return pd.DataFrame()
        d = df[df['period'].isin(last_periods)][['period', col]].set_index('period').sort_index()
        sample = d[col].dropna().head(5)
        if not sample.empty:
            meanv = sample.abs().mean()
            if meanv > 2:  # likely percent values like 1.11
                d[col] = d[col] / 100.0
        d = d.rename(columns={col: label})
        return d

    cdi = index_returns(df_total, 'cdi', 'CDI (%)')
    ipca = index_returns(df_total, 'ipca', 'IPCA (%)')
    ibov = index_returns(df_total, 'ibov', 'IBOV (%)')
    usd = index_returns(df_total, 'usd', 'USD (%)')
    carteira_idx = index_returns(df_total, 'carteira', 'Carteira idx (%)')

    parts = [paula_ret, adolfo_ret, carteira_idx, cdi, ibov, ipca, usd]
    df_monthly = pd.concat(parts, axis=1)
    df_monthly = df_monthly.reindex(pd.to_datetime(last_periods)).fillna(np.nan)
    df_monthly.index.name = 'period'

    # formatar para exibição sem usar applymap (compatível com versões pandas)
    def fmt_cell(v):
        return f"{v*100:.2f}%" if pd.notna(v) else ""
    df_monthly_display = df_monthly.copy()
    df_monthly_display = df_monthly_display.apply(lambda col: col.map(fmt_cell))

    st.markdown("#### Histórico de rentabilidade dos últimos 12 meses (%)")
    st.table(df_monthly_display.T)

    # acumulado
    df_acc = (1 + df_monthly.fillna(0)).cumprod() - 1
    df_acc_plot = df_acc.reset_index().melt(id_vars=['period'], var_name='serie', value_name='acc_ret')

    color_map = {
        'Paula (%)': '#1f77b4',
        'Adolfo (%)': '#17becf',
        'Carteira idx (%)': '#636efa',
        'CDI (%)': '#ff7f0e',
        'IBOV (%)': '#003f5c',
        'IPCA (%)': '#e377c2',
        'USD (%)': '#2ca02c'
    }

    fig = px.line(df_acc_plot, x='period', y='acc_ret', color='serie', markers=True,
                  labels={'period':'Período','acc_ret':'Rentabilidade acumulada'}, color_discrete_map=color_map)
    ymin = min(df_acc_plot['acc_ret'].min()*1.1 if not df_acc_plot['acc_ret'].isna().all() else 0, -0.2)
    ymax = max(df_acc_plot['acc_ret'].max()*1.1 if not df_acc_plot['acc_ret'].isna().all() else 0, 0.6)
    fig.update_yaxes(tickformat=".0%", range=[ymin, ymax])
    fig.update_layout(title="Gráfico de rentabilidade acumulada dos últimos 12 meses",
                      legend_title_text="Séries", height=420, margin=dict(t=40,b=40,l=40,r=40))
    fig.update_traces(hovertemplate='%{x|%b/%y}: %{y:.2%}')
    st.plotly_chart(fig, width='stretch')

    # ---------------- Visão Geral tab ----------------
    with tab_visao:
        st.subheader("Visão Geral")

        # Evolução do patrimônio (global) — usar soma dos owners quando disponível
    if True:
        # tentar carregar snapshots e séries agregadas
        try:
            with engine.connect() as conn:
                df_snapshots = pd.read_sql("SELECT * FROM net_worth_snapshots", conn)
        except Exception:
            df_snapshots = pd.DataFrame()

        # carregar séries agregadas (soma de patrimônios por período)
        try:
            df_total_series = aggregate_total()  # já retorna period, patrimonio, cdi, ipca, ibov, usd, carteira
        except Exception:
            df_total_series = pd.DataFrame()

        # preparar df_plot com prioridade: snapshots (diário) se existirem, senão séries mensais expandidas
        if not df_snapshots.empty:
            # usar snapshots para série diária real
            df_evol_real = df_snapshots.sort_values("snapshot_date").rename(columns={"snapshot_date": "date", "net_worth": "Patrimônio"})
            df_evol_plot = df_evol_real[["date", "Patrimônio"]].copy()

            # se tivermos séries agregadas, adicionar CDI e Renda Fixa a partir de df_total_series (interpolando mensal -> diário)
            if not df_total_series.empty and "period" in df_total_series.columns:
                # transformar period (primeiro dia do mês) em índice e reindexar para datas diárias
                df_idx = df_total_series.copy()
                df_idx['period'] = pd.to_datetime(df_idx['period'])
                df_idx = df_idx.set_index('period').sort_index()
                # criar índice diário cobrindo o intervalo dos snapshots
                date_index = pd.date_range(start=df_evol_plot['date'].min(), end=df_evol_plot['date'].max(), freq='D')
                df_daily_idx = df_idx.reindex(df_idx.index.union(date_index)).sort_index()
                # forward-fill/linear interpolate índices numéricos
                for col in ['cdi', 'ipca', 'ibov', 'usd', 'carteira']:
                    if col in df_daily_idx.columns:
                        df_daily_idx[col] = pd.to_numeric(df_daily_idx[col], errors='coerce').interpolate(method='time').ffill().bfill()
                # extrair apenas o índice diário alinhado ao date_index
                df_indices_daily = df_daily_idx.reindex(date_index).reset_index().rename(columns={'index':'date'})
                # anexar CDI e Carteira (mocks) ao df_evol_plot por join em date
                df_evol_plot = df_evol_plot.merge(df_indices_daily[['date','cdi','carteira']], on='date', how='left')
            else:
                # sem séries agregadas, criar mocks simples para comparação
                base = float(df_evol_plot["Patrimônio"].iloc[0])
                n = len(df_evol_plot)
                df_evol_plot["cdi"] = base * (1 + np.cumsum(np.repeat(0.0003, n)))
                df_evol_plot["carteira"] = df_evol_plot["Patrimônio"]
        else:
            # sem snapshots: tentar usar df_total_series (mensal) e expandir para diário
            if not df_total_series.empty and "period" in df_total_series.columns:
                df_ts = df_total_series.copy()
                df_ts['period'] = pd.to_datetime(df_ts['period'])
                # usar patrimonio agregado como série principal
                df_ts = df_ts[['period','patrimonio','cdi','carteira']].rename(columns={'period':'date','patrimonio':'Patrimônio'})
                # expandir para diário por forward-fill
                df_ts = df_ts.set_index('date').sort_index()
                date_index = pd.date_range(start=df_ts.index.min(), end=df_ts.index.max(), freq='D')
                df_daily = df_ts.reindex(df_ts.index.union(date_index)).sort_index()
                df_daily[['Patrimônio','cdi','carteira']] = df_daily[['Patrimônio','cdi','carteira']].interpolate(method='time').ffill().bfill()
                df_evol_plot = df_daily.reindex(date_index).reset_index().rename(columns={'index':'date'})
            else:
                # fallback: gerar mocks diários
                np.random.seed(42)
                today = datetime.today().date()
                dates = pd.date_range(end=today, periods=180).to_pydatetime().tolist()
                patrimonio = np.cumsum(np.random.normal(loc=50, scale=200, size=len(dates))) + 100000
                cdi = 100000 * (1 + np.cumsum(np.repeat(0.0003, len(dates))))
                carteira = patrimonio.copy()
                df_evol_plot = pd.DataFrame({"date": dates, "Patrimônio": patrimonio, "cdi": cdi, "carteira": carteira})

        # garantir colunas numéricas
        for col in ["Patrimônio","cdi","carteira"]:
            if col in df_evol_plot.columns:
                df_evol_plot[col] = pd.to_numeric(df_evol_plot[col], errors='coerce')

        # montar gráfico: Patrimônio (soma) vs CDI (linha de referência) vs Carteira (índice)
        fig1 = go.Figure()
        if "Patrimônio" in df_evol_plot.columns:
            fig1.add_trace(go.Scatter(x=df_evol_plot['date'], y=df_evol_plot['Patrimônio'],
                                    mode='lines+markers', name='Patrimônio (soma)',
                                    line=dict(color='#1f77b4', width=2),
                                    hovertemplate='%{x|%Y-%m-%d}: %{y:$,.2f}<extra></extra>'))
        if "cdi" in df_evol_plot.columns:
            # se cdi estiver em variação (pct), transformar para escala de patrimônio se necessário; aqui assumimos já em nível compatível
            fig1.add_trace(go.Scatter(x=df_evol_plot['date'], y=df_evol_plot['cdi'],
                                    mode='lines', name='CDI (referência)',
                                    line=dict(color='#ff7f0e', width=2, dash='dash'),
                                    hovertemplate='%{x|%Y-%m-%d}: %{y:$,.2f}<extra></extra>'))
        if "carteira" in df_evol_plot.columns:
            fig1.add_trace(go.Scatter(x=df_evol_plot['date'], y=df_evol_plot['carteira'],
                                    mode='lines', name='Carteira (índice)',
                                    line=dict(color='#636efa', width=2, dash='dot'),
                                    hovertemplate='%{x|%Y-%m-%d}: %{y:$,.2f}<extra></extra>'))

        fig1.update_layout(title="Evolução do Patrimônio vs CDI / Carteira",
                        xaxis_title="Data", yaxis_title="Valor (R$)",
                        height=420, legend_title_text="Séries", margin=dict(t=40,b=40,l=40,r=40))
        st.caption("1) Evolução do Patrimônio vs CDI / Carteira (soma dos owners quando disponível)")
        st.plotly_chart(fig1, use_container_width=True)
        st.markdown("---")

#----------------- Fluxo de Caixa tab ----------------
with tab_cash:
    render_cash_ui()

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
    st.caption("1) Evolução do Patrimônio vs CDI | IPCA  | IBOV")
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