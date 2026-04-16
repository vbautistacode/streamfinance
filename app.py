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
tab_visao, tab_cash, tab_controle, tab_ips = st.tabs(
    ["Início", "Fluxo de Caixa", "Controle de Investimentos", "IPS"]
)

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

# ---------------- Data loaders (centralized) ----------------
def load_transactions():
    try:
        with engine.connect() as conn:
            return pd.read_sql("SELECT * FROM transactions", conn)
    except Exception:
        return pd.DataFrame()

def load_snapshots():
    try:
        with engine.connect() as conn:
            return pd.read_sql("SELECT * FROM net_worth_snapshots", conn)
    except Exception:
        return pd.DataFrame()

def load_total_series():
    try:
        return aggregate_total()
    except Exception:
        return pd.DataFrame()

# ---------------- Performance infographic (logic only) ----------------
def render_performance_infographic(owner_a="Paula Casale", owner_b="Adolfo Pacheco", months=12):
    """
    Exibe tabela de rentabilidades dos últimos `months` meses e gráfico de rentabilidade acumulada
    comparando Carteira (soma de owner_a + owner_b), CDI, IBOV, IPCA e Dólar.
    """
    df_a = list_entries(owner_a)
    df_b = list_entries(owner_b)
    df_total = load_total_series()

    def prepare(df):
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.copy()
        df['period'] = pd.to_datetime(df['period'], errors='coerce')
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
    unique_sorted = pd.Series(periods_series.unique()).dropna().astype('datetime64[ns]').sort_values()
    last_periods = unique_sorted.iloc[-months:].tolist()

    # build sum of patrimônios
    def build_sum_pat(df1, df2):
        if (df1 is None or df1.empty) and (df2 is None or df2.empty):
            return pd.DataFrame()
        parts = []
        if df1 is not None and not df1.empty:
            parts.append(df1[['period','patrimonio']].rename(columns={'patrimonio':'pat1'}))
        if df2 is not None and not df2.empty:
            parts.append(df2[['period','patrimonio']].rename(columns={'patrimonio':'pat2'}))
        if not parts:
            return pd.DataFrame()
        if len(parts) == 1:
            merged = parts[0].copy()
            # unify column to pat_soma
            col = merged.columns[1]
            merged = merged.rename(columns={col: 'pat_soma'})
            merged['pat_soma'] = pd.to_numeric(merged['pat_soma'], errors='coerce').fillna(0.0)
            merged['period'] = pd.to_datetime(merged['period'], errors='coerce')
            return merged.set_index('period')[['pat_soma']].sort_index()
        # two parts: join by period
        m1 = parts[0].set_index('period')
        m2 = parts[1].set_index('period')
        merged = m1.join(m2, how='outer')
        merged = merged.reset_index().rename(columns={'index':'period'})
        merged['pat1'] = pd.to_numeric(merged.get('pat1', 0), errors='coerce').fillna(0.0)
        merged['pat2'] = pd.to_numeric(merged.get('pat2', 0), errors='coerce').fillna(0.0)
        merged['period'] = pd.to_datetime(merged['period'], errors='coerce')
        merged['pat_soma'] = merged['pat1'] + merged['pat2']
        return merged.set_index('period')[['pat_soma']].sort_index()

    # build df_soma and normalize safely
    df_soma = build_sum_pat(df_a, df_b)

    # ensure carteira_soma_df exists and is safe
    carteira_soma_df = pd.DataFrame()
    if isinstance(df_soma, pd.DataFrame) and not df_soma.empty:
        # ensure datetime index
        if not pd.api.types.is_datetime64_any_dtype(df_soma.index):
            if 'period' in df_soma.columns:
                df_soma['period'] = pd.to_datetime(df_soma['period'], errors='coerce')
                df_soma = df_soma.set_index('period')
            else:
                try:
                    df_soma.index = pd.to_datetime(df_soma.index, errors='coerce')
                except Exception:
                    pass
        # normalize last_periods to DatetimeIndex
        lp = pd.to_datetime(last_periods, errors='coerce')
        lp = lp[~pd.isna(lp)]
        if len(lp) > 0:
            target_index = pd.DatetimeIndex(lp)
            try:
                df_soma = df_soma.reindex(target_index).sort_index().fillna(method='ffill').fillna(0.0)
            except Exception:
                try:
                    df_soma.index = pd.to_datetime(df_soma.index, errors='coerce')
                    df_soma = df_soma.reindex(target_index).sort_index().fillna(method='ffill').fillna(0.0)
                except Exception:
                    df_soma = pd.DataFrame(index=target_index)
        # compute returns if numeric column exists
        if 'pat_soma' in df_soma.columns:
            carteira_soma_ret = df_soma['pat_soma'].pct_change().fillna(0.0).rename('Carteira (soma) (%)')
            carteira_soma_df = carteira_soma_ret.to_frame()
        else:
            numeric_cols = [c for c in df_soma.columns if pd.api.types.is_numeric_dtype(df_soma[c])]
            if numeric_cols:
                carteira_soma_ret = df_soma[numeric_cols[0]].pct_change().fillna(0.0).rename('Carteira (soma) (%)')
                carteira_soma_df = carteira_soma_ret.to_frame()
            else:
                carteira_soma_df = pd.DataFrame()

    # monthly returns from individual patrimonios
    def monthly_returns_from_pat(df, label):
        if df.empty:
            return pd.DataFrame()
        d = df[df['period'].isin(last_periods)][['period','patrimonio']].dropna().set_index('period').sort_index()
        if d.empty:
            return pd.DataFrame()
        ret = d['patrimonio'].pct_change().fillna(0.0).rename(label)
        return ret.to_frame()

    paula_ret = monthly_returns_from_pat(df_a, "Paula (%)")
    adolfo_ret = monthly_returns_from_pat(df_b, "Adolfo (%)")

    # index returns (CDI, IPCA, etc.)
    def index_returns(df, col, label):
        if df.empty or col not in df.columns:
            return pd.DataFrame()
        d = df[df['period'].isin(last_periods)][['period', col]].set_index('period').sort_index()
        sample = d[col].dropna().head(5)
        if not sample.empty:
            meanv = sample.abs().mean()
            if meanv > 2:
                d[col] = d[col] / 100.0
        d = d.rename(columns={col: label})
        return d

    cdi = index_returns(df_total, 'cdi', 'CDI (%)')
    ipca = index_returns(df_total, 'ipca', 'IPCA (%)')
    ibov = index_returns(df_total, 'ibov', 'IBOV (%)')
    usd = index_returns(df_total, 'usd', 'USD (%)')
    carteira_idx = index_returns(df_total, 'carteira', 'Carteira idx (%)')

    # assemble parts safely
    parts = []
    if not carteira_soma_df.empty:
        parts.append(carteira_soma_df)
    elif not carteira_idx.empty:
        parts.append(carteira_idx)
    parts += [paula_ret, adolfo_ret, cdi, ibov, ipca, usd]
    # filter out empty frames
    parts = [p for p in parts if isinstance(p, pd.DataFrame) and not p.empty]
    if parts:
        df_monthly = pd.concat(parts, axis=1)
        # ensure index covers last_periods in order
        try:
            target_idx = pd.DatetimeIndex(pd.to_datetime(last_periods, errors='coerce'))
            target_idx = target_idx[~pd.isna(target_idx)]
            if len(target_idx) > 0:
                df_monthly = df_monthly.reindex(target_idx).sort_index().fillna(np.nan)
        except Exception:
            df_monthly = df_monthly.sort_index().fillna(np.nan)
    else:
        # empty structure with index = last_periods
        idx = pd.DatetimeIndex(pd.to_datetime(last_periods, errors='coerce'))
        idx = idx[~pd.isna(idx)]
        df_monthly = pd.DataFrame(index=idx)

    df_monthly.index.name = 'period'

    def fmt_cell(v):
        return f"{v*100:.2f}%" if pd.notna(v) else ""
    df_monthly_display = df_monthly.copy()
    if not df_monthly_display.empty:
        df_monthly_display = df_monthly_display.apply(lambda col: col.map(fmt_cell))

    st.markdown("#### Histórico de rentabilidade dos últimos 12 meses (%)")
    if not df_monthly_display.empty:
        st.table(df_monthly_display.T)
    else:
        st.info("Nenhum dado de rentabilidade mensal disponível para exibir.")

    # acumulado
    if not df_monthly.empty:
        df_acc = (1 + df_monthly.fillna(0)).cumprod() - 1
        df_acc_plot = df_acc.reset_index().melt(id_vars=['period'], var_name='serie', value_name='acc_ret')
        # normalize series names
        df_acc_plot['serie'] = df_acc_plot['serie'].replace({'pat_soma':'Carteira (soma) (%)'})
    else:
        df_acc_plot = pd.DataFrame(columns=['period','serie','acc_ret'])

    color_map = {
        'Carteira (soma) (%)': '#1f77b4',
        'Carteira idx (%)': '#636efa',
        'Paula (%)': '#1f77b4',
        'Adolfo (%)': '#17becf',
        'CDI (%)': '#ff7f0e',
        'IBOV (%)': '#003f5c',
        'IPCA (%)': '#e377c2',
        'USD (%)': '#2ca02c'
    }

    if not df_acc_plot.empty:
        fig = px.line(df_acc_plot, x='period', y='acc_ret', color='serie', markers=True,
                      labels={'period':'Período','acc_ret':'Rentabilidade acumulada'}, color_discrete_map=color_map)
        ymin = min(df_acc_plot['acc_ret'].min()*1.1 if not df_acc_plot['acc_ret'].isna().all() else 0, -0.2)
        ymax = max(df_acc_plot['acc_ret'].max()*1.1 if not df_acc_plot['acc_ret'].isna().all() else 0, 0.6)
        fig.update_yaxes(tickformat=".0%", range=[ymin, ymax])
        fig.update_layout(title="Gráfico de rentabilidade acumulada dos últimos 12 meses",
                          legend_title_text="Séries", height=420, margin=dict(t=40,b=40,l=40,r=40))
        fig.update_traces(hovertemplate='%{x|%b/%y}: %{y:.2%}')
        st.plotly_chart(fig, width='stretch')
    else:
        st.info("Nenhum dado para plotar o gráfico de rentabilidade acumulada.")

# ---------------- Visão Geral renderer (single entry point) ----------------
def render_visao_geral():
    st.subheader("Visão Geral")

    # load shared data once
    df_transactions = load_transactions()
    df_snapshots = load_snapshots()
    df_total_series = load_total_series()

    # --- Evolução do patrimônio (global) using aggregated series when available ---
    if not df_snapshots.empty:
        df_evol_real = df_snapshots.sort_values("snapshot_date").rename(columns={"snapshot_date":"date","net_worth":"Patrimônio"})
        df_evol_plot = df_evol_real[["date","Patrimônio"]].copy()
        if not df_total_series.empty and "period" in df_total_series.columns:
            df_idx = df_total_series.copy()
            df_idx['period'] = pd.to_datetime(df_idx['period'], errors='coerce')
            df_idx = df_idx.set_index('period').sort_index()
            date_index = pd.date_range(start=df_evol_plot['date'].min(), end=df_evol_plot['date'].max(), freq='D')
            df_daily_idx = df_idx.reindex(df_idx.index.union(date_index)).sort_index()
            for col in ['cdi', 'ipca', 'ibov', 'usd', 'carteira']:
                if col in df_daily_idx.columns:
                    df_daily_idx[col] = pd.to_numeric(df_daily_idx[col], errors='coerce').interpolate(method='time').ffill().bfill()
            df_indices_daily = df_daily_idx.reindex(date_index).reset_index().rename(columns={'index':'date'})
            df_evol_plot = df_evol_plot.merge(df_indices_daily[['date','cdi','carteira']], on='date', how='left')
        else:
            base = float(df_evol_plot["Patrimônio"].iloc[0]) if not df_evol_plot["Patrimônio"].isna().all() else 100000.0
            n = len(df_evol_plot)
            df_evol_plot["cdi"] = base * (1 + np.cumsum(np.repeat(0.0003, n)))
            df_evol_plot["carteira"] = df_evol_plot["Patrimônio"]
    else:
        if not df_total_series.empty and "period" in df_total_series.columns:
            df_ts = df_total_series.copy()
            df_ts['period'] = pd.to_datetime(df_ts['period'], errors='coerce')
            df_ts = df_ts[['period','patrimonio','cdi','carteira']].rename(columns={'period':'date','patrimonio':'Patrimônio'})
            df_ts = df_ts.set_index('date').sort_index()
            date_index = pd.date_range(start=df_ts.index.min(), end=df_ts.index.max(), freq='D')
            df_daily = df_ts.reindex(df_ts.index.union(date_index)).sort_index()
            df_daily[['Patrimônio','cdi','carteira']] = df_daily[['Patrimônio','cdi','carteira']].interpolate(method='time').ffill().bfill()
            df_evol_plot = df_daily.reindex(date_index).reset_index().rename(columns={'index':'date'})
        else:
            np.random.seed(42)
            today = datetime.today().date()
            dates = pd.date_range(end=today, periods=180).to_pydatetime().tolist()
            patrimonio = np.cumsum(np.random.normal(loc=50, scale=200, size=len(dates))) + 100000
            cdi = 100000 * (1 + np.cumsum(np.repeat(0.0003, len(dates))))
            carteira = patrimonio.copy()
            df_evol_plot = pd.DataFrame({"date": dates, "Patrimônio": patrimonio, "cdi": cdi, "carteira": carteira})

    for col in ["Patrimônio","cdi","carteira"]:
        if col in df_evol_plot.columns:
            df_evol_plot[col] = pd.to_numeric(df_evol_plot[col], errors='coerce')

    fig1 = go.Figure()
    if "Patrimônio" in df_evol_plot.columns:
        fig1.add_trace(go.Scatter(x=df_evol_plot['date'], y=df_evol_plot['Patrimônio'],
                                  mode='lines+markers', name='Patrimônio (soma)',
                                  line=dict(color='#1f77b4', width=2),
                                  hovertemplate='%{x|%Y-%m-%d}: %{y:$,.2f}<extra></extra>'))
    if "cdi" in df_evol_plot.columns:
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
    st.plotly_chart(fig1, width='stretch')
    st.markdown("---")

    # --- Performance infographic (tabela + acumulado) ---
    render_performance_infographic(owner_a="Paula Casale", owner_b="Adolfo Pacheco", months=12)
    st.markdown("---")

    # --- Fluxo de Caixa summary (small preview) ---
    st.caption("2) Fluxo de Caixa (resumo)")
    if not df_transactions.empty:
        df_tx = df_transactions.copy()
        if "date" in df_tx.columns:
            df_tx["date"] = pd.to_datetime(df_tx["date"], errors="coerce").dt.date
        if "amount" in df_tx.columns:
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
            st.plotly_chart(fig2, width='stretch')
        else:
            st.info("Tabela transactions encontrada, mas sem coluna 'amount' para sumarizar.")
    else:
        st.info("Nenhum dado de transações disponível para resumo de fluxo de caixa.")

    st.markdown("---")

# ---------------- Tabs: call renderers only inside their with blocks ----------------
with tab_visao:
    render_visao_geral()

with tab_cash:
    render_cash_ui()

with tab_controle:
    render_controle_ui()

with tab_ips:
    render_ips()