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

# Import rendering functions from modules
from ips import render_ips
from investment import render_controle, list_assets, list_liabilities, add_asset, add_liability

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

# ---------------- Sidebar: upload and forms ----------------
st.sidebar.title("Adolfo Pacheco")
st.sidebar.header("Upload e processamento")
uploaded = st.sidebar.file_uploader("Envie CSV ou XLSX (um por vez)", type=["csv","xlsx"])
mapping_df = load_mapping("mappings.csv")

if uploaded:
    file_name = uploaded.name
    import_batch_id = f"{int(time.time())}_{uuid.uuid4().hex[:6]}"
    try:
        if file_name.lower().endswith(".csv"):
            df_raw = pd.read_csv(uploaded)
        else:
            df_raw = pd.read_excel(uploaded)
    except Exception as e:
        st.sidebar.error(f"Falha ao ler arquivo: {e}")
        df_raw = None

    if df_raw is not None:
        st.sidebar.info(f"Lido {len(df_raw)} linhas")
        st.sidebar.write("Preview:")
        st.sidebar.dataframe(df_raw.head())

        stg_table = f"stg_{file_name.split('.')[0].lower()}"
        rows_staged = write_staging(df_raw, stg_table, import_batch_id, file_name)

        norm = apply_mapping(df_raw, mapping_df, file_name)
        missing = validate_required(norm, mapping_df, file_name)
        if missing:
            msg = f"Missing required columns: {missing}"
            st.sidebar.error(msg)
            record_upload_error(import_batch_id, file_name, "MISSING_REQUIRED", msg)
            record_upload_result(import_batch_id, file_name, rows_staged, 0, "failed")
        else:
            rows_promoted = promote_merge_sqlite(norm, import_batch_id, file_name)
            record_upload_result(import_batch_id, file_name, rows_staged, rows_promoted, "processed")
            st.sidebar.success(f"Arquivo processado. Linhas promovidas: {rows_promoted}")

# ---------------- Sidebar: quick manual add forms ----------------
st.sidebar.markdown("---")
st.sidebar.subheader("Adicionar bem / ativo (rápido)")
with st.sidebar.form("form_add_asset_quick", clear_on_submit=True):
    a_categoria = st.selectbox("Categoria", ["Imóveis","Bens Móveis","Empresas","Novos Negócios","Investimentos","Outros"], key="asset_cat_quick")
    a_descricao = st.text_input("Descrição", key="asset_desc_quick")
    a_valor = st.number_input("Valor (R$)", min_value=0.0, step=100.0, format="%.2f", key="asset_val_quick")
    submitted_asset = st.form_submit_button("Adicionar ativo")
    if submitted_asset:
        import_batch_id = f"manual_asset_{int(time.time())}_{uuid.uuid4().hex[:6]}"
        add_asset(a_categoria, a_descricao, a_valor, source="manual_form", import_batch_id=import_batch_id)
        st.sidebar.success("Ativo adicionado com sucesso.")
        st.experimental_rerun()

st.sidebar.markdown("---")
st.sidebar.subheader("Adicionar passivo / dívida (rápido)")
with st.sidebar.form("form_add_liability_quick", clear_on_submit=True):
    l_categoria = st.selectbox("Categoria (passivo)", ["Financiamento Imobiliário","Empréstimo Pessoal","Cartão de Crédito","Outros"], key="liab_cat_quick")
    l_descricao = st.text_input("Descrição do passivo", key="liab_desc_quick")
    l_valor = st.number_input("Valor (R$)", min_value=0.0, step=100.0, format="%.2f", key="liab_val_quick")
    submitted_liab = st.form_submit_button("Adicionar passivo")
    if submitted_liab:
        import_batch_id = f"manual_liab_{int(time.time())}_{uuid.uuid4().hex[:6]}"
        add_liability(l_categoria, l_descricao, l_valor, source="manual_form", import_batch_id=import_batch_id)
        st.sidebar.success("Passivo adicionado com sucesso.")
        st.experimental_rerun()

# ---------------- Page navigation ----------------
page = st.sidebar.radio("Navegação", ["Visão Geral", "IPS", "Controle de Investimentos"], index=0)

# ---------------- Visão Geral (keeps main dashboard) ----------------
if page == "Visão Geral":
    # Minimal orchestration: reuse the existing logic in app for the dashboard
    st.subheader("Visão Geral")

    # Try to read transactions and snapshots
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

    # Build df_plot (snapshots or mock)
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

    # Plot 1
    st.caption("1) Evolução do Patrimônio vs CDI / Renda Fixa / IBOV")
    fig1 = px.line(df_plot, x="date", y="valor", color="serie",
                   labels={"date":"Data","valor":"Valor (R$)","serie":"Série"})
    fig1.update_layout(height=420, legend_title_text="Séries", margin=dict(t=40, b=40, l=40, r=40))
    st.plotly_chart(fig1, use_container_width=True)
    st.markdown("---")

    # Fluxo de caixa
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

    # Despesas por categoria
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

    # Resumo e composição patrimonial (keeps previous logic)
    # ... (the rest of the Visão Geral composition code can remain here as before) ...
    # For brevity, reuse the composition logic from your previous app version.

# ---------------- IPS page ----------------
elif page == "IPS":
    render_ips()  # function implemented in ips.py

# ---------------- Controle de Investimentos page ----------------
elif page == "Controle de Investimentos":
    render_controle()  # function implemented in investment.py