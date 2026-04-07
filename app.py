# app.py
import streamlit as st
import pandas as pd
import numpy as np
import time
import uuid
from datetime import datetime, timedelta
from db import engine
from etl.normalizers import load_mapping, apply_mapping, validate_required
from etl.writer import write_staging, promote_merge_sqlite, record_upload_result, record_upload_error
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import text
from babel.numbers import format_currency, format_decimal

def format_brl(value):
    try:
        v = float(value)
    except Exception:
        return value
    s = f"{v:,.2f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"

st.set_page_config(page_title="StreamDash — Finanças Pessoais", layout="wide")
st.title("StreamDash — Finanças Pessoais")

# ---------------- Sidebar: upload e formulários ----------------
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

st.sidebar.markdown("---")
st.sidebar.subheader("Adicionar bem / ativo")
with st.sidebar.form("form_add_asset", clear_on_submit=True):
    a_categoria = st.selectbox("Categoria", ["Imóveis","Bens Móveis","Empresas","Novos Negócios","Investimentos","Outros"], key="asset_cat")
    a_descricao = st.text_input("Descrição", key="asset_desc")
    a_valor = st.number_input("Valor (R$)", min_value=0.0, step=100.0, format="%.2f", key="asset_val")
    submitted_asset = st.form_submit_button("Adicionar ativo")
    if submitted_asset:
        import_batch_id = f"manual_asset_{int(time.time())}_{uuid.uuid4().hex[:6]}"
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO assets (categoria, descricao, valor, source, import_batch_id)
                VALUES (:categoria, :descricao, :valor, :source, :ib)
            """), {"categoria": a_categoria, "descricao": a_descricao, "valor": a_valor, "source": "manual_form", "ib": import_batch_id})
        st.sidebar.success("Ativo adicionado com sucesso.")
        st.experimental_rerun()

st.sidebar.markdown("---")
st.sidebar.subheader("Adicionar passivo / dívida")
with st.sidebar.form("form_add_liability", clear_on_submit=True):
    l_categoria = st.selectbox("Categoria (passivo)", ["Financiamento Imobiliário","Empréstimo Pessoal","Cartão de Crédito","Outros"], key="liab_cat")
    l_descricao = st.text_input("Descrição do passivo", key="liab_desc")
    l_valor = st.number_input("Valor (R$)", min_value=0.0, step=100.0, format="%.2f", key="liab_val")
    submitted_liab = st.form_submit_button("Adicionar passivo")
    if submitted_liab:
        import_batch_id = f"manual_liab_{int(time.time())}_{uuid.uuid4().hex[:6]}"
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO liabilities (categoria, descricao, valor, source, import_batch_id)
                VALUES (:categoria, :descricao, :valor, :source, :ib)
            """), {"categoria": l_categoria, "descricao": l_descricao, "valor": l_valor, "source": "manual_form", "ib": import_batch_id})
        st.sidebar.success("Passivo adicionado com sucesso.")
        st.experimental_rerun()

st.sidebar.markdown("---")
st.sidebar.subheader("Editar / Remover ativos e passivos")

# utilitários para leitura
def read_table(table_name):
    try:
        with engine.connect() as conn:
            return pd.read_sql(f"SELECT * FROM {table_name}", conn)
    except Exception:
        return pd.DataFrame()

df_assets_manual = read_table("assets")
df_liabilities_manual = read_table("liabilities")

# Editar / remover ativos
if not df_assets_manual.empty:
    asset_options = df_assets_manual.apply(lambda r: f"{int(r.id)} | {r.categoria} | {r.descricao} | R$ {float(r.valor):,.2f}", axis=1).tolist()
    selected_asset = st.sidebar.selectbox("Selecionar ativo para editar/remover", [""] + asset_options, key="sel_asset")
    if selected_asset:
        sel_id = int(selected_asset.split("|")[0].strip())
        sel_row = df_assets_manual[df_assets_manual["id"] == sel_id].iloc[0]
        with st.sidebar.form("form_edit_asset", clear_on_submit=False):
            e_categoria = st.selectbox("Categoria", ["Imóveis","Bens Móveis","Empresas","Novos Negócios","Investimentos","Outros"], index=["Imóveis","Bens Móveis","Empresas","Novos Negócios","Investimentos","Outros"].index(sel_row.categoria) if sel_row.categoria in ["Imóveis","Bens Móveis","Empresas","Novos Negócios","Investimentos","Outros"] else 0)
            e_descricao = st.text_input("Descrição", value=sel_row.descricao)
            e_valor = st.number_input("Valor (R$)", min_value=0.0, step=100.0, value=float(sel_row.valor))
            btn_update_asset = st.form_submit_button("Atualizar ativo")
            btn_delete_asset = st.form_submit_button("Remover ativo")
            if btn_update_asset:
                with engine.begin() as conn:
                    conn.execute(text("""
                        UPDATE assets SET categoria=:categoria, descricao=:descricao, valor=:valor WHERE id=:id
                    """), {"categoria": e_categoria, "descricao": e_descricao, "valor": e_valor, "id": sel_id})
                st.sidebar.success("Ativo atualizado.")
                st.experimental_rerun()
            if btn_delete_asset:
                with engine.begin() as conn:
                    conn.execute(text("DELETE FROM assets WHERE id = :id"), {"id": sel_id})
                st.sidebar.success("Ativo removido.")
                st.experimental_rerun()
else:
    st.sidebar.info("Nenhum ativo manual registrado.")

# Editar / remover passivos
if not df_liabilities_manual.empty:
    liab_options = df_liabilities_manual.apply(lambda r: f"{int(r.id)} | {r.categoria} | {r.descricao} | R$ {float(r.valor):,.2f}", axis=1).tolist()
    selected_liab = st.sidebar.selectbox("Selecionar passivo para editar/remover", [""] + liab_options, key="sel_liab")
    if selected_liab:
        sel_id = int(selected_liab.split("|")[0].strip())
        sel_row = df_liabilities_manual[df_liabilities_manual["id"] == sel_id].iloc[0]
        with st.sidebar.form("form_edit_liab", clear_on_submit=False):
            e_categoria = st.selectbox("Categoria (passivo)", ["Financiamento Imobiliário","Empréstimo Pessoal","Cartão de Crédito","Outros"], index=["Financiamento Imobiliário","Empréstimo Pessoal","Cartão de Crédito","Outros"].index(sel_row.categoria) if sel_row.categoria in ["Financiamento Imobiliário","Empréstimo Pessoal","Cartão de Crédito","Outros"] else 0)
            e_descricao = st.text_input("Descrição do passivo", value=sel_row.descricao)
            e_valor = st.number_input("Valor (R$)", min_value=0.0, step=100.0, value=float(sel_row.valor), key="edit_liab_val")
            btn_update_liab = st.form_submit_button("Atualizar passivo")
            btn_delete_liab = st.form_submit_button("Remover passivo")
            if btn_update_liab:
                with engine.begin() as conn:
                    conn.execute(text("""
                        UPDATE liabilities SET categoria=:categoria, descricao=:descricao, valor=:valor WHERE id=:id
                    """), {"categoria": e_categoria, "descricao": e_descricao, "valor": e_valor, "id": sel_id})
                st.sidebar.success("Passivo atualizado.")
                st.experimental_rerun()
            if btn_delete_liab:
                with engine.begin() as conn:
                    conn.execute(text("DELETE FROM liabilities WHERE id = :id"), {"id": sel_id})
                st.sidebar.success("Passivo removido.")
                st.experimental_rerun()
else:
    st.sidebar.info("Nenhum passivo manual registrado.")

# ---------------- Main page: gráficos e composição ----------------
st.subheader("Visão Geral")

# Tentar ler transactions e snapshots do DB
df_transactions = read_table("transactions")
df_snapshots = read_table("net_worth_snapshots")

# Se houver snapshots, usar para evolução do patrimônio; senão, tentar construir a partir de holdings + assets
if not df_snapshots.empty:
    df_evol_real = df_snapshots.sort_values("snapshot_date").rename(columns={"snapshot_date":"date","net_worth":"Patrimônio"})
    df_evol_plot = df_evol_real[["date","Patrimônio"]].copy()
    # gerar séries comparativas mock (CDI, Renda Fixa, INDJ26) com base no primeiro snapshot
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
    # fallback para mocks (como antes)
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

# Plot 1: evolução do patrimônio
st.caption("1) Evolução do Patrimônio vs CDI / Renda Fixa / IBOV")
fig1 = px.line(df_plot, x="date", y="valor", color="serie",
               labels={"date":"Data","valor":"Valor (R$)","serie":"Série"},
               hover_data={"date":True,"serie":True,"valor":":.2f"})
fig1.update_layout(height=420, legend_title_text="Séries", margin=dict(t=40, b=40, l=40, r=40))
st.plotly_chart(fig1, use_container_width=True)
st.markdown("---")

# Fluxo de caixa: se houver transactions, agregue por dia; senão mock
st.caption("2) Fluxo de Caixa Diário")
if not df_transactions.empty:
    df_tx = df_transactions.copy()
    # assumir coluna date e amount
    df_tx["date"] = pd.to_datetime(df_tx["date"], errors="coerce").dt.date
    df_daily = df_tx.groupby("date").agg(entradas=("amount", lambda s: s[s>0].sum() if not s[s>0].empty else 0.0),
                                         saidas=("amount", lambda s: -s[s<0].sum() if not s[s<0].empty else 0.0)).reset_index()
    df_daily = df_daily.sort_values("date").tail(180)
    fig2 = go.Figure()
    fig2.add_trace(go.Bar(x=df_daily["date"], y=df_daily["entradas"], name="Entradas", marker_color="#2ca02c"))
    fig2.add_trace(go.Bar(x=df_daily["date"], y=df_daily["saidas"], name="Saídas", marker_color="#d62728"))
    fig2.update_layout(barmode='group', xaxis_title="Data", yaxis_title="Valor (R$)", height=420, margin=dict(t=40, b=40, l=40, r=40))
    st.plotly_chart(fig2, use_container_width=True)
else:
    fig2 = go.Figure()
    dates = pd.date_range(end=datetime.today().date(), periods=180).to_pydatetime().tolist()
    entradas = np.random.poisson(2, len(dates)) * np.random.uniform(50, 500, len(dates))
    saidas = np.random.poisson(3, len(dates)) * np.random.uniform(20, 400, len(dates))
    fig2.add_trace(go.Bar(x=dates, y=entradas, name="Entradas", marker_color="#2ca02c"))
    fig2.add_trace(go.Bar(x=dates, y=saidas, name="Saídas", marker_color="#d62728"))
    fig2.update_layout(barmode='group', xaxis_title="Data", yaxis_title="Valor (R$)", height=420, margin=dict(t=40, b=40, l=40, r=40))
    st.plotly_chart(fig2, use_container_width=True)
st.markdown("---")

# Despesas por categoria: se transactions tiver category, agregue; senão mock
st.caption("3) Principais despesas por categoria")
if not df_transactions.empty and "category" in df_transactions.columns:
    df_tx = df_transactions.copy()
    df_tx["amount"] = pd.to_numeric(df_tx["amount"], errors="coerce").fillna(0.0)
    # considerar despesas como amount < 0 or type == 'out'
    df_tx["expense"] = df_tx["amount"].apply(lambda x: -x if x < 0 else 0.0)
    df_cat = df_tx.groupby("category", as_index=False)["expense"].sum()
    df_cat = df_cat[df_cat["expense"] > 0].sort_values("expense", ascending=False).head(10)
    if df_cat.empty:
        st.info("Nenhuma despesa categorizada encontrada.")
    else:
        fig3 = px.pie(df_cat, names="category", values="expense", hole=0.35, labels={"category":"Categoria","expense":"Valor (R$)"})
        fig3.update_traces(textposition='inside', textinfo='percent+label')
        fig3.update_layout(height=420, margin=dict(t=40, b=40, l=40, r=40))
        st.plotly_chart(fig3, use_container_width=True)
else:
    fig3 = px.pie(pd.DataFrame({"categoria": ["Alimentação","Moradia","Transporte","Lazer","Saúde","Outros"], "valor": np.random.dirichlet(np.ones(6))*5000}),
                  names="categoria", values="valor", hole=0.35)
    fig3.update_traces(textposition='inside', textinfo='percent+label')
    fig3.update_layout(height=420, margin=dict(t=40, b=40, l=40, r=40))
    st.plotly_chart(fig3, use_container_width=True)

st.markdown("---")

# Pequena tabela resumo (robust)
st.markdown("### Resumo")

# Saldo total (preferir snapshots, senão série de patrimônio, senão 0)
if not df_snapshots.empty:
    latest = df_snapshots.sort_values("snapshot_date").iloc[-1]
    saldo_total = float(latest.net_worth)
else:
    try:
        saldo_total = float(df_plot[df_plot['serie'] == 'Patrimônio']['valor'].iloc[-1])
    except Exception:
        saldo_total = 0.0

# Fluxo líquido últimos 30 dias (preferir df_daily se existir, senão tentar fluxo mock)
if 'df_daily' in locals():
    try:
        fluxo_30 = (df_daily['entradas'] - df_daily['saidas']).tail(30).sum()
    except Exception:
        fluxo_30 = 0.0
else:
    if 'fluxo' in locals():
        try:
            fluxo_30 = fluxo.tail(30)['saldo'].sum()
        except Exception:
            fluxo_30 = 0.0
    else:
        fluxo_30 = 0.0

# Despesas últimos 30 dias (preferir df_cat se existir, senão df_pie, senão 0)
if 'df_cat' in locals():
    try:
        despesas_30 = float(df_cat['expense'].sum())
    except Exception:
        despesas_30 = 0.0
elif 'df_pie' in locals():
    try:
        despesas_30 = float(df_pie['valor'].sum())
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
# ------------------ Composição Patrimonial | Bens e Investimentos ----------------
st.markdown("### Composição Patrimonial | Bens e Investimentos")

# Reusar df_assets_manual e df_liabilities_manual lidos no sidebar
# Se holdings existirem, mapear para ativos
df_holdings = read_table("holdings")
if df_holdings.empty and df_assets_manual.empty and df_liabilities_manual.empty:
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
    df_assets = pd.DataFrame(assets)
    df_liab = pd.DataFrame(liabilities)
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
    if not df_assets_manual.empty:
        parts.append(df_assets_manual[["categoria","descricao","valor"]])
    df_assets = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=["categoria","descricao","valor"])
    df_liab = df_liabilities_manual[["categoria","descricao","valor"]] if not df_liabilities_manual.empty else pd.DataFrame(columns=["categoria","descricao","valor"])

agg_assets = df_assets.groupby("categoria", as_index=False)["valor"].sum() if not df_assets.empty else pd.DataFrame(columns=["categoria","valor"])
agg_liab = df_liab.groupby("categoria", as_index=False)["valor"].sum() if not df_liab.empty else pd.DataFrame(columns=["categoria","valor"])

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
    fig_assets_pie = px.pie(agg_assets, names="categoria", values="valor", hole=0.35,
                            title="Ativos por categoria", labels={"valor":"Valor (R$)","categoria":"Categoria"})
    fig_assets_pie.update_traces(textinfo="percent+label")
    st.plotly_chart(fig_assets_pie, use_container_width=True)
else:
    st.info("Nenhum ativo registrado ainda.")

st.markdown("#### Detalhe de ativos")
if not df_assets.empty:
    st.dataframe(df_assets.assign(valor=lambda d: d["valor"].map(lambda v: f"R$ {float(v):,.2f}")), use_container_width=True)
else:
    st.write("Nenhum ativo detalhado disponível.")

st.markdown("#### Distribuição de passivos por categoria")
if not agg_liab.empty:
    fig_liab = px.bar(agg_liab, x="categoria", y="valor", labels={"valor":"Valor (R$)","categoria":"Categoria"},
                      title="Passivos por categoria", text="valor")
    fig_liab.update_traces(texttemplate="R$ %{y:,.0f}")
    fig_liab.update_layout(yaxis_tickformat=",.0f")
    st.plotly_chart(fig_liab, use_container_width=True)
else:
    st.info("Nenhum passivo registrado ainda.")

st.markdown("#### Detalhe de passivos")
if not df_liab.empty:
    st.dataframe(df_liab.assign(valor=lambda d: d["valor"].map(lambda v: f"R$ {float(v):,.2f}")), use_container_width=True)
else:
    st.write("Nenhum passivo detalhado disponível.")

st.markdown("---")
st.info("Formulários e controles de edição estão no sidebar. Para produção, adicione autenticação, validação e backups.")
