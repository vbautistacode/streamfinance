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

# Modules created for separation of concerns
from ips import load_latest_ips, save_ips, list_ips, delete_ips
from investment import (
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

st.sidebar.markdown("---")
st.sidebar.subheader("Adicionar bem / ativo")
with st.sidebar.form("form_add_asset", clear_on_submit=True):
    a_categoria = st.selectbox("Categoria", ["Imóveis","Bens Móveis","Empresas","Novos Negócios","Investimentos","Outros"], key="asset_cat")
    a_descricao = st.text_input("Descrição", key="asset_desc")
    a_valor = st.number_input("Valor (R$)", min_value=0.0, step=100.0, format="%.2f", key="asset_val")
    submitted_asset = st.form_submit_button("Adicionar ativo")
    if submitted_asset:
        import_batch_id = f"manual_asset_{int(time.time())}_{uuid.uuid4().hex[:6]}"
        add_asset(a_categoria, a_descricao, a_valor, source="manual_form", import_batch_id=import_batch_id)
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
        add_liability(l_categoria, l_descricao, l_valor, source="manual_form", import_batch_id=import_batch_id)
        st.sidebar.success("Passivo adicionado com sucesso.")
        st.experimental_rerun()

st.sidebar.markdown("---")
st.sidebar.subheader("Editar / Remover ativos e passivos")

# load manual assets/liabilities for sidebar controls
df_assets_manual = list_assets()
df_liabilities_manual = list_liabilities()

# Edit / remove assets
if not df_assets_manual.empty:
    asset_options = df_assets_manual.apply(lambda r: f"{int(r.id)} | {r.categoria} | {r.descricao} | {format_brl(r.valor)}", axis=1).tolist()
    selected_asset = st.sidebar.selectbox("Selecionar ativo para editar/remover", [""] + asset_options, key="sel_asset")
    if selected_asset:
        sel_id = int(selected_asset.split("|")[0].strip())
        sel_row = df_assets_manual[df_assets_manual["id"] == sel_id].iloc[0]
        with st.sidebar.form("form_edit_asset", clear_on_submit=False):
            categories = ["Imóveis","Bens Móveis","Empresas","Novos Negócios","Investimentos","Outros"]
            idx = categories.index(sel_row.categoria) if sel_row.categoria in categories else 0
            e_categoria = st.selectbox("Categoria", categories, index=idx)
            e_descricao = st.text_input("Descrição", value=sel_row.descricao)
            e_valor = st.number_input("Valor (R$)", min_value=0.0, step=100.0, value=float(sel_row.valor))
            btn_update_asset = st.form_submit_button("Atualizar ativo")
            btn_delete_asset = st.form_submit_button("Remover ativo")
            if btn_update_asset:
                update_asset(sel_id, e_categoria, e_descricao, e_valor)
                st.sidebar.success("Ativo atualizado.")
                st.experimental_rerun()
            if btn_delete_asset:
                delete_asset(sel_id)
                st.sidebar.success("Ativo removido.")
                st.experimental_rerun()
else:
    st.sidebar.info("Nenhum ativo manual registrado.")

# Edit / remove liabilities
if not df_liabilities_manual.empty:
    liab_options = df_liabilities_manual.apply(lambda r: f"{int(r.id)} | {r.categoria} | {r.descricao} | {format_brl(r.valor)}", axis=1).tolist()
    selected_liab = st.sidebar.selectbox("Selecionar passivo para editar/remover", [""] + liab_options, key="sel_liab")
    if selected_liab:
        sel_id = int(selected_liab.split("|")[0].strip())
        sel_row = df_liabilities_manual[df_liabilities_manual["id"] == sel_id].iloc[0]
        with st.sidebar.form("form_edit_liab", clear_on_submit=False):
            categories_l = ["Financiamento Imobiliário","Empréstimo Pessoal","Cartão de Crédito","Outros"]
            idx = categories_l.index(sel_row.categoria) if sel_row.categoria in categories_l else 0
            e_categoria = st.selectbox("Categoria (passivo)", categories_l, index=idx)
            e_descricao = st.text_input("Descrição do passivo", value=sel_row.descricao)
            e_valor = st.number_input("Valor (R$)", min_value=0.0, step=100.0, value=float(sel_row.valor), key="edit_liab_val")
            btn_update_liab = st.form_submit_button("Atualizar passivo")
            btn_delete_liab = st.form_submit_button("Remover passivo")
            if btn_update_liab:
                update_liability(sel_id, e_categoria, e_descricao, e_valor)
                st.sidebar.success("Passivo atualizado.")
                st.experimental_rerun()
            if btn_delete_liab:
                delete_liability(sel_id)
                st.sidebar.success("Passivo removido.")
                st.experimental_rerun()
else:
    st.sidebar.info("Nenhum passivo manual registrado.")

# ---------------- Page navigation ----------------
page = st.sidebar.radio("Navegação", ["Visão Geral", "IPS", "Controle de Investimentos"], index=0)

# ---------------- Visão Geral page ----------------
def render_visao_geral():
    st.subheader("Visão Geral")

    # Tentar ler transactions e snapshots do DB
    try:
        df_transactions = pd.read_sql("SELECT * FROM transactions", engine.connect())
    except Exception:
        df_transactions = pd.DataFrame()
    try:
        df_snapshots = pd.read_sql("SELECT * FROM net_worth_snapshots", engine.connect())
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

    # Plot 1: evolução do patrimônio
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

# ---------------- IPS page ----------------
def render_ips():
    st.header("Modelo de Investment Policy Statement (IPS)")
    st.markdown("Use este espaço para registrar a política de investimentos do investidor. Você pode editar e salvar a versão atual no banco de dados.")

    ips_row = load_latest_ips()
    default_title = ips_row["title"] if ips_row else "Investment Policy Statement - Cliente"
    default_content = ips_row["content"] if ips_row else """1. Introdução e Propósito

Identificação do Investidor:

Objetivo do Documento: Definir as diretrizes para a gestão dos ativos, estabelecer expectativas de retorno e níveis de risco aceitáveis, e criar uma base para a avaliação de desempenho.

2. Governança e Responsabilidades

Comitê/Gestor: Quem toma as decisões (ex: CFO, Comitê de Investimentos ou o próprio investidor).

Custodiantes e Consultores: Identificação das instituições financeiras e consultores externos.

Frequência de Revisão: (Ex: Anual ou em caso de mudança significativa nas circunstâncias).

3. Objetivos de Investimento

Produto financeiro -> negócios familiares

Objetivo de Retorno:

Primário: Preservação de capital ajustada pela inflação (IPCA + X%).

Secundário: Superar o benchmark de mercado (ex: CDI, IBOV ou S&P 500).

Tolerância ao Risco:

Definição qualitativa (Conservador, Moderado, Agressivo).

Métricas quantitativas (ex: Volatilidade máxima esperada de 15% ao ano ou perda máxima em 12 meses de 10%).

4. Restrições de Investimento (Constraints)

Liquidez: Necessidade de saques de curto prazo ou reserva de emergência (ex: 20% do fundo deve ter liquidez D+1).

Horizonte Temporal: Prazo esperado para o investimento (Curto, Médio ou Longo Prazo).

Considerações Fiscais: Estratégias para otimização de impostos.

Requisitos Legais/Regulatórios: Limites de exposição em ativos específicos ou jurisdições internacionais.

5. Alocação Estratégica de Ativos (Strategic Asset Allocation)

Classe de Ativo:
- Renda Fixa Pós-Fixada
- Renda Fixa Inflação
- Ações (Brasil)
- Ativos Internacionais
- Alternativos/Multimercados

6. Diretrizes de Rebalanceamento

Gatilho de Rebalanceamento: O rebalanceamento ocorrerá quando uma classe de ativos desviar mais de x% de sua alocação alvo ou em intervalos semestrais.

7. Monitoramento e Avaliação de Desempenho

Relatórios: Frequência de análise dos resultados (Mensal/Trimestral).

Critérios de Sucesso: Comparação da rentabilidade da carteira consolidada contra o benchmark ponderado (Policy Benchmark).
"""

    st.markdown("#### Visualizar / Editar IPS")
    title = st.text_input("Título do documento", value=default_title)
    content = st.text_area("Conteúdo do IPS (Markdown)", value=default_content, height=420)

    col_save, col_preview = st.columns([1,1])
    with col_save:
        if st.button("Salvar IPS"):
            save_ips(title, content)
            st.success("IPS salvo com sucesso.")
    with col_preview:
        if st.button("Pré-visualizar (Markdown)"):
            st.markdown("---")
            st.markdown(f"## {title}")
            st.markdown(content)

    st.markdown("---")
    st.info("Sugestão: revise o IPS anualmente ou sempre que houver mudança significativa nas circunstâncias do investidor.")

# ---------------- Controle de Investimentos page ----------------
def render_controle():
    st.header("Controle de Investimentos")

    df_holdings = list_holdings()
    df_assets = list_assets()
    df_liab = list_liabilities()

    st.subheader("Holdings (ativos financeiros)")
    if not df_holdings.empty:
        display = df_holdings.copy()
        for c in ["market_value","quantity","avg_cost"]:
            if c in display.columns:
                display[c] = display[c].apply(lambda v: format_brl(v) if pd.notna(v) else "")
        st.dataframe(display, use_container_width=True)

        st.markdown("**Editar / Remover holding**")
        options = df_holdings.apply(lambda r: f"{int(r.id)} | {r.get('asset_symbol','')} | {format_brl(r.get('market_value',0) or 0)}", axis=1).tolist()
        sel = st.selectbox("Selecionar holding", [""] + options)
        if sel:
            hid = int(sel.split("|")[0].strip())
            row = df_holdings[df_holdings["id"] == hid].iloc[0]
            with st.form("edit_holding"):
                new_symbol = st.text_input("Símbolo", value=row.get("asset_symbol",""))
                new_qty = st.number_input("Quantidade", value=float(row.get("quantity") or 0.0))
                new_avg = st.number_input("Preço médio", value=float(row.get("avg_cost") or 0.0))
                new_mv = st.number_input("Valor de mercado", value=float(row.get("market_value") or 0.0))
                btn_upd = st.form_submit_button("Atualizar holding")
                btn_del = st.form_submit_button("Remover holding")
                if btn_upd:
                    with engine.begin() as conn:
                        conn.execute(text("""
                            UPDATE holdings SET asset_symbol=:sym, quantity=:qty, avg_cost=:avg, market_value=:mv, updated_at=CURRENT_TIMESTAMP WHERE id=:id
                        """), {"sym": new_symbol, "qty": new_qty, "avg": new_avg, "mv": new_mv, "id": hid})
                    st.success("Holding atualizada.")
                    st.experimental_rerun()
                if btn_del:
                    with engine.begin() as conn:
                        conn.execute(text("DELETE FROM holdings WHERE id = :id"), {"id": hid})
                    st.success("Holding removida.")
                    st.experimental_rerun()
    else:
        st.info("Nenhuma holding registrada.")

    st.markdown("---")
    st.subheader("Ativos (bens manuais)")
    if not df_assets.empty:
        df_display = df_assets.copy()
        df_display["valor"] = df_display["valor"].apply(format_brl)
        st.dataframe(df_display, use_container_width=True)
    else:
        st.info("Nenhum ativo manual registrado.")

    st.markdown("#### Gerenciar ativos manuais")
    if not df_assets.empty:
        opts = df_assets.apply(lambda r: f"{int(r.id)} | {r.categoria} | {r.descricao} | {format_brl(r.valor)}", axis=1).tolist()
        sel_a = st.selectbox("Selecionar ativo", [""] + opts, key="ctrl_asset_sel")
        if sel_a:
            aid = int(sel_a.split("|")[0].strip())
            row = df_assets[df_assets["id"] == aid].iloc[0]
            with st.form("form_edit_asset_ctrl", clear_on_submit=False):
                categories = ["Imóveis","Bens Móveis","Empresas","Novos Negócios","Investimentos","Outros"]
                idx = categories.index(row.categoria) if row.categoria in categories else 0
                ca = st.selectbox("Categoria", categories, index=idx)
                cd = st.text_input("Descrição", value=row.descricao)
                cv = st.number_input("Valor (R$)", value=float(row.valor))
                b_up = st.form_submit_button("Atualizar ativo")
                b_del = st.form_submit_button("Remover ativo")
                if b_up:
                    update_asset(aid, ca, cd, cv)
                    st.success("Ativo atualizado.")
                    st.experimental_rerun()
                if b_del:
                    delete_asset(aid)
                    st.success("Ativo removido.")
                    st.experimental_rerun()
    else:
        st.info("Nenhum ativo manual para gerenciar.")

    st.markdown("---")
    st.subheader("Passivos (dívidas manuais)")
    if not df_liab.empty:
        df_display_l = df_liab.copy()
        df_display_l["valor"] = df_display_l["valor"].apply(format_brl)
        st.dataframe(df_display_l, use_container_width=True)
    else:
        st.info("Nenhum passivo registrado.")

    st.markdown("#### Gerenciar passivos manuais")
    if not df_liab.empty:
        opts_l = df_liab.apply(lambda r: f"{int(r.id)} | {r.categoria} | {r.descricao} | {format_brl(r.valor)}", axis=1).tolist()
        sel_l = st.selectbox("Selecionar passivo", [""] + opts_l, key="ctrl_liab_sel")
        if sel_l:
            lid = int(sel_l.split("|")[0].strip())
            row = df_liab[df_liab["id"] == lid].iloc[0]
            with st.form("form_edit_liab_ctrl", clear_on_submit=False):
                categories_l = ["Financiamento Imobiliário","Empréstimo Pessoal","Cartão de Crédito","Outros"]
                idx = categories_l.index(row.categoria) if row.categoria in categories_l else 0
                la = st.selectbox("Categoria (passivo)", categories_l, index=idx)
                ld = st.text_input("Descrição", value=row.descricao)
                lv = st.number_input("Valor (R$)", value=float(row.valor))
                b_up_l = st.form_submit_button("Atualizar passivo")
                b_del_l = st.form_submit_button("Remover passivo")
                if b_up_l:
                    update_liability(lid, la, ld, lv)
                    st.success("Passivo atualizado.")
                    st.experimental_rerun()
                if b_del_l:
                    delete_liability(lid)
                    st.success("Passivo removido.")
                    st.experimental_rerun()
    else:
        st.info("Nenhum passivo manual para gerenciar.")

    st.markdown("---")
    st.info("Use este painel para controlar holdings, bens e dívidas. Para produção, adicione autenticação e validação adicional.")

# ---------------- Render selected page ----------------
if page == "Visão Geral":
    render_visao_geral()
elif page == "IPS":
    render_ips()
elif page == "Controle de Investimentos":
    render_controle()