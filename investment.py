# investment.py
import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import text
from db import engine
import time
import uuid
from datetime import date
import io

# utilitários compartilhados
from utils import safe_rerun, format_brl
_format_brl = format_brl

# ---------------- Ensure assets schema (adds 'tipo' column if missing) ----------------
def ensure_assets_schema():
    """
    Ensure the assets table has a 'tipo' column to store asset instrument type.
    This function is idempotent and safe to call on each import.
    """
    try:
        dialect = engine.dialect.name.lower()
    except Exception:
        # engine not available; nothing to do
        return

    try:
        if dialect == "sqlite":
            with engine.connect() as conn:
                try:
                    res = conn.execute(text("PRAGMA table_info(assets)")).fetchall()
                    cols = [r[1] for r in res]
                    if "tipo" not in cols:
                        conn.execute(text("ALTER TABLE assets ADD COLUMN tipo TEXT"))
                except Exception:
                    # table may not exist yet; ignore
                    pass
        else:
            with engine.begin() as conn:
                try:
                    conn.execute(text("ALTER TABLE assets ADD COLUMN tipo TEXT"))
                except Exception:
                    # ignore if already exists or not supported
                    pass
    except Exception:
        # non-fatal
        pass

ensure_assets_schema()

# ---------------- Read helpers ----------------
def read_table_safe(name: str) -> pd.DataFrame:
    """
    Read a table safely. Returns empty DataFrame on any error.
    Note: name is interpolated directly; ensure it's a trusted value.
    """
    try:
        with engine.connect() as conn:
            return pd.read_sql(f"SELECT * FROM {name}", conn)
    except Exception:
        return pd.DataFrame()

def list_holdings() -> pd.DataFrame:
    return read_table_safe("holdings")

def list_assets() -> pd.DataFrame:
    return read_table_safe("assets")

def list_liabilities() -> pd.DataFrame:
    return read_table_safe("liabilities")

# ---------------- CRUD for assets (now with tipo) ----------------
def add_asset(categoria: str, tipo: str, descricao: str, valor: float, source: str = "manual_form", import_batch_id: str = None):
    ensure_assets_schema()
    with engine.begin() as conn:
        try:
            conn.execute(text("""
                INSERT INTO assets (categoria, tipo, descricao, valor, source, import_batch_id)
                VALUES (:categoria, :tipo, :descricao, :valor, :source, :ib)
            """), {"categoria": categoria, "tipo": tipo, "descricao": descricao, "valor": valor, "source": source, "ib": import_batch_id})
        except Exception:
            conn.execute(text("""
                INSERT INTO assets (categoria, descricao, valor, source, import_batch_id)
                VALUES (:categoria, :descricao, :valor, :source, :ib)
            """), {"categoria": categoria, "descricao": descricao, "valor": valor, "source": source, "ib": import_batch_id})

def update_asset(asset_id: int, categoria: str, tipo: str, descricao: str, valor: float):
    ensure_assets_schema()
    with engine.begin() as conn:
        try:
            conn.execute(text("""
                UPDATE assets SET categoria=:categoria, tipo=:tipo, descricao=:descricao, valor=:valor WHERE id=:id
            """), {"categoria": categoria, "tipo": tipo, "descricao": descricao, "valor": valor, "id": asset_id})
        except Exception:
            conn.execute(text("""
                UPDATE assets SET categoria=:categoria, descricao=:descricao, valor=:valor WHERE id=:id
            """), {"categoria": categoria, "descricao": descricao, "valor": valor, "id": asset_id})

def delete_asset(asset_id: int):
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM assets WHERE id = :id"), {"id": asset_id})

# ---------------- CRUD for liabilities ----------------
def add_liability(categoria: str, descricao: str, valor: float, source: str = "manual_form", import_batch_id: str = None):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO liabilities (categoria, descricao, valor, source, import_batch_id)
            VALUES (:categoria, :descricao, :valor, :source, :ib)
        """), {"categoria": categoria, "descricao": descricao, "valor": valor, "source": source, "ib": import_batch_id})

def update_liability(liab_id: int, categoria: str, descricao: str, valor: float):
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE liabilities SET categoria=:categoria, descricao=:descricao, valor=:valor WHERE id=:id
        """), {"categoria": categoria, "descricao": descricao, "valor": valor, "id": liab_id})

def delete_liability(liab_id: int):
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM liabilities WHERE id = :id"), {"id": liab_id})

# ---------------- Aggregation helpers ----------------
def aggregate_assets_by_category() -> pd.DataFrame:
    df = list_assets()
    if df.empty:
        return pd.DataFrame(columns=["categoria", "valor"])
    if "categoria" not in df.columns or "valor" not in df.columns:
        return pd.DataFrame(columns=["categoria", "valor"])
    return df.groupby("categoria", as_index=False)["valor"].sum()

def aggregate_assets_by_type() -> pd.DataFrame:
    df = list_assets()
    if df.empty:
        return pd.DataFrame(columns=["tipo", "valor"])
    if "tipo" not in df.columns:
        df["tipo"] = "N/A"
    if "valor" not in df.columns:
        df["valor"] = 0.0
    return df.groupby("tipo", as_index=False)["valor"].sum()

def aggregate_liabilities_by_category() -> pd.DataFrame:
    df = list_liabilities()
    if df.empty:
        return pd.DataFrame(columns=["categoria", "valor"])
    if "categoria" not in df.columns or "valor" not in df.columns:
        return pd.DataFrame(columns=["categoria", "valor"])
    return df.groupby("categoria", as_index=False)["valor"].sum()

# ---------------- Render function moved to this module ----------------
def render_controle_ui():
    st.header("Controle de Investimentos")

    # layout: left = form + quick stats, right = charts + list
    col_left, col_right = st.columns([1,2])

    asset_types = ["Fundo de Investimento", "CDB", "LCI", "LCA", "Selic", "ETC", "COE", "Ações", "Outros"]

    # Left column: add asset form
    with col_left:
        st.subheader("Adicionar bem / ativo")
        with st.form("form_add_asset_local", clear_on_submit=True):
            a_categoria = st.selectbox("Categoria", ["Imóveis","Bens Móveis","Empresas","Novos Negócios","Investimentos","Outros"], key="add_cat")
            a_tipo = st.selectbox("Tipo do instrumento", asset_types, index=0, key="add_tipo")
            a_descricao = st.text_input("Descrição", key="add_desc")
            a_valor = st.number_input("Valor (R$)", min_value=0.0, step=100.0, format="%.2f", key="add_val")
            submitted_asset = st.form_submit_button("Adicionar ativo")
            if submitted_asset:
                if not a_descricao:
                    st.error("Descrição é obrigatória.")
                elif a_valor <= 0:
                    st.error("Valor deve ser maior que zero.")
                else:
                    import_batch_id = f"manual_asset_{int(time.time())}_{uuid.uuid4().hex[:6]}"
                    try:
                        add_asset(a_categoria, a_tipo, a_descricao, a_valor, source="manual_form", import_batch_id=import_batch_id)
                        st.success("Ativo adicionado com sucesso.")
                    except Exception as e:
                        st.error(f"Falha ao adicionar ativo: {e}")
                    safe_rerun()

    # Right column: add liability form and optional portfolio import
    with col_right:
        st.subheader("Adicionar passivo / dívida")
        with st.form("form_add_liability_local", clear_on_submit=True):
            l_categoria = st.selectbox("Categoria (passivo)", ["Financiamento Imobiliário","Empréstimo Pessoal","Cartão de Crédito","Outros"], key="add_liab_cat")
            l_descricao = st.text_input("Descrição do passivo", key="add_liab_desc")
            l_valor = st.number_input("Valor (R$)", min_value=0.0, step=100.0, format="%.2f", key="add_liab_val")
            submitted_liab = st.form_submit_button("Adicionar passivo")
            if submitted_liab:
                if not l_descricao:
                    st.error("Descrição do passivo é obrigatória.")
                elif l_valor <= 0:
                    st.error("Valor do passivo deve ser maior que zero.")
                else:
                    import_batch_id = f"manual_liab_{int(time.time())}_{uuid.uuid4().hex[:6]}"
                    try:
                        add_liability(l_categoria, l_descricao, l_valor, source="manual_form", import_batch_id=import_batch_id)
                        st.success("Passivo adicionado com sucesso.")
                    except Exception as e:
                        st.error(f"Falha ao adicionar passivo: {e}")
                    safe_rerun()

        # Optional: import portfolio snapshot form (submit button included)
        st.markdown("---")
        st.subheader("Importar snapshot de carteira")
        with st.form("form_import_portfolio", clear_on_submit=True):
            owner = st.selectbox("Owner", ["Paula Casale", "Adolfo Pacheco"], index=0, key="import_owner")
            snapshot_date = st.date_input("Data do snapshot", value=date.today(), key="import_date")
            uploaded = st.file_uploader("Arquivo CSV/XLSX", type=["csv","xlsx"], key="import_file")
            import_submit = st.form_submit_button("Importar snapshot")
            if import_submit:
                if uploaded is None:
                    st.error("Envie um arquivo CSV ou XLSX.")
                else:
                    import_batch_id = f"portfolio_{owner}_{int(time.time())}_{uuid.uuid4().hex[:6]}"
                    try:
                        res = import_portfolio_file(uploaded, owner=owner, snapshot_date=snapshot_date, import_batch_id=import_batch_id)
                        if res.get("errors", 1) == 0:
                            st.success(f"{res.get('inserted',0)} linhas importadas.")
                            synced = sync_assets_from_snapshot(owner, snapshot_date, import_batch_id=import_batch_id)
                            if synced:
                                st.info(f"{synced} assets sincronizados a partir do snapshot.")
                        else:
                            st.error(f"Falha na importação: {res.get('error')}")
                    except Exception as e:
                        st.error(f"Erro ao importar snapshot: {e}")
                    safe_rerun()

    st.markdown("---")
    # Quick KPIs for assets by type
    st.subheader("Resumo rápido")
    df_assets = list_assets()
    df_liab = list_liabilities()

    # garantir colunas
    if df_assets is None or df_assets.empty or "valor" not in df_assets.columns:
        total_assets = 0.0
    else:
        total_assets = float(pd.to_numeric(df_assets["valor"], errors="coerce").fillna(0.0).sum())

    if df_liab is None or df_liab.empty or "valor" not in df_liab.columns:
        total_liab = 0.0
    else:
        total_liab = float(pd.to_numeric(df_liab["valor"], errors="coerce").fillna(0.0).sum())

    net_worth = total_assets - total_liab
    st.metric("Ativos Totais", format_brl(total_assets))
    st.metric("Passivos Totais", format_brl(total_liab))
    st.metric("Patrimônio Líquido", format_brl(net_worth))

    st.markdown("---")
    st.subheader("Visão dos Ativos")

    # Charts: distribution by type and by category
    df_assets = list_assets()
    if df_assets is None or df_assets.empty or "valor" not in df_assets.columns:
        st.info("Nenhum ativo registrado ainda.")
        return

    # ensure tipo column exists in df
    if "tipo" not in df_assets.columns:
        df_assets["tipo"] = "N/A"
    if "categoria" not in df_assets.columns:
        df_assets["categoria"] = "Outros"
    if "descricao" not in df_assets.columns:
        df_assets["descricao"] = ""

    # Aggregate by type
    agg_type = df_assets.groupby("tipo", as_index=False)["valor"].sum().sort_values("valor", ascending=False)
    agg_cat = df_assets.groupby("categoria", as_index=False)["valor"].sum().sort_values("valor", ascending=False)

    # Bar chart by type
    st.markdown("**Distribuição por Tipo**")
    try:
        import plotly.express as px
        fig_type = px.bar(agg_type, x="tipo", y="valor", text="valor", labels={"valor":"Valor (R$)","tipo":"Tipo"})
        fig_type.update_traces(texttemplate="%{text:.2f}", textposition="outside")
        fig_type.update_layout(yaxis_tickformat=",.0f", height=320, margin=dict(t=30, b=30))
        st.plotly_chart(fig_type, width='stretch')
    except Exception:
        st.table(agg_type.assign(valor=lambda d: d["valor"].map(_format_brl)))

    st.markdown("**Distribuição por Categoria**")
    try:
        fig_cat = px.pie(agg_cat, names="categoria", values="valor", hole=0.35)
        fig_cat.update_traces(textposition='inside', textinfo='percent+label')
        fig_cat.update_layout(height=320, margin=dict(t=30, b=30))
        st.plotly_chart(fig_cat, width='stretch')
    except Exception:
        st.table(agg_cat.assign(valor=lambda d: d["valor"].map(_format_brl)))

    st.markdown("---")
    st.subheader("Lista detalhada de ativos")
    # Provide searchable/filterable table and expanders per asset
    cols_filter = st.columns([2,2,1])
    with cols_filter[0]:
        q_text = st.text_input("Buscar descrição / símbolo", value="", key="filter_text")
    with cols_filter[1]:
        tipos_list = sorted(df_assets["tipo"].dropna().unique().tolist()) if "tipo" in df_assets.columns else []
        q_tipo = st.selectbox("Filtrar por tipo", options=["Todos"] + tipos_list, key="filter_tipo")
    with cols_filter[2]:
        q_min = st.number_input("Valor mínimo (R$)", min_value=0.0, value=0.0, format="%.2f", key="filter_min")

    df_filtered = df_assets.copy()
    if q_text:
        df_filtered = df_filtered[df_filtered["descricao"].str.contains(q_text, case=False, na=False) | df_filtered.get("categoria", "").str.contains(q_text, case=False, na=False)]
    if q_tipo and q_tipo != "Todos":
        df_filtered = df_filtered[df_filtered["tipo"] == q_tipo]
    if q_min and q_min > 0:
        df_filtered = df_filtered[df_filtered["valor"].astype(float) >= float(q_min)]

    # show table safely
    display = df_filtered.copy()
    display["valor_fmt"] = display["valor"].apply(lambda v: format_brl(v))
    display_cols = []
    for c in ["id", "categoria", "tipo", "descricao", "valor_fmt"]:
        if c in display.columns:
            display_cols.append(c)
    if "valor_fmt" in display_cols:
        display = display.rename(columns={"valor_fmt": "valor"})
        display_cols = [c if c != "valor_fmt" else "valor" for c in display_cols]
    st.dataframe(display[display_cols], width='stretch')

    # Expanders for each asset to edit/delete
    st.markdown("#### Editar / Remover ativos")
    if "id" not in df_filtered.columns:
        st.info("Ativos sem identificador (id) não podem ser editados via UI.")
    else:
        for _, row in df_filtered.sort_values("valor", ascending=False).iterrows():
            aid = int(row["id"])
            title = f"{row.get('categoria','')} — {row.get('tipo','')} — {row.get('descricao','')} — {format_brl(row.get('valor',0))}"
            with st.expander(title):
                st.write("Categoria:", row.get("categoria"))
                st.write("Tipo:", row.get("tipo"))
                st.write("Descrição:", row.get("descricao"))
                st.write("Valor:", format_brl(row.get("valor",0)))
                with st.form(f"edit_asset_{aid}", clear_on_submit=False):
                    categories = ["Imóveis","Bens Móveis","Empresas","Novos Negócios","Investimentos","Outros"]
                    types = asset_types
                    idx_cat = categories.index(row.get("categoria")) if row.get("categoria") in categories else 0
                    idx_tipo = types.index(row.get("tipo")) if row.get("tipo") in types else (len(types)-1)
                    new_cat = st.selectbox("Categoria", categories, index=idx_cat, key=f"cat_{aid}")
                    new_tipo = st.selectbox("Tipo", types, index=idx_tipo, key=f"tipo_{aid}")
                    new_desc = st.text_input("Descrição", value=row.get("descricao",""), key=f"desc_{aid}")
                    new_val = st.number_input("Valor (R$)", value=float(row.get("valor") or 0.0), format="%.2f", key=f"val_{aid}")
                    b_up = st.form_submit_button("Atualizar")
                    b_del = st.form_submit_button("Remover")
                    if b_up:
                        try:
                            update_asset(aid, new_cat, new_tipo, new_desc, new_val)
                            st.success("Ativo atualizado.")
                        except Exception as e:
                            st.error(f"Falha ao atualizar ativo: {e}")
                        safe_rerun()
                    if b_del:
                        try:
                            delete_asset(aid)
                            st.success("Ativo removido.")
                        except Exception as e:
                            st.error(f"Falha ao remover ativo: {e}")
                        safe_rerun()

# ------------------ Composição Patrimonial ----------------
    st.markdown("### Composição Patrimonial | Bens e Investimentos")

    df_holdings = list_holdings()
    df_assets = list_assets()
    df_liab = list_liabilities()

    # Build assets from holdings + manual assets
    if (df_holdings is None or df_holdings.empty) and (df_assets is None or df_assets.empty) and (df_liab is None or df_liab.empty):
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
        if df_holdings is not None and not df_holdings.empty:
            df_h = df_holdings.copy()
            if "market_value" in df_h.columns:
                df_h["valor"] = pd.to_numeric(df_h["market_value"], errors="coerce").fillna(0.0)
            elif "quantity" in df_h.columns and "avg_cost" in df_h.columns:
                df_h["valor"] = pd.to_numeric(df_h["quantity"], errors="coerce").fillna(0.0) * pd.to_numeric(df_h["avg_cost"], errors="coerce").fillna(0.0)
            else:
                df_h["valor"] = 0.0
            df_h["categoria"] = df_h.get("category", "Investimentos")
            df_h["descricao"] = df_h.get("asset_symbol", "")
            parts.append(df_h[["categoria","descricao","valor"]])
        if df_assets is not None and not df_assets.empty:
            cols = [c for c in ["categoria","descricao","valor"] if c in df_assets.columns]
            if cols:
                parts.append(df_assets[cols])
        df_assets_display = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=["categoria","descricao","valor"])
        df_liab_display = df_liab[["categoria","descricao","valor"]] if (df_liab is not None and not df_liab.empty and all(c in df_liab.columns for c in ["categoria","descricao","valor"])) else pd.DataFrame(columns=["categoria","descricao","valor"])

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
        import plotly.express as px
        fig_assets_pie = px.pie(agg_assets, names="categoria", values="valor", hole=0.35)
        fig_assets_pie.update_traces(textinfo="percent+label")
        st.plotly_chart(fig_assets_pie, width='stretch')
    else:
        st.info("Nenhum ativo registrado ainda.")

    st.markdown("#### Detalhe de ativos")
    if not df_assets_display.empty:
        df_display = df_assets_display.copy()
        if "valor" in df_display.columns:
            df_display["valor"] = df_display["valor"].apply(format_brl)
        st.dataframe(df_display, width='stretch')
    else:
        st.write("Nenhum ativo detalhado disponível.")

    st.markdown("#### Distribuição de passivos por categoria")
    if not agg_liab.empty:
        import plotly.express as px
        fig_liab = px.bar(agg_liab, x="categoria", y="valor", labels={"valor":"Valor (R$)","categoria":"Categoria"}, text="valor")
        fig_liab.update_traces(texttemplate="R$ %{y:,.0f}")
        fig_liab.update_layout(yaxis_tickformat=",.0f")
        st.plotly_chart(fig_liab, width='stretch')
    else:
        st.info("Nenhum passivo registrado ainda.")

    st.markdown("#### Detalhe de passivos")
    if not df_liab_display.empty:
        df_display_l = df_liab_display.copy()
        if "valor" in df_display_l.columns:
            df_display_l["valor"] = df_display_l["valor"].apply(format_brl)
        st.dataframe(df_display_l, width='stretch')
    else:
        st.write("Nenhum passivo detalhado disponível.")

# --- Função helper para semear dados mockados (para desenvolvimento / testes) ---
def seed_mock_portfolio_samples(commit_to_assets: bool = False):
    """
    Insere dois snapshots mockados no banco usando save_portfolio_snapshot.
    Use apenas em ambiente de desenvolvimento. Retorna dict com contagens.
    """
    from datetime import date
    results = {"inserted_1": 0, "inserted_2": 0, "synced_1": 0, "synced_2": 0}

    # Snapshot 1 (baseado na primeira imagem)
    owner1 = "Paula Casale"
    snapshot_date1 = date.today()
    data1 = [
        {"descricao": "VINLAND SELECAO INFR", "categoria": "Fundo de Investimento", "saldo": 171736.83, "alocacao": 20.71},
        {"descricao": "ITAU KINEA ANDES RF", "categoria": "Fundo de Investimento", "saldo": 135807.12, "alocacao": 16.38},
        {"descricao": "ACTIVE FIX ALL RF CP", "categoria": "Fundo de Investimento", "saldo": 104130.07, "alocacao": 12.56},
        {"descricao": "ABSOLUTE HIDRA INFRA", "categoria": "Fundo de Investimento", "saldo": 56295.29, "alocacao": 6.79},
        {"descricao": "DIF CP FICFI", "categoria": "Fundo de Investimento", "saldo": 36640.70, "alocacao": 4.42},
        {"descricao": "CDBB24A27D3 CDB BANCO C6", "categoria": "CDB, Renda Fixa e Inv. Estruturados", "saldo": 154816.22, "alocacao": 18.67},
        {"descricao": "COE SP 500 GANHO GARANTI ITAU", "categoria": "CDB, Renda Fixa e Inv. Estruturados", "saldo": 57327.24, "alocacao": 6.91},
        {"descricao": "CDB6247W6TG CDB BANCO C6", "categoria": "CDB, Renda Fixa e Inv. Estruturados", "saldo": 55920.90, "alocacao": 6.74},
        {"descricao": "IT7824LEQOI COE ITAU UNIBANCO", "categoria": "CDB, Renda Fixa e Inv. Estruturados", "saldo": 32926.81, "alocacao": 3.97},
        {"descricao": "CDBB24A26V3 CDB NOVO BANCO", "categoria": "CDB, Renda Fixa e Inv. Estruturados", "saldo": 23712.08, "alocacao": 2.86},
    ]
    df1 = pd.DataFrame(data1)
    try:
        results["inserted_1"] = save_portfolio_snapshot(df1, owner1, snapshot_date1, import_batch_id=f"mock_{int(time.time())}_1")
    except Exception as e:
        results["inserted_1"] = 0

    # Snapshot 2 (baseado na segunda imagem / rentab. simulada)
    owner2 = "Adolfo Pacheco"
    snapshot_date2 = date.today()
    data2 = [
        {"descricao": "1a. Prev Itau Absol Atenas Rf Cp Vgbl", "categoria": "Previdência", "saldo": 580678.87, "alocacao": 80.22},
        {"descricao": "CDB62489SRY CDB VIA CERTA FINANCIADORA", "categoria": "CDB, Renda Fixa e Inv. Estruturados", "saldo": 75483.98, "alocacao": 10.43},
        {"descricao": "CDB62489TJ2 CDB BANCO C6 CONSIGNADO", "categoria": "CDB, Renda Fixa e Inv. Estruturados", "saldo": 67289.65, "alocacao": 9.30},
        {"descricao": "PRIVILEGE RF REF DI", "categoria": "Fundo de Investimento", "saldo": 328.14, "alocacao": 0.05},
        {"descricao": "SUPER DI", "categoria": "Fundo de Investimento", "saldo": 62.68, "alocacao": 0.01},
    ]
    df2 = pd.DataFrame(data2)
    try:
        results["inserted_2"] = save_portfolio_snapshot(df2, owner2, snapshot_date2, import_batch_id=f"mock_{int(time.time())}_2")
    except Exception as e:
        results["inserted_2"] = 0

    # opcional: sincronizar assets top-level a partir dos snapshots
    if commit_to_assets:
        try:
            results["synced_1"] = sync_assets_from_snapshot(owner1, snapshot_date1, import_batch_id=f"mock_sync_{int(time.time())}_1")
        except Exception:
            results["synced_1"] = 0
        try:
            results["synced_2"] = sync_assets_from_snapshot(owner2, snapshot_date2, import_batch_id=f"mock_sync_{int(time.time())}_2")
        except Exception:
            results["synced_2"] = 0

    return results

# Exemplo de uso (descomente para rodar manualmente em dev)
print(seed_mock_portfolio_samples(commit_to_assets=False))