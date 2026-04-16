# investment.py
import streamlit as st
import pandas as pd
import numpy as np
from sqlalchemy import text
from db import engine
import time
import uuid

from utils import safe_rerun, format_brl as _format_brl

# ---------------- Ensure assets schema (adds 'tipo' column if missing) ----------------
def ensure_assets_schema():
    """
    Ensure the assets table has a 'tipo' column to store asset instrument type.
    This function is idempotent and safe to call on each import.
    """
    dialect = engine.dialect.name.lower()
    try:
        if dialect == "sqlite":
            with engine.connect() as conn:
                # check if column exists
                res = conn.execute(text("PRAGMA table_info(assets)")).fetchall()
                cols = [r[1] for r in res]
                if "tipo" not in cols:
                    conn.execute(text("ALTER TABLE assets ADD COLUMN tipo TEXT"))
        else:
            # For other DBs, try to add column if not exists (generic SQL)
            with engine.begin() as conn:
                # attempt to add column; many DBs support IF NOT EXISTS, but fallback to try/except
                try:
                    conn.execute(text("ALTER TABLE assets ADD COLUMN tipo TEXT"))
                except Exception:
                    # ignore if already exists or not supported
                    pass
    except Exception:
        # non-fatal: UI will still work, but tipo may not be persisted
        pass

ensure_assets_schema()

# ---------------- Read helpers ----------------
def read_table_safe(name: str) -> pd.DataFrame:
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
    # ensure schema again just before insert (safe)
    ensure_assets_schema()
    with engine.begin() as conn:
        # include tipo column if present
        try:
            conn.execute(text("""
                INSERT INTO assets (categoria, tipo, descricao, valor, source, import_batch_id)
                VALUES (:categoria, :tipo, :descricao, :valor, :source, :ib)
            """), {"categoria": categoria, "tipo": tipo, "descricao": descricao, "valor": valor, "source": source, "ib": import_batch_id})
        except Exception:
            # fallback to insert without tipo if DB doesn't support it
            conn.execute(text("""
                INSERT INTO assets (categoria, descricao, valor, source, import_batch_id)
                VALUES (:categoria, :descricao, :valor, :source, :ib)
            """), {"categoria": categoria, "descricao": descricao, "valor": valor, "source": source, "ib": import_batch_id})

def update_asset(asset_id: int, categoria: str, tipo: str, descricao: str, valor: float):
    ensure_assets_schema()
    with engine.begin() as conn:
        # try update including tipo
        try:
            conn.execute(text("""
                UPDATE assets SET categoria=:categoria, tipo=:tipo, descricao=:descricao, valor=:valor WHERE id=:id
            """), {"categoria": categoria, "tipo": tipo, "descricao": descricao, "valor": valor, "id": asset_id})
        except Exception:
            # fallback to update without tipo
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
    return df.groupby("categoria", as_index=False)["valor"].sum()

def aggregate_assets_by_type() -> pd.DataFrame:
    df = list_assets()
    if df.empty:
        return pd.DataFrame(columns=["tipo", "valor"])
    if "tipo" not in df.columns:
        df["tipo"] = "N/A"
    return df.groupby("tipo", as_index=False)["valor"].sum()

def aggregate_liabilities_by_category() -> pd.DataFrame:
    df = list_liabilities()
    if df.empty:
        return pd.DataFrame(columns=["categoria", "valor"])
    return df.groupby("categoria", as_index=False)["valor"].sum()

# ---------------- Render function moved to this module ----------------
def render_controle_ui():
    st.header("Controle de Investimentos")

    # layout: left = form + quick stats, right = charts + list
    col_left, col_right = st.columns([1,2])

    asset_types = ["Fundo de Investimento", "CDB", "LCI", "LCA", "Selic", "ETC", "COE", "Ações", "Outros"]

    with col_left:
        st.subheader("Adicionar bem / ativo")
        with st.form("form_add_asset_local", clear_on_submit=True):
            a_categoria = st.selectbox("Categoria", ["Imóveis","Bens Móveis","Empresas","Novos Negócios","Investimentos","Outros"], key="add_cat")
            a_tipo = st.selectbox("Tipo do instrumento", asset_types, index=0, key="add_tipo")
            a_descricao = st.text_input("Descrição", key="add_desc")
            a_valor = st.number_input("Valor (R$)", min_value=0.0, step=100.0, format="%.2f", key="add_val")
            submitted_asset = st.form_submit_button("Adicionar ativo")
            if submitted_asset:
                import_batch_id = f"manual_asset_{int(time.time())}_{uuid.uuid4().hex[:6]}"
                try:
                    add_asset(a_categoria, a_tipo, a_descricao, a_valor, source="manual_form", import_batch_id=import_batch_id)
                    st.success("Ativo adicionado com sucesso.")
                except Exception as e:
                    st.error(f"Falha ao adicionar ativo: {e}")
                safe_rerun()

        st.markdown("---")
        st.subheader("Adicionar passivo / dívida")
        with st.form("form_add_liability_local", clear_on_submit=True):
            l_categoria = st.selectbox("Categoria (passivo)", ["Financiamento Imobiliário","Empréstimo Pessoal","Cartão de Crédito","Outros"], key="add_liab_cat")
            l_descricao = st.text_input("Descrição do passivo", key="add_liab_desc")
            l_valor = st.number_input("Valor (R$)", min_value=0.0, step=100.0, format="%.2f", key="add_liab_val")
            submitted_liab = st.form_submit_button("Adicionar passivo")
            if submitted_liab:
                import_batch_id = f"manual_liab_{int(time.time())}_{uuid.uuid4().hex[:6]}"
                try:
                    add_liability(l_categoria, l_descricao, l_valor, source="manual_form", import_batch_id=import_batch_id)
                    st.success("Passivo adicionado com sucesso.")
                except Exception as e:
                    st.error(f"Falha ao adicionar passivo: {e}")
                safe_rerun()

        st.markdown("---")
        # Quick KPIs for assets by type
        st.subheader("Resumo rápido")
        df_assets = list_assets()
        df_liab = list_liabilities()
        total_assets = float(df_assets["valor"].sum()) if not df_assets.empty else 0.0
        total_liab = float(df_liab["valor"].sum()) if not df_liab.empty else 0.0
        net_worth = total_assets - total_liab
        st.metric("Ativos Totais", _format_brl(total_assets))
        st.metric("Passivos Totais", _format_brl(total_liab))
        st.metric("Patrimônio Líquido", _format_brl(net_worth))

    with col_right:
        st.subheader("Visão dos Ativos")

        # Charts: distribution by type and by category
        df_assets = list_assets()
        if df_assets.empty:
            st.info("Nenhum ativo registrado ainda.")
        else:
            # ensure tipo column exists in df
            if "tipo" not in df_assets.columns:
                df_assets["tipo"] = "N/A"

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
                st.plotly_chart(fig_type, use_container_width=True)
            except Exception:
                # fallback simple table
                st.table(agg_type.assign(valor=lambda d: d["valor"].map(_format_brl)))

            st.markdown("**Distribuição por Categoria**")
            try:
                fig_cat = px.pie(agg_cat, names="categoria", values="valor", hole=0.35)
                fig_cat.update_traces(textposition='inside', textinfo='percent+label')
                fig_cat.update_layout(height=320, margin=dict(t=30, b=30))
                st.plotly_chart(fig_cat, use_container_width=True)
            except Exception:
                st.table(agg_cat.assign(valor=lambda d: d["valor"].map(_format_brl)))

            st.markdown("---")
            st.subheader("Lista detalhada de ativos")
            # Provide searchable/filterable table and expanders per asset
            # Add a small filter row
            cols_filter = st.columns([2,2,1])
            with cols_filter[0]:
                q_text = st.text_input("Buscar descrição / símbolo", value="", key="filter_text")
            with cols_filter[1]:
                q_tipo = st.selectbox("Filtrar por tipo", options=["Todos"] + sorted(df_assets["tipo"].dropna().unique().tolist()), key="filter_tipo")
            with cols_filter[2]:
                q_min = st.number_input("Valor mínimo (R$)", min_value=0.0, value=0.0, format="%.2f", key="filter_min")

            df_filtered = df_assets.copy()
            if q_text:
                df_filtered = df_filtered[df_filtered["descricao"].str.contains(q_text, case=False, na=False) | df_filtered.get("categoria", "").str.contains(q_text, case=False, na=False)]
            if q_tipo and q_tipo != "Todos":
                df_filtered = df_filtered[df_filtered["tipo"] == q_tipo]
            if q_min and q_min > 0:
                df_filtered = df_filtered[df_filtered["valor"].astype(float) >= float(q_min)]

            # show table
            display = df_filtered.copy()
            # format valor for display
            display["valor_fmt"] = display["valor"].apply(lambda v: _format_brl(v))
            display_cols = ["id", "categoria", "tipo", "descricao", "valor_fmt"]
            st.dataframe(display[display_cols].rename(columns={"valor_fmt":"valor"}), use_container_width=True)

            # Expanders for each asset to edit/delete
            st.markdown("#### Editar / Remover ativos")
            for _, row in df_filtered.sort_values("valor", ascending=False).iterrows():
                aid = int(row["id"])
                title = f"{row.get('categoria','')} — {row.get('tipo','')} — {row.get('descricao','')} — {_format_brl(row.get('valor',0))}"
                with st.expander(title):
                    # show details
                    st.write("Categoria:", row.get("categoria"))
                    st.write("Tipo:", row.get("tipo"))
                    st.write("Descrição:", row.get("descricao"))
                    st.write("Valor:", _format_brl(row.get("valor",0)))
                    # edit form
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

    st.markdown("---")
    st.info("Use este painel para controlar holdings, bens e dívidas. Para produção, adicione autenticação, validação e backups.")