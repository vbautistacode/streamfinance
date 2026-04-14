# investment.py
import streamlit as st
import pandas as pd
from sqlalchemy import text
from db import engine

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

# ---------------- CRUD for assets ----------------
def add_asset(categoria: str, descricao: str, valor: float, source: str = "manual_form", import_batch_id: str = None):
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO assets (categoria, descricao, valor, source, import_batch_id)
            VALUES (:categoria, :descricao, :valor, :source, :ib)
        """), {"categoria": categoria, "descricao": descricao, "valor": valor, "source": source, "ib": import_batch_id})

def update_asset(asset_id: int, categoria: str, descricao: str, valor: float):
    with engine.begin() as conn:
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

def aggregate_liabilities_by_category() -> pd.DataFrame:
    df = list_liabilities()
    if df.empty:
        return pd.DataFrame(columns=["categoria", "valor"])
    return df.groupby("categoria", as_index=False)["valor"].sum()

# ---------------- Small formatter used in this module ----------------
def _format_brl(value):
    try:
        v = float(value)
    except Exception:
        return value
    s = f"{v:,.2f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"

# ---------------- Render function moved to this module ----------------
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
                display[c] = display[c].apply(lambda v: _format_brl(v) if pd.notna(v) else "")
        st.dataframe(display, use_container_width=True)

        st.markdown("**Editar / Remover holding**")
        options = df_holdings.apply(lambda r: f"{int(r.id)} | {r.get('asset_symbol','')} | {_format_brl(r.get('market_value',0) or 0)}", axis=1).tolist()
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
        df_display["valor"] = df_display["valor"].apply(_format_brl)
        st.dataframe(df_display, use_container_width=True)
    else:
        st.info("Nenhum ativo manual registrado.")

    st.markdown("#### Gerenciar ativos manuais")
    if not df_assets.empty:
        opts = df_assets.apply(lambda r: f"{int(r.id)} | {r.categoria} | {r.descricao} | {_format_brl(r.valor)}", axis=1).tolist()
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
        df_display_l["valor"] = df_display_l["valor"].apply(_format_brl)
        st.dataframe(df_display_l, use_container_width=True)
    else:
        st.info("Nenhum passivo registrado.")

    st.markdown("#### Gerenciar passivos manuais")
    if not df_liab.empty:
        opts_l = df_liab.apply(lambda r: f"{int(r.id)} | {r.categoria} | {r.descricao} | {_format_brl(r.valor)}", axis=1).tolist()
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