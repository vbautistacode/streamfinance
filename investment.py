# investment.py
import streamlit as st
import pandas as pd
from sqlalchemy import text
from db import engine
import time
import uuid

# ---------------- Utility: safe rerun ----------------
def safe_rerun():
    """
    Tenta reiniciar o script com st.experimental_rerun().
    Se não estiver disponível ou falhar, mostra instrução para o usuário.
    """
    try:
        if hasattr(st, "experimental_rerun"):
            st.experimental_rerun()
            return
    except Exception:
        # se st.experimental_rerun existir mas lançar erro, capturamos e seguimos para fallback
        pass

    # fallback: instruir o usuário a atualizar manualmente
    st.info("Alteração salva. Atualize a página manualmente para ver as mudanças.")

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
def render_controle_ui():
    st.header("Controle de Investimentos")

    # Left column: quick add forms (moved from sidebar)
    col_left, col_right = st.columns([1,2])

    with col_left:
        st.subheader("Adicionar bem / ativo")
        with st.form("form_add_asset_local", clear_on_submit=True):
            a_categoria = st.selectbox("Categoria", ["Imóveis","Bens Móveis","Empresas","Novos Negócios","Investimentos","Outros"])
            a_descricao = st.text_input("Descrição")
            a_valor = st.number_input("Valor (R$)", min_value=0.0, step=100.0, format="%.2f")
            submitted_asset = st.form_submit_button("Adicionar ativo")
            if submitted_asset:
                import_batch_id = f"manual_asset_{int(time.time())}_{uuid.uuid4().hex[:6]}"
                try:
                    add_asset(a_categoria, a_descricao, a_valor, source="manual_form", import_batch_id=import_batch_id)
                    st.success("Ativo adicionado com sucesso.")
                except Exception as e:
                    st.error(f"Falha ao adicionar ativo: {e}")
                # use safe rerun
                safe_rerun()

        st.markdown("---")
        st.subheader("Adicionar passivo / dívida")
        with st.form("form_add_liability_local", clear_on_submit=True):
            l_categoria = st.selectbox("Categoria (passivo)", ["Financiamento Imobiliário","Empréstimo Pessoal","Cartão de Crédito","Outros"])
            l_descricao = st.text_input("Descrição do passivo")
            l_valor = st.number_input("Valor (R$)", min_value=0.0, step=100.0, format="%.2f")
            submitted_liab = st.form_submit_button("Adicionar passivo")
            if submitted_liab:
                import_batch_id = f"manual_liab_{int(time.time())}_{uuid.uuid4().hex[:6]}"
                try:
                    add_liability(l_categoria, l_descricao, l_valor, source="manual_form", import_batch_id=import_batch_id)
                    st.success("Passivo adicionado com sucesso.")
                except Exception as e:
                    st.error(f"Falha ao adicionar passivo: {e}")
                safe_rerun()

    # Right column: lists, edit/remove controls
    with col_right:
        st.subheader("Ativos manuais")
        df_assets = list_assets()
        if not df_assets.empty:
            df_display = df_assets.copy()
            df_display["valor"] = df_display["valor"].apply(_format_brl)
            st.dataframe(df_display, use_container_width=True)

            st.markdown("**Gerenciar ativos**")
            opts = df_assets.apply(lambda r: f"{int(r.id)} | {r.categoria} | {r.descricao} | {_format_brl(r.valor)}", axis=1).tolist()
            sel = st.selectbox("Selecionar ativo", [""] + opts, key="ctrl_asset_sel")
            if sel:
                aid = int(sel.split("|")[0].strip())
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
                        try:
                            update_asset(aid, ca, cd, cv)
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
        else:
            st.info("Nenhum ativo manual registrado.")

        st.markdown("---")
        st.subheader("Passivos manuais")
        df_liab = list_liabilities()
        if not df_liab.empty:
            df_display_l = df_liab.copy()
            df_display_l["valor"] = df_display_l["valor"].apply(_format_brl)
            st.dataframe(df_display_l, use_container_width=True)

            st.markdown("**Gerenciar passivos**")
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
                        try:
                            update_liability(lid, la, ld, lv)
                            st.success("Passivo atualizado.")
                        except Exception as e:
                            st.error(f"Falha ao atualizar passivo: {e}")
                        safe_rerun()
                    if b_del_l:
                        try:
                            delete_liability(lid)
                            st.success("Passivo removido.")
                        except Exception as e:
                            st.error(f"Falha ao remover passivo: {e}")
                        safe_rerun()
        else:
            st.info("Nenhum passivo manual registrado.")

    st.markdown("---")
    st.info("Use este painel para controlar holdings, bens e dívidas. Para produção, adicione autenticação e validação adicional.")
