# utils.py
import streamlit as st

def safe_rerun():
    """
    Tenta reiniciar o script com st.experimental_rerun().
    Se não estiver disponível ou falhar, mostra instrução para o usuário.
    """
    try:
        # chama experimental_rerun se existir
        if hasattr(st, "experimental_rerun"):
            st.experimental_rerun()
            return
    except Exception:
        pass
    # fallback: instruir o usuário a atualizar manualmente
    st.info("Alteração salva. Atualize a página manualmente para ver as mudanças.")

def format_brl(value):
    try:
        v = float(value)
    except Exception:
        return value
    s = f"{v:,.2f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"
