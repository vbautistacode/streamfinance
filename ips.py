# ips.py
import streamlit as st
import pandas as pd
from sqlalchemy import text
from db import engine

DDL_IPS = """
CREATE TABLE IF NOT EXISTS ips (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT,
  content TEXT,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
"""

def ensure_ips_table():
    dialect = engine.dialect.name.lower()
    if dialect == "sqlite":
        raw = engine.raw_connection()
        try:
            cur = raw.cursor()
            cur.executescript(DDL_IPS)
            raw.commit()
        finally:
            cur.close()
            raw.close()
    else:
        statements = [s.strip() for s in DDL_IPS.split(";") if s.strip()]
        with engine.begin() as conn:
            for stmt in statements:
                conn.execute(text(stmt))

def save_ips(title: str, content: str):
    ensure_ips_table()
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO ips (title, content) VALUES (:t, :c)"), {"t": title, "c": content})

def load_latest_ips():
    ensure_ips_table()
    try:
        with engine.connect() as conn:
            df = pd.read_sql("SELECT * FROM ips ORDER BY id DESC LIMIT 1", conn)
            if df.empty:
                return None
            return df.iloc[0].to_dict()
    except Exception:
        return None

def list_ips(limit: int = 50):
    ensure_ips_table()
    try:
        with engine.connect() as conn:
            return pd.read_sql(f"SELECT * FROM ips ORDER BY id DESC LIMIT {int(limit)}", conn)
    except Exception:
        return pd.DataFrame()

def delete_ips(ips_id: int):
    ensure_ips_table()
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM ips WHERE id = :id"), {"id": ips_id})

# Render function moved to this module
def render_ips():
    st.header("Modelo de Investment Policy Statement (IPS)")
    st.markdown("Use este espaço para registrar a política de investimentos do investidor. Você pode editar e salvar a versão atual no banco de dados.")

    ips_row = load_latest_ips()
    default_title = ips_row["title"] if ips_row else "Investment Policy Statement - Cliente"
    default_content = ips_row["content"] if ips_row else """1. Introdução e Propósito

Identificação do Investidor:

Objetivo do Documento: Definir as diretrizes para a gestão dos ativos, estabelecer expectativas de retorno e níveis de risco aceitáveis, e criar uma base para a avaliação de desempenho.

2. Governança e Responsabilidades

Comitê/Gestor: .

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