import streamlit as st
import pandas as pd

from utils import (
    load_config,
    load_preprocessed_dataset,
    list_available_years,
    mean_valor_pago_by_uf_for_year,
    fig_bar_mean_by_uf,
    get_dataset_notes,
    get_logger,   # para mostrar caminho do log
)

st.set_page_config(
    page_title="CNPq Pagamentos - UF por Ano",
    page_icon="🗺️",
    layout="wide",
)

st.title("🗺️ CNPq - Média do Valor Pago por UF")
st.caption("Selecione um ano e visualize a média de valor pago por UF (Destino/Origem).")

# Sidebar - controles
with st.sidebar:
    st.header("Configuração")
    uf_pref = st.radio(
        "Coluna de UF",
        options=["AUTO", "DESTINO", "ORIGEM"],
        index=0,
        help="AUTO escolhe a coluna com maior cobertura. Você pode forçar DESTINO ou ORIGEM."
    )
    show_table = st.checkbox("Mostrar tabela agregada", value=True)
    show_raw = st.checkbox("Mostrar amostra do dataset bruto", value=False)
    show_debug = st.checkbox("Mostrar debug", value=True)
    st.markdown("---")
    st.caption("Espera um dataset pré-processado em `data/preprocessed/`.")

# Carrega dataset
try:
    paths, _ = load_config()
    df = load_preprocessed_dataset(paths)
except Exception as e:
    st.error(
        "Não foi possível carregar o dataset pré-processado. "
        "Rode antes a pipeline que gera `data/preprocessed/cnpq_pagamentos_2022_2024.(parquet|csv)`.\n\n"
        f"Detalhes: {e}"
    )
    st.stop()

# Notas/avisos
notes = get_dataset_notes(df)
if notes:
    for n in notes:
        st.warning(n)

# Seleção de ano
anos = list_available_years(df)
if not anos:
    st.error("Nenhum ano disponível em 'ANO_REFERENCIA'.")
    st.stop()

default_year = max(anos)
year = st.selectbox("Escolha o ano", options=anos, index=anos.index(default_year))

# Agregação e figura
try:
    pref_arg = None if uf_pref == "AUTO" else uf_pref
    agg = mean_valor_pago_by_uf_for_year(df, year, uf_preference=pref_arg)
    fig = fig_bar_mean_by_uf(agg, year)
except Exception as e:
    st.error(f"Falha ao calcular/plotar agregação: {e}")
    st.stop()

st.plotly_chart(fig, use_container_width=True)

# Tabela agregada opcional
if show_table:
    st.subheader(f"Tabela - Média de Valor Pago por UF ({year})")
    st.dataframe(
        agg.assign(media_valor_pago=lambda x: x["media_valor_pago"].round(2)),
        use_container_width=True,
        hide_index=True,
    )

# Amostra do bruto opcional
if show_raw:
    st.subheader("Amostra do Dataset Unificado (bruto)")
    st.dataframe(df.head(200), use_container_width=True, hide_index=True)

# Debug
if show_debug:
    with st.expander("🔎 Debug"):
        st.write("Contagem por ano:")
        cnt = df.assign(_ano=pd.to_numeric(df["ANO_REFERENCIA"], errors="coerce")).groupby("_ano").size().reset_index(name="rows")
        st.dataframe(cnt, hide_index=True, use_container_width=True)

        st.write("Primeiras linhas do dataset (tipos):")
        st.write(df.dtypes.astype(str))

        logger = get_logger()
        st.caption("Logs gravados em `logs/app.log` (veja no disco).")
