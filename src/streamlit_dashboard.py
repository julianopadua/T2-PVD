# streamlit_dashboard.py
import streamlit as st
import pandas as pd

from utils_streamlit import (
    load_config,
    load_preprocessed_dataset,
    get_dataset_notes,
    list_available_years,
    # UF por ano
    mean_valor_pago_by_uf_for_year,
    fig_bar_mean_by_uf,
    # Região
    agg_total_invest_by_region,
    fig_bar_total_by_region,
    # Áreas
    agg_total_invest_by_area,
    fig_bar_total_by_area,
    # Categorias
    agg_invest_by_category,
    fig_bar_category,
    # Boxplots
    agg_box_data_by_category,
    fig_box_by_category,
    agg_box_data_by_area,
    fig_box_by_area,
    agg_box_data_by_area_and_category,
    fig_box_by_area_and_category,
    # Evolução temporal
    agg_time_mean_by_category,
    fig_time_mean_by_category,
    # logs
    get_logger,
)

st.set_page_config(page_title="CNPq Pagamentos - Dashboard", page_icon="📊", layout="wide")
st.title("📊 CNPq - Dashboard de Pagamentos")
st.caption("Exploração do dataset pré-processado (2022–2024).")

# Sidebar - controles globais
with st.sidebar:
    st.header("Dashboard")
    try:
        paths, _ = load_config()
        df = load_preprocessed_dataset(paths)
    except Exception as e:
        st.error(
            "Não consegui carregar o dataset pré-processado.\n\n"
            "Gere antes `data/preprocessed/cnpq_pagamentos_2022_2024.(parquet|csv)`.\n\n"
            f"Detalhes: {e}"
        )
        st.stop()

    # avisos rápidos
    for note in get_dataset_notes(df):
        st.warning(note)

    page = st.radio("Navegar:", ["Distribuição de verba", "Valor médio da bolsa", "Evolução temporal"])

    anos = list_available_years(df)
    if not anos:
        st.error("Nenhum ano disponível em 'ANO_REFERENCIA'.")
        st.stop()

    default_year = max(anos)
    st.subheader("Filtros Globais")
    year_global = st.selectbox("Ano padrão", options=anos, index=anos.index(default_year))

    st.markdown("---")
    st.caption("Logs em `logs/app.log` (veja no disco).")

# Pergunta 1 - Como o CNPq distribui as bolsas?
if page == "Distribuição de verba":
    tab_geo, tab_area, tab_cat = st.tabs(["Por região", "Por área de pesquisa", "Por modalidade"])

    # Por região
    with tab_geo:
        st.subheader("🧭 Investimento por Região")

        year_region_opt = st.segmented_control(
            "Ano",
            options=[None] + anos,
            default=year_global,
            format_func=lambda x: "Todos os anos" if x is None else str(x),
            key="year_region_opt",
        )

        agg_reg = agg_total_invest_by_region(df, year=year_region_opt)
        fig_reg = fig_bar_total_by_region(agg_reg, year_region_opt)
    
        st.plotly_chart(fig_reg, use_container_width=True)

        show_tables = st.checkbox("Mostrar tabelas", key="show_table_region")
        if show_tables:
            st.dataframe(agg_reg, use_container_width=True, hide_index=True)

    # Por área
    with tab_area:
        st.subheader("🧩 Investimento por área")

        col1, col2, col3 = st.columns([1, 1, 2], gap="small")
        with col1:
            level = st.selectbox("Nível", ["GRANDE_AREA", "AREA", "SUBAREA"], index=1)
        with col2:
            year_area_opt = st.segmented_control(
                "Ano",
                options=[None] + anos,
                default=year_global,
                format_func=lambda x: "Todos os anos" if x is None else str(x),
                key="year_area_opt",
            )
        with col3:
            topn = st.slider("Top N para exibir", min_value=5, max_value=100, value=10, step=5, width=500)

        agg_area = agg_total_invest_by_area(df, year=year_area_opt, level=level)
        fig_area = fig_bar_total_by_area(agg_area, year_area_opt, level=level, top_n=topn)
        
        st.plotly_chart(fig_area, use_container_width=True)
        
        show_tables = st.checkbox("Mostrar tabelas", key="show_table_area")
        if show_tables:
            st.dataframe(agg_area, use_container_width=True, hide_index=True)

    # Por modalidade
    with tab_cat:
        st.subheader("🏷️ Investimento por modalidade de bolsa")

        col1, col2 = st.columns([1,3], gap="small")
        with col1:
            year_cat = st.segmented_control(
                "Ano", 
                options=[None] + anos,
                default=year_global,
                format_func=lambda x: "Todos os anos" if x is None else str(x),
                key="year_cat")
            # year_cat = st.selectbox("Ano", options=anos, index=anos.index(year_global), key="year_cat")
        with col2:
            topn = st.slider("Limite de modalidades a exibir", min_value=5, max_value=40, value=10, step=5, width=500)

        # soma total
        agg_cat_sum = agg_invest_by_category(df, year=year_cat, how="sum")
        fig_cat_sum = fig_bar_category(agg_cat_sum, year_cat, metric_label="Total (R$)", top_n=topn)

        # média por beneficiário (ponderada)
        agg_cat_benef = agg_invest_by_category(df, year=year_cat, how="per_beneficiary_mean")
        fig_cat_benef = fig_bar_category(agg_cat_benef, year_cat, metric_label="Média por beneficiário (R$)", top_n=topn)

        # média por processo (ponderada)
        agg_cat_proc = agg_invest_by_category(df, year=year_cat, how="per_process_mean")
        fig_cat_proc = fig_bar_category(agg_cat_proc, year_cat, metric_label="Média por processo (R$)", top_n=topn)

        col1, col2 = st.columns([1,1], gap="small")
        with col1:
            # st.markdown("**Total por categoria**")
            st.plotly_chart(fig_cat_sum, use_container_width=True)
            # st.markdown("**Média por beneficiário (ponderada)**")
            st.plotly_chart(fig_cat_benef, use_container_width=True)
        with col2:
            # st.markdown("**Média por processo (ponderada)**")
            st.plotly_chart(fig_cat_proc, use_container_width=True)

        show_tables = st.checkbox("Mostrar tabelas", key="show_table_mod")
        if show_tables:
            st.markdown("**Tabelas**")
            c1, c2, c3 = st.columns(3)
            with c1:
                st.markdown("Total")
                st.dataframe(agg_cat_sum, use_container_width=True, hide_index=True)
            with c2:
                st.markdown("Médio por processo (ponderada)")
                st.dataframe(agg_cat_proc, use_container_width=True, hide_index=True)
            with c3:
                st.markdown("Média por beneficiário")
                st.dataframe(agg_cat_benef, use_container_width=True, hide_index=True)

                

elif page=="Valor médio da bolsa":
    st.subheader("📦 Boxplots do valor pago")

    # Opções de ano
    col1, col2 = st.columns([1, 1], gap="large")
    with col1:
        year_box = st.segmented_control(
            "Ano", 
            options=[None] + anos,
            default=year_global,
            format_func=lambda x: "Todos os anos" if x is None else str(x),
            key="year_box")
    with col2:
        facet_on = st.checkbox("Facet por categoria", value=False)

    # Seleção específica
    col1, col2 = st.columns([1, 1], gap="large")
    with col1:
        cat_box = st.selectbox("Modalidade", options=df["MODALIDADE"].unique(), key="cat_box")
    with col2:
        level_box = st.selectbox("Nível (área)", ["GRANDE_AREA", "AREA", "SUBAREA"], index=1)
        level_opt_box = st.selectbox("Área", options=df[level_box].unique(), index=1)

    # Por categoria
    df_box_cat = agg_box_data_by_category(df, year=year_box, category=cat_box)
    fig_box_cat = fig_box_by_category(df_box_cat, year_box, modalidade=cat_box)

    # Por área
    df_box_area = agg_box_data_by_area(df, year=year_box, level=level_box, level_val=level_opt_box)
    fig_box_area_plot = fig_box_by_area(df_box_area, year=year_box, level=level_box, level_val=level_opt_box)

    # Combinado
    df_box_comb = agg_box_data_by_area_and_category(df, year=year_box, level=level_box, category=cat_box, level_val=level_opt_box)
    fig_box_comb = fig_box_by_area_and_category(df_box_comb, year=year_box, level=level_box, facet=facet_on)

    col1, col2 = st.columns([1, 1], gap="large")
    with col1:
        # st.markdown("**Por categoria**")
        st.plotly_chart(fig_box_cat, use_container_width=True)
    with col2:
        # st.markdown(f"**Por {level_box.title().replace('_',' ')}**")
        st.plotly_chart(fig_box_area_plot, use_container_width=True)
    
    # st.markdown(f"**Combinado: {level_box.title().replace('_',' ')} × Categoria**")
    st.plotly_chart(fig_box_comb, use_container_width=True)


    show_tables = st.checkbox("Mostrar tabelas", key="show_table_mod")
    if show_tables:
        col1, col2 = st.columns([1, 1], gap="large")
        with col1:
            st.dataframe(df_box_cat, use_container_width=True, hide_index=True)
        with col2:
            st.dataframe(df_box_area, use_container_width=True, hide_index=True)
        st.dataframe(df_box_comb, use_container_width=True, hide_index=True)


elif page=="Evolução temporal":
    st.subheader("⏱️ Progressão da média por categoria ao longo dos anos")
    kind = st.radio("Tipo de gráfico", ["line", "area"], horizontal=True, index=0)
    
    df_time = agg_time_mean_by_category(df)
    fig_time = fig_time_mean_by_category(df_time, kind=kind)
    st.plotly_chart(fig_time, use_container_width=True)

# Debug mínimo opcional
with st.expander("🔎 Debug rápido"):
    logger = get_logger()
    cnt = df.assign(_ano=pd.to_numeric(df["ANO_REFERENCIA"], errors="coerce")).groupby("_ano").size().reset_index(name="rows")
    st.write("Contagem por ano:")
    st.dataframe(cnt, hide_index=True, use_container_width=True)
    st.write("Tipos:")
    st.write(df.dtypes.astype(str))
