# src/utils_streamlit.py
from __future__ import annotations

import os
import yaml
from typing import Dict, List, Optional, Tuple, Literal
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pandas as pd
import numpy as np
import plotly.express as px
from pandas.api.types import is_numeric_dtype


# =============================
# Seção 0 - Config e Paths
# =============================

def load_config() -> Tuple[Dict[str, str], dict]:
    """
    Carrega config.yaml do diretório raiz e resolve caminhos relativos.
    Espera em config['paths'] as chaves: data_raw, data_processed, images, report, addons.
    Adiciona:
      - data_preprocessed: pasta de saídas consolidadas (parquet/csv final)
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, "..", "config.yaml")

    with open(config_path, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    paths = {
        "script_dir": script_dir,
        "data_raw": os.path.join(script_dir, config["paths"]["data_raw"]),
        "data_processed": os.path.join(script_dir, config["paths"]["data_processed"]),
        "images": os.path.join(script_dir, config["paths"]["images"]),
        "report": os.path.join(script_dir, config["paths"]["report"]),
        "addons": os.path.join(script_dir, config["paths"]["addons"]),
        "data_preprocessed": os.path.join(script_dir, "..", "data", "preprocessed"),
    }
    ensure_dir(paths["data_preprocessed"])
    return paths, config


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


# =============================
# Logging
# =============================

_LOGGER: Optional[logging.Logger] = None

def get_logger() -> logging.Logger:
    global _LOGGER
    if _LOGGER is not None:
        return _LOGGER

    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        log_dir = os.path.join(script_dir, "..", "logs")
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, "app.log")
    except Exception:
        log_path = "app.log"

    logger = logging.getLogger("t2pvd")
    logger.setLevel(logging.INFO)

    fh = RotatingFileHandler(log_path, maxBytes=1_000_000, backupCount=5, encoding="utf-8")
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.propagate = False
    logger.info("logger_initialized | path=%s", log_path)

    _LOGGER = logger
    return logger


def log_call(fn):
    """Decorator simples para logar entrada/saída das funções utilitárias."""
    def wrapper(*args, **kwargs):
        lg = get_logger()
        try:
            lg.info("call_start | fn=%s", fn.__name__)
            for i, a in enumerate(args):
                if isinstance(a, pd.DataFrame):
                    lg.info("arg_df | idx=%d | shape=%s | cols=%s", i, a.shape, list(a.columns)[:10])
            for k, v in kwargs.items():
                if isinstance(v, pd.DataFrame):
                    lg.info("kw_df | key=%s | shape=%s | cols=%s", k, v.shape, list(v.columns)[:10])
                else:
                    lg.info("kw | %s=%s", k, v)

            out = fn(*args, **kwargs)

            if isinstance(out, pd.DataFrame):
                lg.info("call_end_df | fn=%s | shape=%s | cols=%s", fn.__name__, out.shape, list(out.columns)[:10])
            else:
                lg.info("call_end | fn=%s | type=%s", fn.__name__, type(out).__name__)
            return out
        except Exception as e:
            lg.exception("call_error | fn=%s | err=%s", fn.__name__, e)
            raise
    return wrapper


# ==========================================
# Seção 1 - I/O para o Streamlit (carregar df final)
# ==========================================

@log_call
def load_preprocessed_dataset(paths: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    """
    Carrega o dataset consolidado de data/preprocessed/cnpq_pagamentos_2022_2024.(parquet|csv).
    """
    if paths is None:
        paths, _ = load_config()
    base = Path(paths["data_preprocessed"]) / "cnpq_pagamentos_2022_2024"
    pq_path = base.with_suffix(".parquet")
    csv_path = base.with_suffix(".csv")

    lg = get_logger()
    lg.info("load_preprocessed | pq=%s | csv=%s", pq_path, csv_path)

    if pq_path.exists():
        df = pd.read_parquet(pq_path)
        lg.info("loaded_df | shape=%s | cols=%s", df.shape, list(df.columns))
        return df

    if csv_path.exists():
        df = pd.read_csv(
            csv_path,
            dtype={
                "ANO_REFERENCIA": "Int64",
                "PROCESSO": "string",
                "CPF_HASH": "string",
            }
        )
        lg.info("loaded_df_csv | shape=%s | cols=%s", df.shape, list(df.columns))
        return df

    lg.error("file_not_found | pq=%s | csv=%s", pq_path, csv_path)
    raise FileNotFoundError(
        "Dataset pré-processado não encontrado. "
        "Rode a pipeline que gera `data/preprocessed/cnpq_pagamentos_2022_2024.(parquet|csv)`."
    )


@log_call
def get_dataset_notes(df: pd.DataFrame) -> List[str]:
    notes: List[str] = []

    if "VALOR_PAGO" not in df.columns:
        notes.append("Coluna 'VALOR_PAGO' ausente no dataset.")
    elif df["VALOR_PAGO"].isna().all():
        notes.append("Todos os valores de 'VALOR_PAGO' estão vazios (NaN). Verifique a conversão de moeda.")

    if "ANO_REFERENCIA" not in df.columns:
        notes.append("Coluna 'ANO_REFERENCIA' ausente no dataset.")
    else:
        anos = pd.to_numeric(df["ANO_REFERENCIA"], errors="coerce").dropna().astype(int).unique()
        if len(anos) == 0:
            notes.append("Nenhum 'ANO_REFERENCIA' válido encontrado.")

    # Regiões / UFs
    if ("SIGLA_UF_DESTINO" not in df.columns) and ("SIGLA_UF_ORIGEM" not in df.columns):
        notes.append("Colunas de UF ausentes (SIGLA_UF_DESTINO / SIGLA_UF_ORIGEM).")

    return notes


@log_call
def list_available_years(df: pd.DataFrame) -> List[int]:
    if "ANO_REFERENCIA" not in df.columns:
        return []
    anos = pd.to_numeric(df["ANO_REFERENCIA"], errors="coerce").dropna().astype(int).unique()
    years = sorted(anos.tolist())
    get_logger().info("years_available | %s", years)
    return years


# ==========================================
# Seção 2 - Helpers de tipagem/filtragem
# ==========================================

def _ensure_numeric_valor(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if not is_numeric_dtype(df.get("VALOR_PAGO", pd.Series([], dtype=float))):
        df["VALOR_PAGO"] = pd.to_numeric(df["VALOR_PAGO"], errors="coerce")
    return df

def _filter_year(df: pd.DataFrame, year: Optional[int]) -> pd.DataFrame:
    if year is None:
        return df
    return df[pd.to_numeric(df["ANO_REFERENCIA"], errors="coerce").astype("Int64") == year].copy()


# =========================================================
# Seção 3 - Agregações (para os gráficos)
# =========================================================

@log_call
def agg_total_invest_by_region(df: pd.DataFrame, year: Optional[int] = None,
                               prefer_destino: bool = True) -> pd.DataFrame:
    """
    Soma de VALOR_PAGO por REGIÃO (usa REGIAO_DESTINO quando disponível; fallback: REGIAO ou UF).
    Retorna colunas: ['REGIAO', 'valor_total', 'n_linhas'].
    """
    df = _ensure_numeric_valor(df)
    df = _filter_year(df, year)

    if "REGIAO_DESTINO" in df.columns:
        reg_col = "REGIAO_DESTINO"
    elif "REGIAO" in df.columns:
        reg_col = "REGIAO"
    else:
        # fallback bruto via UF, se não houver região no dataset final
        uf_col = "SIGLA_UF_DESTINO" if prefer_destino and ("SIGLA_UF_DESTINO" in df.columns) else \
                 ("SIGLA_UF_ORIGEM" if "SIGLA_UF_ORIGEM" in df.columns else None)
        if uf_col is None:
            raise ValueError("Sem coluna de região ou UF para agregar por região.")
        reg_col = uf_col  # agrega por UF mesmo

    out = (df.dropna(subset=[reg_col])
             .groupby(reg_col, as_index=False)
             .agg(valor_total=("VALOR_PAGO", "sum"),
                  n_linhas=("VALOR_PAGO", "size"))
             .rename(columns={reg_col: "REGIAO"}))
    out["valor_total"] = out["valor_total"].astype(float)
    return out.sort_values("valor_total", ascending=False, kind="mergesort").reset_index(drop=True)


@log_call
def agg_total_invest_by_area(df: pd.DataFrame, year: Optional[int] = None,
                             level: Literal["GRANDE_AREA","AREA","SUBAREA"] = "AREA") -> pd.DataFrame:
    """
    Soma de VALOR_PAGO por área/grande área/subárea.
    Retorna: [<level>, 'valor_total', 'n_linhas'].
    """
    df = _ensure_numeric_valor(df)
    df = _filter_year(df, year)
    col = level
    if col not in df.columns:
        raise ValueError(f"Coluna '{col}' ausente para agregar.")

    out = (df.dropna(subset=[col])
             .groupby(col, as_index=False)
             .agg(valor_total=("VALOR_PAGO","sum"),
                  n_linhas=("VALOR_PAGO","size")))
    out["valor_total"] = out["valor_total"].astype(float)
    return out.sort_values("valor_total", ascending=False, kind="mergesort").reset_index(drop=True)


@log_call
def agg_invest_by_category(df: pd.DataFrame, year: Optional[int] = None,
                           how: Literal["sum","per_beneficiary_mean","per_process_mean"]="sum") -> pd.DataFrame:
    """
    Investimento por categoria de bolsa (MODALIDADE).
    - how="sum": soma total (clássico)
    - how="per_beneficiary_mean": média por beneficiário (pondera pela quantidade de bolsistas)
    - how="per_process_mean": média por processo (pondera pela quantidade de processos)
    Retorna: ['MODALIDADE','valor','n_base'] onde 'valor' é a métrica escolhida.
    """
    df = _ensure_numeric_valor(df)
    df = _filter_year(df, year)

    if "MODALIDADE" not in df.columns:
        raise ValueError("Coluna 'MODALIDADE' ausente.")

    if how == "sum":
        out = (df.dropna(subset=["MODALIDADE"])
                 .groupby("MODALIDADE", as_index=False)
                 .agg(valor_em_reais=("VALOR_PAGO","sum"),
                      n_unicos=("VALOR_PAGO","size")))
    elif how == "per_beneficiary_mean":
        # média do total por beneficiário, por categoria
        if "BENEFICIARIO" not in df.columns:
            raise ValueError("Métrica 'per_beneficiary_mean' requer 'BENEFICIARIO'.")
        censored_names = ["XXXX", "XXX XXX XXX"]
        df_tmp = df[~df["BENEFICIARIO"].isin(censored_names)]
        tmp = (df_tmp.dropna(subset=["BENEFICIARIO","MODALIDADE"])
                 .groupby(["MODALIDADE","BENEFICIARIO"], as_index=False)["VALOR_PAGO"].sum())
        out = (tmp.groupby("MODALIDADE", as_index=False)
                 .agg(valor_em_reais=("VALOR_PAGO","mean"),
                      n_unicos=("BENEFICIARIO","nunique")))
    else:  # per_process_mean
        if "PROCESSO" not in df.columns:
            raise ValueError("Métrica 'per_process_mean' requer 'PROCESSO'.")
        tmp = (df.dropna(subset=["PROCESSO","MODALIDADE"])
                 .groupby(["MODALIDADE","PROCESSO"], as_index=False)["VALOR_PAGO"].sum())
        out = (tmp.groupby("MODALIDADE", as_index=False)
                 .agg(valor_em_reais=("VALOR_PAGO","mean"),
                      n_unicos=("PROCESSO","nunique")))

    out["valor_em_reais"] = out["valor_em_reais"].astype(float)
    return out.sort_values("valor_em_reais", ascending=False, kind="mergesort").reset_index(drop=True)


@log_call
def agg_box_data_by_category(df: pd.DataFrame, year: Optional[int] = None, category: Optional[str] = None) -> pd.DataFrame:
    """
    Retorna dados prontos para boxplot: VALOR_PAGO x MODALIDADE (com filtro de ano).
    """
    df = _ensure_numeric_valor(df)
    df = _filter_year(df, year)

    if category is not None:
        df = df[df["MODALIDADE"] == category]

    selected_cols = ["ANO_REFERENCIA", "PROCESSO", "MODALIDADE", "VALOR_PAGO"]
    df_clean = df[selected_cols].dropna(subset=["VALOR_PAGO","MODALIDADE"]).copy()
    # df_avg = df_clean.groupby("MODALIDADE", as_index=False).agg({"VALOR_PAGO" : "mean"})
    # df_avg.rename(columns={"VALOR_PAGO":"VALOR_MEDIO"}, inplace=True)


    return df_clean


@log_call
def agg_box_data_by_area(
        df: pd.DataFrame,
        year: Optional[int] = None, 
        level: Literal["GRANDE_AREA","AREA","SUBAREA"]="AREA",
        level_val : Optional[str] = None
    ) -> pd.DataFrame:
    """
    Dados para boxplot: VALOR_PAGO x área/subárea/grande área.
    """
    df = _ensure_numeric_valor(df)
    df = _filter_year(df, year)

    if level not in df.columns:
        raise ValueError(f"Coluna '{level}' ausente.")
    
    if level_val is not None:
        df = df[df[level] == level_val]
    
    selected_columns = ["ANO_REFERENCIA", "PROCESSO", level, "VALOR_PAGO"]
    df_clean = df[selected_columns].dropna(subset=["VALOR_PAGO", level]).copy()
    # df_avg = df_clean.groupby(level, as_index=False).agg({"VALOR_PAGO" : "mean"})
    # df_avg.rename(columns={"VALOR_PAGO":"VALOR_MEDIO"}, inplace=True)

    return df_clean


@log_call
def agg_box_data_by_area_and_category(
        df: pd.DataFrame,
        year: Optional[int] = None,
        level: Literal["GRANDE_AREA","AREA","SUBAREA"]="AREA",
        level_val : Optional[str] = None,
        category : Optional[str] = None
    ) -> pd.DataFrame:
    """
    Dados para boxplot combinado: VALOR_PAGO x área(subárea) color/facet por MODALIDADE.
    """
    df = _ensure_numeric_valor(df)
    df = _filter_year(df, year)
    required = [level, "MODALIDADE"]

    for c in required:
        if c not in df.columns:
            raise ValueError(f"Coluna '{c}' ausente.")
        
    if level_val is not None:
        if category is not None:
            df = df[(df[level] == level_val) & (df["MODALIDADE"] == category)]
        else:
            df = df[df[level] == level_val]
    elif category is not None:
        df = df[df["MODALIDADE"] == category]

    selected_columns = ["ANO_REFERENCIA", "PROCESSO", "MODALIDADE", level, "VALOR_PAGO"]
    df_clean = df[selected_columns].dropna(subset=["VALOR_PAGO", level, "MODALIDADE"]).copy()
    # df_avg = df_clean.groupby([level, "MODALIDADE"], as_index=False).agg({"VALOR_PAGO" : "mean"})
    # df_avg.rename(columns={"VALOR_PAGO":"VALOR_MEDIO"}, inplace=True)
    
    return df_clean


@log_call
def agg_time_mean_by_category(df: pd.DataFrame) -> pd.DataFrame:
    """
    Progressão temporal: média de VALOR_PAGO por ANO_REFERENCIA e MODALIDADE.
    Retorna: ['ANO_REFERENCIA','MODALIDADE','media_valor'] com anos ordenados.
    """
    df = _ensure_numeric_valor(df).copy()
    df["ANO_REFERENCIA"] = pd.to_numeric(df["ANO_REFERENCIA"], errors="coerce").astype("Int64")
    out = (df.dropna(subset=["ANO_REFERENCIA","MODALIDADE"])
             .groupby(["ANO_REFERENCIA","MODALIDADE"], as_index=False)
             .agg(media_valor=("VALOR_PAGO","mean")))
    out["media_valor"] = out["media_valor"].astype(float)
    return out.sort_values(["ANO_REFERENCIA","MODALIDADE"], kind="mergesort").reset_index(drop=True)


# =============================================
# Seção 4 - Gráficos (Plotly) para o Streamlit
# =============================================

@log_call
def fig_bar_mean_by_uf(df_agg: pd.DataFrame, year: int) -> "px.Figure":
    if df_agg.empty:
        return px.bar(title=f"Sem dados para exibir em {year}")

    fig = px.bar(
        df_agg,
        x="UF",
        y="media_valor_pago",
        text="media_valor_pago",
        labels={"UF": "UF", "media_valor_pago": "Média de Valor Pago (R$)"},
        title=f"Média de Valor Pago por UF - {year}",
    )
    fig.update_traces(texttemplate="R$ %{y:,.2f}", textposition="outside", marker_color="#118AB2")
    fig.update_layout(
        xaxis_tickangle=-30,
        yaxis=dict(tickformat="R$,.2f"),
        bargap=0.2,
        template="plotly_white",
        margin=dict(l=20, r=20, t=60, b=100),
    )
    return fig


@log_call
def fig_bar_total_by_region(df_agg: pd.DataFrame, year: Optional[int]) -> "px.Figure":
    title = f"Investimento total por Região" + (f" — {year}" if year else "")
    if df_agg.empty:
        return px.bar(title=f"Sem dados para exibir. {title}")

    fig = px.bar(
        df_agg,
        x="REGIAO",
        y="valor_total",
        text="valor_total",
        labels={"REGIAO":"Região","valor_total":"Total investido (R$)"},
        title=title
    )
    fig.update_traces(texttemplate="R$ %{y:,.2f}", textposition="outside", marker_color="#06D6A0")
    fig.update_layout(yaxis=dict(tickformat="R$,.2f"), template="plotly_white", bargap=0.25)
    return fig


@log_call
def fig_bar_total_by_area(df_agg: pd.DataFrame, year: Optional[int],
                          level: str = "AREA", top_n: Optional[int] = 20) -> "px.Figure":
    title = f"Investimento total por {level.replace('_',' ').title()}" + (f" — {year}" if year else "")
    data = df_agg.copy()
    if top_n is not None and len(data) > top_n:
        data = data.nlargest(top_n, "valor_total")
    x_col = level
    fig = px.bar(
        data,
        x=x_col,
        y="valor_total",
        text="valor_total",
        labels={x_col: level.replace("_"," ").title(), "valor_total":"Total investido (R$)"},
        title=title
    )
    fig.update_traces(texttemplate="R$ %{y:,.2f}", marker_color="#FFD166")
    fig.update_layout(yaxis=dict(tickformat="R$,.2f"), xaxis_tickangle=-30, template="plotly_white", bargap=0.25)
    return fig


@log_call
def fig_bar_category(df_agg: pd.DataFrame, year: Optional[int], metric_label: str, top_n: Optional[int] = 20) -> "px.Figure":
    """
    Mostra soma / média ponderada por categoria, dependendo do df_agg passado (coluna 'valor_em_reais').
    """
    title = f"Investimento por Categoria de Bolsa ({metric_label})" + (f" — {year}" if year else "")
    data = df_agg.copy()
    if top_n is not None and len(data) > top_n:
        data = data.nlargest(top_n, "valor_em_reais")
    fig = px.bar(
        data,
        x="MODALIDADE",
        y="valor_em_reais",
        text="valor_em_reais",
        labels={"MODALIDADE":"Modalidade","valor_em_reais":metric_label},
        title=title
    )
    fig.update_traces(texttemplate="R$ %{y:,.2f}", marker_color="#118AB2")
    fig.update_layout(yaxis=dict(tickformat="R$,.2f"), xaxis_tickangle=-30, template="plotly_white", bargap=0.25)
    return fig


@log_call
def fig_box_by_category(df_box: pd.DataFrame, year: Optional[int] = None, modalidade: Optional[str] = None) -> "px.Figure":
    title = f"Distribuição de valores: {modalidade if modalidade is not None else 'Todas as modalidades'} ({year if year is not None else 'Todos os anos'})" 
    fig = px.box(
        df_box,
        x="MODALIDADE",
        y="VALOR_PAGO",
        points="outliers",
        labels={"MODALIDADE":"Modalidade","VALOR_PAGO":"Valor pago (R$)"},
        title=title
    )

    # for modalidade_atual in df_box["MODALIDADE"].unique():
    #     df_mod = df_box[df_box["MODALIDADE"] == modalidade_atual]
    #     media = df_mod["VALOR_MEDIO"].mean()
        
    #     fig.add_trace(px.Scatter(
    #         x=[modalidade_atual],
    #         y=[media],
    #         mode='markers',
    #         marker=dict(color='red', size=12, symbol='diamond'),
    #         name=f'Média: R$ {media:,.2f}',
    #         showlegend=True
    #     ))
    
    fig.update_layout(yaxis=dict(tickformat="R$,.2f"), xaxis_tickangle=-30, template="plotly_white")
    fig.update_layout(height=500)
    return fig


@log_call
def fig_box_by_area(df_box: pd.DataFrame, year: Optional[int], level: str = "AREA", level_val: Optional[str] = None) -> "px.Figure":
    title = f"Distribuição de valores: {level_val if level_val is not None else level} ({year if year is not None else 'Todos os anos'})" 
    fig = px.box(
        df_box,
        x=level,
        y="VALOR_PAGO",
        points="outliers",
        labels={level: level.replace("_"," ").title(), "VALOR_PAGO":"Valor pago (R$)"},
        title=title
    )

    # for area_atual in df_box[level].unique():
    #     df_area = df_box[df_box[level] == area_atual]
    #     media = df_area["VALOR_MEDIO"].mean()
        
    #     fig.add_trace(px.Scatter(
    #         x=[area_atual],
    #         y=[media],
    #         mode='markers',
    #         marker=dict(color='red', size=12, symbol='diamond'),
    #         name=f'Média: R$ {media:,.2f}',
    #         showlegend=True
    #     ))
    
    fig.update_layout(yaxis=dict(tickformat="R$,.2f"), xaxis_tickangle=-30, template="plotly_white")
    fig.update_layout(height=500)
    return fig


@log_call
def fig_box_by_area_and_category(df_box: pd.DataFrame, year: Optional[int],
                                 level: str = "AREA", facet: bool = False) -> "px.Figure":
    """
    Boxplot combinado: por área(subárea) com cor por categoria; opcional facet por categoria.
    """
    title = f"Distribuição do Valor Pago por {level.replace('_',' ').title()} e Categoria" + (f" — {year}" if year else "")
    if facet:
        fig = px.box(
            df_box, x=level, y="VALOR_PAGO",
            facet_col="MODALIDADE", facet_col_wrap=3,
            points=False, color_discrete_sequence=px.colors.qualitative.Set2,
            labels={level: level.replace("_"," ").title(), "VALOR_PAGO":"Valor pago (R$)"},
            title=title
        )
    else:
        fig = px.box(
            df_box, x=level, y="VALOR_PAGO",
            color="MODALIDADE", points="outliers",
            color_discrete_sequence=px.colors.qualitative.Set2,
            labels={level: level.replace("_"," ").title(), "VALOR_PAGO":"Valor pago (R$)","MODALIDADE":"Modalidade"},
            title=title
        )
    fig.update_layout(yaxis=dict(tickformat="R$,.2f"), xaxis_tickangle=-30, template="plotly_white")
    return fig


@log_call
def fig_time_mean_by_category(df_time: pd.DataFrame, kind: Literal["line","area"]="line") -> "px.Figure":
    """
    Progressão da MÉDIA ao longo dos anos por categoria:
    - kind="line": linhas separadas por categoria (média)
    - kind="area": stacked area com média (apenas visual, soma de médias não tem interpretação aditiva forte)
    """
    title = "Progressão da média de Valor Pago por Categoria ao longo dos anos"
    if kind == "line":
        fig = px.line(
            df_time, x="ANO_REFERENCIA", y="media_valor", color="MODALIDADE",
            markers=True,
            labels={"ANO_REFERENCIA":"Ano","media_valor":"Média do valor (R$)","MODALIDADE":"Modalidade"},
            title=title
        )
    else:
        fig = px.area(
            df_time, x="ANO_REFERENCIA", y="media_valor", color="MODALIDADE",
            groupnorm=None,
            labels={"ANO_REFERENCIA":"Ano","media_valor":"Média do valor (R$)","MODALIDADE":"Modalidade"},
            title=title
        )
    fig.update_layout(yaxis=dict(tickformat="R$,.2f"), template="plotly_white")
    return fig


# ======================================================
# Seção 5 - (Opcional) Média por UF por ano (já existente)
# ======================================================

def _choose_uf_column(df: pd.DataFrame, preference: Optional[str] = None) -> str:
    dest_exists = "SIGLA_UF_DESTINO" in df.columns
    orig_exists = "SIGLA_UF_ORIGEM" in df.columns

    if preference == "DESTINO" and dest_exists:
        return "SIGLA_UF_DESTINO"
    if preference == "ORIGEM" and orig_exists:
        return "SIGLA_UF_ORIGEM"

    if dest_exists and orig_exists:
        nd = df["SIGLA_UF_DESTINO"].notna().sum()
        no = df["SIGLA_UF_ORIGEM"].notna().sum()
        return "SIGLA_UF_DESTINO" if nd >= no else "SIGLA_UF_ORIGEM"
    if dest_exists:
        return "SIGLA_UF_DESTINO"
    if orig_exists:
        return "SIGLA_UF_ORIGEM"
    raise ValueError("Nenhuma coluna de UF encontrada no dataset.")


@log_call
def mean_valor_pago_by_uf_for_year(df: pd.DataFrame, year: int, uf_preference: Optional[str] = None) -> pd.DataFrame:
    required = ["ANO_REFERENCIA", "VALOR_PAGO"]
    for c in required:
        if c not in df.columns:
            raise ValueError(f"Coluna obrigatória ausente: {c}")

    df = df.copy()
    if not is_numeric_dtype(df["VALOR_PAGO"]):
        df["VALOR_PAGO"] = pd.to_numeric(df["VALOR_PAGO"], errors="coerce")
    df["ANO_REFERENCIA"] = pd.to_numeric(df["ANO_REFERENCIA"], errors="coerce").astype("Int64")

    uf_col = _choose_uf_column(df, preference=uf_preference)
    df[uf_col] = df[uf_col].astype("string").str.upper().str.replace(r"[^A-Z]", "", regex=True).str[:3]

    df_year = df[df["ANO_REFERENCIA"] == year]

    out = (
        df_year.dropna(subset=[uf_col])
              .groupby(uf_col, as_index=False)
              .agg(media_valor_pago=("VALOR_PAGO", "mean"),
                   n=("VALOR_PAGO", "size"))
              .rename(columns={uf_col: "UF"})
    )
    out["media_valor_pago"] = out["media_valor_pago"].astype(float)
    out["UF"] = out["UF"].astype(str)
    return out.sort_values("media_valor_pago", ascending=False, kind="mergesort").reset_index(drop=True)
