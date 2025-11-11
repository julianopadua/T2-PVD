# src/utils.py

from __future__ import annotations

import os
import re
import yaml
from typing import Dict, List, Optional, Tuple
import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
import numpy as np
import plotly.express as px
from pathlib import Path
from pandas.api.types import is_numeric_dtype



# =============================
# Seção 0 - Config e Paths
# =============================

def load_config() -> Tuple[Dict[str, str], dict]:
    """
    Carrega config.yaml do diretório raiz e resolve caminhos relativos.
    Acrescenta:
      - data_preprocessed: pasta de saídas consolidadas
      - data_parquet_yearly: cache anual (parquet por ano)
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
        # NOVO: cache anual parquet
        "data_parquet_yearly": os.path.join(script_dir, "..", "data", "preprocessed", "yearly"),
    }
    ensure_dir(paths["data_preprocessed"])
    ensure_dir(paths["data_parquet_yearly"])
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

    # pastas
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        log_dir = os.path.join(script_dir, "..", "logs")
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, "app.log")
    except Exception:
        # fallback local
        log_path = "app.log"

    logger = logging.getLogger("t2pvd")
    logger.setLevel(logging.INFO)

    fh = RotatingFileHandler(log_path, maxBytes=1_000_000, backupCount=5, encoding="utf-8")
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # silencioso no console (Streamlit já printa)
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
                    lg.info("arg_df | idx=%d | shape=%s | cols=%s", i, a.shape, list(a.columns)[:8])
            for k, v in kwargs.items():
                if isinstance(v, pd.DataFrame):
                    lg.info("kw_df | key=%s | shape=%s | cols=%s", k, v.shape, list(v.columns)[:8])
                else:
                    lg.info("kw | %s=%s", k, v)

            out = fn(*args, **kwargs)

            if isinstance(out, pd.DataFrame):
                lg.info("call_end_df | fn=%s | shape=%s | cols=%s", fn.__name__, out.shape, list(out.columns))
            else:
                lg.info("call_end | fn=%s | type=%s", fn.__name__, type(out).__name__)
            return out
        except Exception as e:
            lg.exception("call_error | fn=%s | err=%s", fn.__name__, e)
            raise
    return wrapper

# ============================
# Cache anual em Parquet
# ============================

def _yearly_parquet_path(paths: Dict[str, str], year: int) -> str:
    fname = f"cnpq_pagamentos_{year}.parquet"
    return os.path.join(paths["data_parquet_yearly"], fname)

def save_year_parquet(df: pd.DataFrame, year: int, paths: Dict[str, str]) -> str:
    """
    Salva df anual normalizado/coagido em Parquet, para reuso.
    """
    p = _yearly_parquet_path(paths, year)
    ensure_dir(os.path.dirname(p))
    df.to_parquet(p, index=False)
    get_logger().info("year_parquet_saved | year=%s | path=%s | shape=%s", year, p, df.shape)
    return p

def load_year_parquet_if_exists(year: int, paths: Dict[str, str]) -> Optional[pd.DataFrame]:
    """
    Se existir o Parquet anual, carrega e retorna; senão, None.
    """
    p = _yearly_parquet_path(paths, year)
    if os.path.exists(p):
        df = pd.read_parquet(p)
        get_logger().info("year_parquet_loaded | year=%s | path=%s | shape=%s", year, p, df.shape)
        return df
    return None


# ==============================================
# Seção 1 - Descoberta de arquivos em data/raw
# ==============================================

def discover_raw_files(data_raw: str) -> Dict[str, str]:
    """
    Descobre os três arquivos nas convenções informadas.
    Retorna um dict { 'y2022': <path>, 'y2023': <path>, 'y2024': <path> }.
    Levemente tolerante a variações de maiúsculas/minúsculas.
    """
    files = os.listdir(data_raw)

    # Padrões (flexíveis) para nomes citados
    patt_2022 = re.compile(r"Relatorio*", re.I)
    patt_2023 = re.compile(r"Dados-de-Pagamento-2023-PDA*", re.I)
    patt_2024 = re.compile(r"20250204*", re.I)

    out = {}
    for f in files:
        full = os.path.join(data_raw, f)
        if not os.path.isfile(full):
            continue
        low = f.lower()
        if patt_2022.search(low):
            out["y2022"] = full
        elif patt_2023.search(low):
            out["y2023"] = full
        elif patt_2024.search(low):
            out["y2024"] = full

    required = {"y2022", "y2023", "y2024"}
    missing = required.difference(out.keys())
    if missing:
        raise FileNotFoundError(
            f"Arquivos esperados não encontrados em {data_raw}: {sorted(missing)}"
        )
    return out


# ==========================================
# Seção 2 - Leitura dos CSVs por ano
# ==========================================

def _read_csv_flexible(path: str, header_row: int, sep_override: Optional[str] = None) -> pd.DataFrame:
    """
    Lê CSV com header em 'header_row' (0-based).
    - Se sep_override vier, usa-o diretamente (ex.: ';' no 2024).
    - Caso contrário, tenta sniff (engine='python', sep=None).
    Tenta encodings comuns do BR.
    """
    encodings = ("utf-8", "utf-8-sig", "latin-1", "cp1252")
    errs = []
    for enc in encodings:
        try:
            df = pd.read_csv(
                path,
                header=header_row,
                engine="python",
                sep=(sep_override if sep_override is not None else None),
                dtype=str,
                encoding=enc,
            )
            return df
        except Exception as e:
            errs.append((enc, str(e)))
    raise RuntimeError(f"Falha ao ler {path} (sep={sep_override}) com encodings: {errs}")


def read_2024(path: str) -> pd.DataFrame:
    """
    2024:
      - header na linha 1 (1-based), header_row=0
      - separador ';'
      - dados iniciam na 2
    """
    # Força ';' porque a planilha 2024 é CSV semicolon (pt-BR).
    return _read_csv_flexible(path, header_row=0, sep_override=";")



def read_2022(path: str) -> pd.DataFrame:
    """
    2022:
      - header na linha 6 (1-based), ou seja header_row=5
      - dados iniciam na 7
    """
    return _read_csv_flexible(path, header_row=5)


def read_2023(path: str) -> pd.DataFrame:
    """
    2023:
      - header na linha 8 (1-based), header_row=7
      - dados iniciam na 9
    """
    return _read_csv_flexible(path, header_row=7)


# ==================================================
# Seção 3 - Padronização de nomes de colunas (schema)
# ==================================================

CANONICAL_COLS = [
    "ANO_REFERENCIA",
    "PROCESSO",
    "DATA_INICIO_PROCESSO",
    "DATA_TERMINO_PROCESSO",
    "BENEFICIARIO",
    "CPF_HASH",                  # derivado de NU_CPF (2023) e CPF ANONIMIZADO (2024)
    "LINHA_FOMENTO",
    "MODALIDADE",
    "CATEGORIA_NIVEL",
    "NOME_CHAMADA",
    "PROGRAMA_CNPQ",
    "GRANDE_AREA",
    "AREA",
    "SUBAREA",
    "INSTITUICAO_ORIGEM",
    "SIGLA_UF_ORIGEM",
    "PAIS_ORIGEM",
    "INSTITUICAO_DESTINO",
    "SIGLA_INSTITUICAO_DESTINO",
    "SIGLA_INSTITUICAO_MACRO",
    "CIDADE_DESTINO",
    "SIGLA_UF_DESTINO",
    "REGIAO_DESTINO",
    "PAIS_DESTINO",
    "TITULO_PROJETO",
    "PALAVRA_CHAVE",
    "UO",                        # só 2022
    "NATUREZA_DESPESA",          # só 2022
    "VALOR_PAGO",
]

# Mapeamentos 2022 (origem -> canônico)
MAP_2022 = {
    "Ano Referência": "ANO_REFERENCIA",
    "Processo": "PROCESSO",
    "Data Início Processo": "DATA_INICIO_PROCESSO",
    "Data Término Processo": "DATA_TERMINO_PROCESSO",
    "Beneficiário": "BENEFICIARIO",
    "Linha de Fomento": "LINHA_FOMENTO",
    "Modalidade": "MODALIDADE",
    "Categoria/Nível": "CATEGORIA_NIVEL",
    "Nome Chamada": "NOME_CHAMADA",
    "Programa CNPq": "PROGRAMA_CNPQ",
    "Grande Área": "GRANDE_AREA",
    "Área": "AREA",
    "Subárea": "SUBAREA",
    "Instituição Origem": "INSTITUICAO_ORIGEM",
    "Sigla UF Origem": "SIGLA_UF_ORIGEM",
    "País Origem": "PAIS_ORIGEM",
    "Instituição Destino": "INSTITUICAO_DESTINO",
    "Sigla Instituição Destino": "SIGLA_INSTITUICAO_DESTINO",
    "Sigla Instituição Macro": "SIGLA_INSTITUICAO_MACRO",
    "Cidade Destino": "CIDADE_DESTINO",
    "Sigla UF Destino": "SIGLA_UF_DESTINO",
    "Região Destino": "REGIAO_DESTINO",
    "País Destino": "PAIS_DESTINO",
    "Título do Projeto": "TITULO_PROJETO",
    "Palavra Chave": "PALAVRA_CHAVE",
    "UO": "UO",
    "Natureza de Despesa": "NATUREZA_DESPESA",
    "Valor Pago": "VALOR_PAGO",
}

# Mapeamentos 2023
MAP_2023 = {
    "ANO_REFERENCIA": "ANO_REFERENCIA",
    "PROCESSO": "PROCESSO",
    "DATA_INICIO_PROCESSO": "DATA_INICIO_PROCESSO",
    "DATA_TERMINO_PROCESSO": "DATA_TERMINO_PROCESSO",
    "BENEFICIARIO": "BENEFICIARIO",
    "NU_CPF": "CPF_HASH",  # renomeia
    "LINHA_FOMENTO": "LINHA_FOMENTO",
    "MODALIDADE": "MODALIDADE",
    "CATEGORIA_NIVEL": "CATEGORIA_NIVEL",
    "NOME_CHAMADA": "NOME_CHAMADA",
    "PROGRAMA_CNPQ": "PROGRAMA_CNPQ",
    "GRANDE_AREA": "GRANDE_AREA",
    "AREA": "AREA",
    "SUBAREA": "SUBAREA",
    "INSTITUICAO_ORIGEM": "INSTITUICAO_ORIGEM",
    "SIGLA_UF_ORIGEM": "SIGLA_UF_ORIGEM",
    "PAIS_ORIGEM": "PAIS_ORIGEM",
    "INSTITUICAO_DESTINO": "INSTITUICAO_DESTINO",
    "SIGLA_INSTITUICAO_DESTINO": "SIGLA_INSTITUICAO_DESTINO",
    "SIGLA_INSTITUICAO_MACRO": "SIGLA_INSTITUICAO_MACRO",
    "CIDADE_DESTINO": "CIDADE_DESTINO",
    "SIGLA_UF_DESTINO": "SIGLA_UF_DESTINO",
    "REGIAO": "REGIAO_DESTINO",  # padroniza nome
    "PAIS_DESTINO": "PAIS_DESTINO",
    "TITULO_PROJETO": "TITULO_PROJETO",
    "PALAVRA_CHAVE": "PALAVRA_CHAVE",
    "VALOR_PAGO": "VALOR_PAGO",
}

# Mapeamentos 2024
MAP_2024 = {
    "ANO_REFERENCIA": "ANO_REFERENCIA",
    "PROCESSO": "PROCESSO",
    "DATA_INICIO_PROCESSO": "DATA_INICIO_PROCESSO",
    "DATA_TERMINO_PROCESSO": "DATA_TERMINO_PROCESSO",
    "BENEFICIARIO": "BENEFICIARIO",
    "CPF ANONIMIZADO": "CPF_HASH",  # renomeia
    "LINHA_FOMENTO": "LINHA_FOMENTO",
    "MODALIDADE": "MODALIDADE",
    "CATEGORIA_NIVEL": "CATEGORIA_NIVEL",
    "NOME_CHAMADA": "NOME_CHAMADA",
    "PROGRAMA_CNPQ": "PROGRAMA_CNPQ",
    "GRANDE_AREA": "GRANDE_AREA",
    "AREA": "AREA",
    "SUBAREA": "SUBAREA",
    "INSTITUICAO_ORIGEM": "INSTITUICAO_ORIGEM",
    "SIGLA_UF_ORIGEM": "SIGLA_UF_ORIGEM",
    "PAIS_ORIGEM": "PAIS_ORIGEM",
    "INSTITUICAO_DESTINO": "INSTITUICAO_DESTINO",
    "SIGLA_INSTITUICAO_DESTINO": "SIGLA_INSTITUICAO_DESTINO",
    "SIGLA_INSTITUICAO_MACRO": "SIGLA_INSTITUICAO_MACRO",
    "CIDADE_DESTINO": "CIDADE_DESTINO",
    "SIGLA_UF_DESTINO": "SIGLA_UF_DESTINO",
    "REGIAO": "REGIAO_DESTINO",
    "PAIS_DESTINO": "PAIS_DESTINO",
    "TITULO_PROJETO": "TITULO_PROJETO",
    "PALAVRA_CHAVE": "PALAVRA_CHAVE",
    "VALOR_PAGO": "VALOR_PAGO",
}


def normalize_columns(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    """
    Renomeia colunas de df conforme mapping, remove espaços extras e
    retorna apenas colunas mapeadas, sem perder a ordem canônica.
    """
    # Primeiro, strip em colunas
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    # Aplica mapping específico do ano
    df = df.rename(columns=mapping)

    # Mantém apenas as colunas canônicas existentes
    keep = [c for c in CANONICAL_COLS if c in df.columns]
    df = df[keep]

    # Garante que TODAS as CANONICAL_COLS existam (mesmo se vazias)
    for col in CANONICAL_COLS:
        if col not in df.columns:
            df[col] = pd.NA

    # Reordena
    return df[CANONICAL_COLS]


# =================================================
# Seção 4 - Coerção de tipos e normalizações leves
# =================================================

DATE_COLS = ["DATA_INICIO_PROCESSO", "DATA_TERMINO_PROCESSO"]
STRIP_UPPER_COLS = [
    "ANO_REFERENCIA",
    "PROCESSO",
    "BENEFICIARIO",
    "CPF_HASH",
    "LINHA_FOMENTO",
    "MODALIDADE",
    "CATEGORIA_NIVEL",
    "NOME_CHAMADA",
    "PROGRAMA_CNPQ",
    "GRANDE_AREA",
    "AREA",
    "SUBAREA",
    "INSTITUICAO_ORIGEM",
    "SIGLA_UF_ORIGEM",
    "PAIS_ORIGEM",
    "INSTITUICAO_DESTINO",
    "SIGLA_INSTITUICAO_DESTINO",
    "SIGLA_INSTITUICAO_MACRO",
    "CIDADE_DESTINO",
    "SIGLA_UF_DESTINO",
    "REGIAO_DESTINO",
    "PAIS_DESTINO",
    "TITULO_PROJETO",
    "PALAVRA_CHAVE",
    "UO",
    "NATUREZA_DESPESA",
]

def parse_brazilian_currency_to_float(s: pd.Series) -> pd.Series:
    """
    Converte strings de moeda BR -> float.
    Robusto para "R$ 1.234,56", " $ 8,100.00 ", "600,00", "300.0".
    Remove R$, $, espaços, e aspas.
    """
    if pd.api.types.is_numeric_dtype(s):
        return s.astype(float)
    
    s_str = s.astype("string").str.strip()
    
    # Remove aspas que podem envolver o valor (ex: "R$ 400,00")
    s_str = s_str.str.strip('"')
    
    # Remove símbolos de moeda (R$ ou $) e espaços em branco
    s_str = s_str.str.replace(r"R\$|\$", "", regex=True).str.strip()
    
    # Se ainda tivermos '.' e ',', a vírgula é o decimal (formato BR)
    # Ex: "1.234,56"
    is_br_format = s_str.str.contains(r"\.", regex=False) & s_str.str.contains(r",", regex=False)
    
    # Se tiver só vírgula, ela é o decimal (formato BR simplificado)
    # Ex: "600,00"
    is_simple_br = s_str.str.contains(r",", regex=False) & ~s_str.str.contains(r"\.", regex=False)

    # Aplica a limpeza
    
    # Formato BRL (1.234,56 -> 1234.56)
    s_br = s_str.where(is_br_format | is_simple_br)
    s_br = s_br.str.replace(r"\.", "", regex=False).str.replace(r",", ".", regex=False)

    # O que sobrar (formato "300.0" ou "1500")
    s_other = s_str.where(~(is_br_format | is_simple_br))
    # Apenas remove vírgulas de milhar (se houver, ex: 1,500)
    s_other = s_other.str.replace(r",", "", regex=False)

    # Combina
    s_final = s_br.fillna(s_other)

    return pd.to_numeric(s_final, errors="coerce")


@log_call
def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    lg = get_logger()
    df = df.copy()

    # Datas (robusto para formatos com ou sem hora)
    for c in DATE_COLS:
        if c in df.columns:
            # Tenta parsear, extraindo apenas a data (ignora a hora)
            # O format='mixed' e dayfirst=True ajudam a pegar
            # "dd/mm/yyyy" e "dd/mm/yyyy HH:MM:SS"
            df[c] = pd.to_datetime(
                df[c], 
                dayfirst=True, 
                errors="coerce",
                format='mixed' 
            )

    # Strings (strip/upper onde convém)
    for c in STRIP_UPPER_COLS:
        if c in df.columns:
            df[c] = df[c].astype("string").str.strip()
            if c in ("TITULO_PROJETO", "PALAVRA_CHAVE", "BENEFICIARIO"):
                continue
            df[c] = df[c].str.upper()

    # Valor pago (agora usando a nova função robusta)
    if "VALOR_PAGO" in df.columns:
        before_nonnull = df["VALOR_PAGO"].notna().sum()
        df["VALOR_PAGO"] = parse_brazilian_currency_to_float(df["VALOR_PAGO"])
        after_nonnull = df["VALOR_PAGO"].notna().sum()
        lg.info("valor_pago_parse | nonnull_before=%d | nonnull_after=%d", before_nonnull, after_nonnull)
    else:
        lg.warning("col_missing | col=VALOR_PAGO")


    # UF normalização
    for c in ("SIGLA_UF_ORIGEM", "SIGLA_UF_DESTINO"):
        if c in df.columns:
            df[c] = df[c].astype("string").str.upper().str.replace(r"[^A-Z]", "", regex=True).str[:3]

    # Ano referência — extrai 4 dígitos de qualquer coisa (robusto pra 2024)
    if "ANO_REFERENCIA" in df.columns:
        raw_year = df["ANO_REFERENCIA"].astype("string")
        extracted = raw_year.str.extract(r"(\d{4})", expand=False)
        df["ANO_REFERENCIA"] = pd.to_numeric(extracted, errors="coerce").astype("Int64")
        lg.info(
            "ano_ref_parse | unique_years=%s",
            sorted(df["ANO_REFERENCIA"].dropna().astype(int).unique().tolist())
            if df["ANO_REFERENCIA"].notna().any() else []
        )
    else:
         lg.warning("col_missing | col=ANO_REFERENCIA")

    return df


# ==========================================
# Seção 5 - Harmonização, união e salvamento
# ==========================================

def load_and_standardize_all(paths: Dict[str, str]) -> Dict[str, pd.DataFrame]:
    """
    Para cada ano:
      1) tenta carregar o Parquet anual do cache;
      2) se não existir, lê CSV bruto, normaliza, coage tipos e salva o Parquet anual;
    Retorna dict {"2022": df22, "2023": df23, "2024": df24}.
    """
    found = discover_raw_files(paths["data_raw"])

    out: Dict[str, pd.DataFrame] = {}

    # 2022
    df22_cached = load_year_parquet_if_exists(2022, paths)
    if df22_cached is not None:
        out["2022"] = df22_cached
    else:
        df22 = read_2022(found["y2022"])
        df22 = normalize_columns(df22, MAP_2022)
        df22 = coerce_types(df22)
        save_year_parquet(df22, 2022, paths)
        out["2022"] = df22

    # 2023
    df23_cached = load_year_parquet_if_exists(2023, paths)
    if df23_cached is not None:
        out["2023"] = df23_cached
    else:
        df23 = read_2023(found["y2023"])
        df23 = normalize_columns(df23, MAP_2023)
        df23 = coerce_types(df23)
        save_year_parquet(df23, 2023, paths)
        out["2023"] = df23

    # 2024
    df24_cached = load_year_parquet_if_exists(2024, paths)
    if df24_cached is not None:
        out["2024"] = df24_cached
    else:
        df24 = read_2024(found["y2024"])
        df24 = normalize_columns(df24, MAP_2024)
        df24 = coerce_types(df24)
        save_year_parquet(df24, 2024, paths)
        out["2024"] = df24

    return out


def _common_cols_across(dfs: Dict[str, pd.DataFrame]) -> List[str]:
    """
    Retorna a interseção das colunas presentes em TODOS os dataframes,
    garantindo ANO_REFERENCIA como primeira coluna no retorno final.
    """
    sets = [set(df.columns) for df in dfs.values()]
    common = set.intersection(*sets) if sets else set()
    # Ordem amigável: ANO_REFERENCIA primeiro, depois ordem alfabética das demais
    ordered = ["ANO_REFERENCIA"] + sorted([c for c in common if c != "ANO_REFERENCIA"])
    return ordered

@log_call
def unify_pagamentos(dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Concatena os três dfs padronizados (2022, 2023, 2024).
    Assume que normalize_columns() já alinhou todos ao CANONICAL_COLS.
    Remove linhas totalmente vazias em colunas-chave e ordena por ano/processo.
    """
    lg = get_logger()
    lg.info("unify_start | dfs_keys=%s", list(dfs.keys()))

    # A função normalize_columns() (linha 303) já garante que
    # todos os DFs têm exatamente as colunas de CANONICAL_COLS
    # (com NA onde o dado não existia).
    # Não precisamos de "interseção", apenas concatenar.
    
    # Garante a ordem das colunas para a concatenação
    aligned_dfs = [
        dfs["2022"][CANONICAL_COLS],
        dfs["2023"][CANONICAL_COLS],
        dfs["2024"][CANONICAL_COLS]
    ]
    
    out = pd.concat(aligned_dfs, axis=0, ignore_index=True)
    lg.info("unify_concatenated | shape=%s | cols=%s", out.shape, out.columns.tolist())

    # 3) Limpa linhas sem info chave
    key_cols = ["ANO_REFERENCIA", "PROCESSO", "VALOR_PAGO"]
    out = out.dropna(axis=0, how="all", subset=key_cols)
    lg.info("unify_dropna | shape=%s", out.shape)

    # 4) Ordena
    sort_cols = ["ANO_REFERENCIA", "PROCESSO"]
    out = out.sort_values(by=sort_cols, na_position="last", kind="mergesort").reset_index(drop=True)
    lg.info("unify_sorted | shape=%s", out.shape)

    return out


@log_call
def save_preprocessed(
    df: pd.DataFrame, 
    paths: Dict[str, str], 
    write_csv: bool | None = None
) -> Dict[str, str]:
    """
    Salva o dataset unificado. Por padrão salva somente Parquet.
    CSV só é escrito se:
      - write_csv=True, OU
      - variável de ambiente WRITE_CSV == "1".
    """
    ensure_dir(paths["data_preprocessed"])
    base = os.path.join(paths["data_preprocessed"], "cnpq_pagamentos_2022_2024")

    # Garante ANO_REFERENCIA primeiro
    cols = df.columns.tolist()
    if "ANO_REFERENCIA" in cols:
        cols = ["ANO_REFERENCIA"] + [c for c in cols if c != "ANO_REFERENCIA"]
        df = df[cols]

    out_parquet = f"{base}.parquet"
    df.to_parquet(out_parquet, index=False)

    should_write_csv = (
        (write_csv is True) or
        (write_csv is None and os.getenv("WRITE_CSV", "0") == "1")
    )
    out_csv = ""
    if should_write_csv:
        out_csv = f"{base}.csv"
        df.to_csv(out_csv, index=False, encoding="utf-8")

    get_logger().info("save_preprocessed | parquet=%s | csv=%s | shape=%s", out_parquet, out_csv, df.shape)
    return {"parquet": out_parquet, "csv": out_csv}




# =====================================================
# Seção 6 - Função orquestradora (para o fluxo principal)
# =====================================================

def build_cnpq_pagamentos_preprocessed() -> Tuple[pd.DataFrame, Dict[str, str]]:
    """
    Fluxo completo:
      - carrega config e paths
      - lê e padroniza 2022/2023/2024
      - unifica
      - salva em data/preprocessed
      - retorna df e caminhos
    """
    paths, _ = load_config()
    raw_dfs = load_and_standardize_all(paths)
    merged = unify_pagamentos(raw_dfs)
    outputs = save_preprocessed(merged, paths)
    return merged, outputs

# ==========================================
# Seção 7 - I/O para o Streamlit e diagnósticos
# ==========================================

@log_call
def load_preprocessed_dataset(paths: Optional[Dict[str, str]] = None) -> pd.DataFrame:
    if paths is None:
        paths, _ = load_config()
    base = Path(paths["data_preprocessed"]) / "cnpq_pagamentos_2022_2024"
    pq_path = base.with_suffix(".parquet")
    csv_path = base.with_suffix(".csv")  # opcional

    lg = get_logger()
    lg.info("load_preprocessed | pq=%s | csv=%s", pq_path, csv_path)

    if pq_path.exists():
        lg.info("Loading from Parquet: %s", pq_path)
        df = pd.read_parquet(pq_path)
        lg.info("loaded_df | shape=%s | cols=%s", df.shape, list(df.columns))
        return df

    if csv_path.exists():
        lg.info("Loading from CSV (fallback): %s", csv_path)
        df = pd.read_csv(
            csv_path,
            dtype={
                "ANO_REFERENCIA": "Int64",
                "PROCESSO": "string",
                "CPF_HASH": "string",
            }
        )
        lg.info("loaded_df | shape=%s | cols=%s", df.shape, list(df.columns))
        return df

    lg.error("file_not_found | pq=%s | csv=%s", pq_path, csv_path)
    raise FileNotFoundError(
        "Dataset pré-processado não encontrado. "
        "Rode a pipeline: python -m src.build_dataset"
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

    # verificar UFs
    if ("SIGLA_UF_DESTINO" not in df.columns) and ("SIGLA_UF_ORIGEM" not in df.columns):
        notes.append("Colunas de UF ausentes (SIGLA_UF_DESTINO / SIGLA_UF_ORIGEM).")

    return notes

# =========================================================
# Seção 8 - Métricas/Agregações para os gráficos do Streamlit
# =========================================================

@log_call
def list_available_years(df: pd.DataFrame) -> List[int]:
    if "ANO_REFERENCIA" not in df.columns:
        return []
    anos = pd.to_numeric(df["ANO_REFERENCIA"], errors="coerce").dropna().astype(int).unique()
    years = sorted(anos.tolist())
    get_logger().info("years_available | %s", years)
    return years


def _choose_uf_column(df: pd.DataFrame, preference: Optional[str] = None) -> str:
    """
    Decide qual coluna de UF usar.
    preference: 'DESTINO' | 'ORIGEM' | None(AUTO).
    AUTO: prefere DESTINO se existir e tiver mais não-nulos; senão ORIGEM.
    """
    lg = get_logger()
    dest_exists = "SIGLA_UF_DESTINO" in df.columns
    orig_exists = "SIGLA_UF_ORIGEM" in df.columns

    if preference == "DESTINO" and dest_exists:
        lg.info("uf_choice | forced=DESTINO")
        return "SIGLA_UF_DESTINO"
    if preference == "ORIGEM" and orig_exists:
        lg.info("uf_choice | forced=ORIGEM")
        return "SIGLA_UF_ORIGEM"

    # AUTO
    if dest_exists and orig_exists:
        nd = df["SIGLA_UF_DESTINO"].notna().sum()
        no = df["SIGLA_UF_ORIGEM"].notna().sum()
        chosen = "SIGLA_UF_DESTINO" if nd >= no else "SIGLA_UF_ORIGEM"
        lg.info("uf_choice | auto | ndest=%d | norig=%d | chosen=%s", nd, no, chosen)
        return chosen
    if dest_exists:
        lg.info("uf_choice | only_destino")
        return "SIGLA_UF_DESTINO"
    if orig_exists:
        lg.info("uf_choice | only_origem")
        return "SIGLA_UF_ORIGEM"
    raise ValueError("Nenhuma coluna de UF encontrada no dataset.")


@log_call
def mean_valor_pago_by_uf_for_year(df: pd.DataFrame, year: int, uf_preference: Optional[str] = None) -> pd.DataFrame:
    """
    Média de VALOR_PAGO por UF no ano escolhido.
    uf_preference: None(AUTO) | 'DESTINO' | 'ORIGEM'
    Retorna ['UF', 'media_valor_pago', 'n'].
    """
    required = ["ANO_REFERENCIA", "VALOR_PAGO"]
    for c in required:
        if c not in df.columns:
            raise ValueError(f"Coluna obrigatória ausente: {c}")

    df = df.copy()

    # dtypes
    if not is_numeric_dtype(df["VALOR_PAGO"]):
        df["VALOR_PAGO"] = pd.to_numeric(df["VALOR_PAGO"], errors="coerce")
    df["ANO_REFERENCIA"] = pd.to_numeric(df["ANO_REFERENCIA"], errors="coerce").astype("Int64")

    # qual UF
    uf_col = _choose_uf_column(df, preference=uf_preference)

    # normalização UF (somente A-Z até 3 chars)
    df[uf_col] = df[uf_col].astype("string").str.upper().str.replace(r"[^A-Z]", "", regex=True).str[:3]

    # filtra ano
    df_year = df[df["ANO_REFERENCIA"] == year]
    get_logger().info("filter_year | year=%s | rows=%d", year, len(df_year))

    # agrega
    out = (
        df_year.dropna(subset=[uf_col])
               .groupby(uf_col, as_index=False)
               .agg(media_valor_pago=("VALOR_PAGO", "mean"),
                    n=("VALOR_PAGO", "size"))
               .rename(columns={uf_col: "UF"})
    )

    # tipagem para plot
    out["media_valor_pago"] = out["media_valor_pago"].astype(float)
    out["UF"] = out["UF"].astype(str)

    # ordena desc
    out = out.sort_values("media_valor_pago", ascending=False, kind="mergesort").reset_index(drop=True)
    get_logger().info("agg_done | groups=%d | top5=%s", len(out), out.head(5).to_dict("records"))

    return out


# =============================================
# Seção 9 - Gráficos (Plotly) para o Streamlit
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
