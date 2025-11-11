# Trabalho 2 PVD - CNPq Pagamentos - Guia de Execução e Referência Técnica

## Sumário

* [1. Visão geral](#1-visão-geral)
* [2. Arquivos de dados](#2-arquivos-de-dados)
* [3. Preparação do ambiente](#3-preparação-do-ambiente)

  * [3.1 Windows](#31-windows)
  * [3.2 macOS e Linux](#32-macos-e-linux)
* [4. Configuração](#4-configuração)
* [5. Construção do dataset unificado](#5-construção-do-dataset-unificado)
* [6. Execução do dashboard Streamlit](#6-execução-do-dashboard-streamlit)
* [7. Estrutura de pastas recomendada](#7-estrutura-de-pastas-recomendada)
* [8. Referência dos módulos](#8-referência-dos-módulos)

  * [8.1 utils.py](#81-utilspy)
  * [8.2 build_dataset.py](#82-build_datasetpy)
  * [8.3 Aplicativo Streamlit](#83-aplicativo-streamlit)
* [9. Logging e diagnóstico](#9-logging-e-diagnóstico)
* [10. Boas práticas de reprodutibilidade](#10-boas-práticas-de-reprodutibilidade)
* [11. Solução de problemas frequentes](#11-solução-de-problemas-frequentes)

---

## 1. Visão geral

O projeto integra e padroniza três bases anuais de pagamentos do CNPq, harmoniza o esquema de colunas e converte valores monetários para `float`, gerando um dataset unificado para análise. O dashboard em Streamlit permite selecionar um ano e visualizar a média de valor pago por UF.

Saída principal:

* `data/preprocessed/cnpq_pagamentos_2022_2024.csv`
* `data/preprocessed/cnpq_pagamentos_2022_2024.parquet` quando possível

---

## 2. Arquivos de dados

Coloque os CSVs originais em `data/raw/` com os nomes abaixo.

* 2022: `Relatorio_de_dados_abertos_CNPq_Jan-Dez2022 (atualizado em 10-04-2023).csv`
* 2023: `Dados-de-Pagamento-2023-PDA-CSV-Beneficiarios-com-18-ou-menos-de-idade-ocultados.csv`
* 2024: `20250204 Planilha Dados de Pagamento jan-dez_2024 - PDA CSV.csv`

---

## 3. Preparação do ambiente

Requisito mínimo recomendado: Python 3.10 ou superior.

Dependências principais: `pandas`, `numpy`, `pyarrow` opcional para Parquet, `plotly`, `streamlit`, `pyyaml`.

Se houver um `requirements.txt`, use-o. Caso não haja, utilize a instalação direta indicada abaixo.

### 3.1 Windows

PowerShell:

```powershell
# 1) Criar e ativar venv
python -m venv .venv
. .\.venv\Scripts\Activate.ps1

# 2) Atualizar pip
python -m pip install --upgrade pip

# 3) Instalar dependências
pip install pandas numpy plotly streamlit pyyaml pyarrow
```

Se preferir `requirements.txt`:

```powershell
pip install -r requirements.txt
```

### 3.2 macOS e Linux

Terminal bash:

```bash
# 1) Criar e ativar venv
python3 -m venv .venv
source .venv/bin/activate

# 2) Atualizar pip
python -m pip install --upgrade pip

# 3) Instalar dependências
pip install pandas numpy plotly streamlit pyyaml pyarrow
```

Com `requirements.txt`:

```bash
pip install -r requirements.txt
```

Sugestão de `requirements.txt` mínimo:

```text
pandas>=2.2
numpy>=1.26
plotly>=5.22
streamlit>=1.38
pyyaml>=6.0
pyarrow>=16.0
```

---

## 4. Configuração

O arquivo `config.yaml` na raiz deve conter ao menos os caminhos relativos usados por `utils.load_config()`. Exemplo mínimo:

```yaml
paths:
  data_raw: "../data/raw"
  data_processed: "../data/processed"
  images: "../images"
  report: "../report"
  addons: "../addons"
```

O utilitário cria automaticamente `data/preprocessed/` sob a raiz do projeto.

---

## 5. Construção do dataset unificado

Execute a pipeline para ler, normalizar e unificar as bases, salvando em `data/preprocessed/`.

```bash
# No diretório raiz do projeto
python -m src.build_dataset
```

Saída esperada no console:

* Caminhos dos arquivos salvos
* Shape do DataFrame final
* Primeiras linhas do dataset

Arquivos gerados:

* `data/preprocessed/cnpq_pagamentos_2022_2024.csv`
* `data/preprocessed/cnpq_pagamentos_2022_2024.parquet` quando o `pyarrow` estiver disponível

---

## 6. Execução do dashboard Streamlit

Após gerar o dataset unificado:

```bash
streamlit run src/build_dataset.py
```

O app exibe:

* Seletor de ano
* Gráfico de barras com média de valor pago por UF
* Tabela agregada opcional
* Amostra do dataset bruto opcional
* Seção de debug com contagens por ano, dtypes e caminho do log

Observação importante: o mesmo arquivo `build_dataset.py` contém a pipeline e o app Streamlit. O Streamlit apenas utiliza as funções do módulo `utils` para carregar e agregar os dados.

---

## 7. Estrutura de pastas recomendada

```
.
├── config.yaml
├── data
│   ├── raw
│   │   ├── Relatorio_de_dados_abertos_CNPq_Jan-Dez2022 (atualizado em 10-04-2023).csv
│   │   ├── Dados-de-Pagamento-2023-PDA-CSV-Beneficiarios-com-18-ou-menos-de-idade-ocultados.csv
│   │   └── 20250204 Planilha Dados de Pagamento jan-dez_2024 - PDA CSV.csv
│   ├── processed
│   └── preprocessed
│       ├── cnpq_pagamentos_2022_2024.csv
│       └── cnpq_pagamentos_2022_2024.parquet
├── logs
│   └── app.log
└── src
    ├── build_dataset.py
    └── utils.py
```

---

## 8. Referência dos módulos

### 8.1 utils.py

Funções principais por seção.

1. Config e paths

* `load_config()`: lê `config.yaml`, resolve caminhos e garante `data/preprocessed/`.
* `ensure_dir()`: criação idempotente de diretórios.

2. Logging

* `get_logger()`: logger rotativo em `logs/app.log`.
* `log_call`: decorador para logar chamadas, formas e colunas dos DataFrames, além de exceções.

3. Descoberta de arquivos

* `discover_raw_files(data_raw)`: identifica os três arquivos brutos esperados em `data/raw/` via padrões robustos.

4. Leitura dos CSVs por ano

* `_read_csv_flexible(path, header_row, sep_override=None)`: leitura tolerante a encodings e separadores.
* `read_2022`, `read_2023`, `read_2024`: wrappers com linhas de cabeçalho corretas; 2024 força `sep=';'`.

5. Padronização de colunas

* `CANONICAL_COLS`: esquema canônico comum.
* `MAP_2022`, `MAP_2023`, `MAP_2024`: mapeiam nomes originais para o canônico.
* `normalize_columns(df, mapping)`: renomeia, mantém apenas colunas canônicas e reordena.

6. Coerção de tipos e normalizações

* `DATE_COLS`, `STRIP_UPPER_COLS`: listas de colunas para tratamento.
* `parse_brazilian_currency_to_float(s)`: parser robusto para BR e US, normalizando para `float`.
* `coerce_types(df)`: aplica parse de datas, normalização de strings, moeda, UF e extração segura do ano.

7. Harmonização, união e salvamento

* `load_and_standardize_all(paths)`: pipeline por ano, da leitura à coerção.
* `unify_pagamentos(dfs)`: concatena 2022, 2023 e 2024 já alinhados ao esquema canônico; remove linhas vazias nas chaves e ordena por ano e processo.
* `save_preprocessed(df, paths)`: salva CSV e, se possível, Parquet; força `ANO_REFERENCIA` como primeira coluna.

8. I/O para o Streamlit e diagnósticos

* `load_preprocessed_dataset(paths)`: carrega o dataset unificado, preferindo Parquet.
* `get_dataset_notes(df)`: retorna avisos sobre colunas e dados faltantes.
* `list_available_years(df)`: anos distintos disponíveis.
* `_choose_uf_column(df, preference)`: escolhe entre `SIGLA_UF_DESTINO` e `SIGLA_UF_ORIGEM` por cobertura.
* `mean_valor_pago_by_uf_for_year(df, year, uf_preference)`: agrega a média por UF no ano selecionado.
* `fig_bar_mean_by_uf(df_agg, year)`: figura de barras com rótulos.

### 8.2 build_dataset.py

* Executado como script: roda a pipeline completa `build_cnpq_pagamentos_preprocessed()`, imprime caminhos e shape, e registra no log.
* Executado via Streamlit: usa as funções de `utils` para carregar o dataset unificado, selecionar o ano, agregar por UF e exibir gráfico e tabela.

### 8.3 Aplicativo Streamlit

Controles e saídas:

* Sidebar: seleção da coluna de UF (AUTO, DESTINO, ORIGEM), exibição opcional de tabela e bruto, bloco de debug.
* Corpo: seletor de ano com default no máximo disponível, gráfico de barras por UF, tabela agregada opcional, amostra do bruto opcional, e seção de debug com contagens e dtypes.

---

## 9. Logging e diagnóstico

* Local do log: `logs/app.log`.
* O decorador `@log_call` registra início, fim, shape e colunas das funções marcadas.
* Mensagens úteis:

  * `logger_initialized` com o caminho do arquivo de log.
  * `ano_ref_parse` lista de anos reconhecidos após a extração de dígitos.
  * `valor_pago_parse` contagem de não nulos antes e depois da conversão monetária.
  * `uf_choice` indicando a escolha da coluna de UF.

Para verificar rapidamente:

```bash
# macOS e Linux
tail -n 100 logs/app.log

# Windows PowerShell
Get-Content logs/app.log -Tail 100
```

---

## 10. Boas práticas de reprodutibilidade

* Fixar a versão do Python e congelar dependências com `pip freeze > requirements-lock.txt`.
* Manter os nomes originais dos CSVs em `data/raw`.
* Versionar apenas scripts e metadados; não versionar dados brutos sensíveis.

---

## 11. Solução de problemas frequentes

1. O Streamlit não mostra dados para 2024

* Verifique se o dataset unificado contém 2024:

  ```python
  import pandas as pd
  df = pd.read_csv("data/preprocessed/cnpq_pagamentos_2022_2024.csv")
  print(df["ANO_REFERENCIA"].dropna().unique())
  ```
* Consulte `logs/app.log` por entradas `ano_ref_parse` durante a construção.

2. Todos os valores de `VALOR_PAGO` aparecem vazios

* Confirme no log a mensagem `valor_pago_parse`.
* Verifique se os campos vieram com aspas ou símbolos e se foram removidos.
* Recrie o dataset após instalar `pyarrow` para evitar problemas de tipo ao reler Parquet.

3. Erro de leitura do 2024

* Confirme a presença de `;` como separador. O leitor do 2024 já força `sep=';'`.
* Verifique encoding e cabeçalhos no arquivo original.

4. Anos não aparecem no seletor

* O app chama `list_available_years`. Se a coluna não estiver no tipo correto, revise o `coerce_types` e a extração de 4 dígitos do ano.
* Cheque `logs/app.log` para `ano_ref_parse`.

5. Permissões de execução em macOS e Linux

* Caso o comando `streamlit` não esteja no PATH, ative o venv e chame `python -m streamlit run src/build_dataset.py`.

