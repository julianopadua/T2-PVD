# src/build_dataset.py
from utils import build_cnpq_pagamentos_preprocessed, get_logger

if __name__ == "__main__":
    lg = get_logger()
    df, outs = build_cnpq_pagamentos_preprocessed()
    print("Arquivos salvos:", outs)
    print("Shape final:", df.shape)
    # Mostra primeiras 3 linhas e colunas
    print(df.head(3).to_string())
    lg.info("build_done | outputs=%s | shape=%s", outs, df.shape)
