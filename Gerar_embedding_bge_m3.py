import os
import duckdb
import pandas as pd
import torch
from tqdm import tqdm
from sentence_transformers import SentenceTransformer

# CONFIGURAÇÃO DE DIRETÓRIO E MODELO
DIRETORIO_SCRIPT = os.path.dirname(os.path.abspath(__file__))
CAMINHO_BANCO = os.path.join(DIRETORIO_SCRIPT, "bds", "TCCa.duckdb")

NOME_MODELO = "BAAI/bge-m3"
NOME_COLUNA = "vetor_embedding_bge_m3"
DIMENSAO_VETOR = 1024

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

# CARREGAMENTO DO MODELO
def carregar_modelo():
    print(f"[INFO] Carregando modelo '{NOME_MODELO}' no dispositivo '{DEVICE}'...")
    modelo = SentenceTransformer(NOME_MODELO, device=DEVICE)
    print(f"[INFO] Modelo '{NOME_MODELO}' pronto para inferência.")
    return modelo

# FUNÇÃO DE VETORIZAÇÃO EM LOTE
def obter_embeddings_lote(modelo, lista_textos):
    """
    Gera embeddings densos (1024 dimensões) normalizados usando BGE-M3.
    """
    vetores = modelo.encode(
        lista_textos,
        show_progress_bar=False,
        normalize_embeddings=True,
        convert_to_numpy=True
    )
    return vetores.tolist()

# EXECUÇÃO E PERSISTÊNCIA NO DUCKDB
def processar_embeddings(tamanho_lote=32):
    if not os.path.exists(CAMINHO_BANCO):
        raise FileNotFoundError(f"Banco não encontrado em: {CAMINHO_BANCO}")

    print(f"\n[INFO] Conectando ao DuckDB em: {CAMINHO_BANCO}")
    conn = duckdb.connect(CAMINHO_BANCO)

    # Garante que a coluna específica do modelo exista no schema
    colunas = [c[0] for c in conn.execute("DESCRIBE main.chunks_contratos").fetchall()]
    if NOME_COLUNA not in colunas:
        print(f"[INFO] Criando coluna {NOME_COLUNA} FLOAT[{DIMENSAO_VETOR}] na tabela main.chunks_contratos...")
        conn.execute(f"ALTER TABLE main.chunks_contratos ADD COLUMN {NOME_COLUNA} FLOAT[{DIMENSAO_VETOR}];")

    # Busca apenas registros pendentes de vetorização
    query_pendentes = f"""
        SELECT chunk_id, texto_chunk_denso
        FROM main.chunks_contratos
        WHERE {NOME_COLUNA} IS NULL
          AND texto_chunk_denso IS NOT NULL
        ORDER BY contrato_id, chunk_index
    """
    df_pendentes = conn.execute(query_pendentes).df()
    total_pendentes = len(df_pendentes)
    print(f"[INFO] Chunks pendentes de vetorização ({NOME_MODELO}): {total_pendentes}")

    if total_pendentes == 0:
        print(f"[SUCESSO] Todos os chunks já possuem embeddings salvos na coluna '{NOME_COLUNA}'!")
        conn.close()
        return

    # Carrega o modelo de embeddings
    modelo = carregar_modelo()

    # Processamento em Batches com persistência incremental
    for i in tqdm(range(0, total_pendentes, tamanho_lote), desc=f"Gerando Embeddings ({NOME_MODELO})"):
        lote_df = df_pendentes.iloc[i : i + tamanho_lote].copy()
        textos = lote_df['texto_chunk_denso'].tolist()
        ids = lote_df['chunk_id'].tolist()

        vetores = obter_embeddings_lote(modelo, textos)

        df_lote_atualizar = pd.DataFrame({
            'chunk_id': ids,
            NOME_COLUNA: vetores
        })

        conn.register("lote_temp", df_lote_atualizar)
        conn.execute(f"""
            UPDATE main.chunks_contratos AS c
            SET {NOME_COLUNA} = t.{NOME_COLUNA}
            FROM lote_temp AS t
            WHERE c.chunk_id = t.chunk_id;
        """)

    conn.close()
    print(f"\n[SUCESSO] Todos os embeddings de '{NOME_MODELO}' foram salvos com sucesso no DuckDB!")

if __name__ == "__main__":
    processar_embeddings(tamanho_lote=32)
