import os
import time
import duckdb
import pandas as pd
from openai import AzureOpenAI
from tqdm import tqdm
from dotenv import load_dotenv

# CONFIGURAÇÃO DE DIRETÓRIO E .ENV
DIRETORIO_SCRIPT = os.path.dirname(os.path.abspath(__file__))
CAMINHO_ENV = os.path.join(DIRETORIO_SCRIPT, ".env")

# Carrega o .env explicitamente do diretório do script
load_dotenv(CAMINHO_ENV)

# Busca as variáveis 
API_KEY = os.getenv("AZURE_OPENAI_KEY") or os.getenv("AZURE_OPENAI_API_KEY")
ENDPOINT = os.getenv("ENDPOINT_EMBEDDING_SMALL") or os.getenv("AZURE_OPENAI_ENDPOINT")
API_VERSION = os.getenv("OPENAI_API_VERSION", "2024-02-01")
DEPLOYMENT_EMBEDDING = os.getenv("DEPLOYMENT_EMBEDDING", "text-embedding-3-small")

# Diagnóstico explícito para evitar erro genérico do SDK
if not API_KEY or not ENDPOINT:
    print("\n" + "="*60)
    print("[ERRO CRÍTICO] Credenciais não encontradas!")
    print(f"Diretório esperado do .env: {CAMINHO_ENV}")
    print(f"O arquivo .env existe? {os.path.exists(CAMINHO_ENV)}")
    print(f"AZURE_OPENAI_KEY carregada? {'SIM' if API_KEY else 'NÃO'}")
    print(f"ENDPOINT carregado? {'SIM' if ENDPOINT else 'NÃO'}")
    print("="*60 + "\n")
    raise ValueError("Preencha as variáveis no arquivo .env ou defina-as diretamente no script.")

# 
# CONFIGURAÇÃO DO BANCO DUCKDB
CAMINHO_BANCO = os.path.join(DIRETORIO_SCRIPT, "bds", "TCCa.duckdb")

if not os.path.exists(CAMINHO_BANCO):
    raise FileNotFoundError(f"Banco não encontrado em: {CAMINHO_BANCO}")

print(f"[INFO] Conectando ao DuckDB em: {CAMINHO_BANCO}")
print(f"[INFO] Usando Endpoint Azure: {ENDPOINT}")
print(f"[INFO] Usando Deployment: {DEPLOYMENT_EMBEDDING}")

# CLIENTE AZURE OPENAI
client = AzureOpenAI(
    api_key=API_KEY.strip(),
    api_version=API_VERSION.strip(),
    azure_endpoint=ENDPOINT.strip()
)

# FUNÇÃO DE VETORIZAÇÃO EM LOTE
def obter_embeddings_lote(lista_textos, tentativas=3):
    """Envia um lote de textos para a Azure OpenAI com retry defensivo."""
    for tentativa in range(tentativas):
        try:
            resposta = client.embeddings.create(
                input=lista_textos,
                model=DEPLOYMENT_EMBEDDING
            )
            return [item.embedding for item in resposta.data]
        except Exception as e:
            print(f"\n[AVISO] Erro na requisição (Tentativa {tentativa+1}/{tentativas}): {e}")
            time.sleep(2 ** (tentativa + 1))
    raise RuntimeError("Falha persistente na chamada da API da Azure.")

# EXECUÇÃO E PERSISTÊNCIA NO DUCKDB
def processar_embeddings(tamanho_lote=128):
    conn = duckdb.connect(CAMINHO_BANCO)

    # Garante que a coluna de vetor exista no schema
    colunas = [c[0] for c in conn.execute("DESCRIBE main.chunks_contratos").fetchall()]
    if 'vetor_embedding' not in colunas:
        print("[INFO] Criando coluna vetor_embedding FLOAT[1536] na tabela main.chunks_contratos...")
        conn.execute("ALTER TABLE main.chunks_contratos ADD COLUMN vetor_embedding FLOAT[1536];")

    # Busca apenas registros pendentes de vetorização
    query_pendentes = """
        SELECT chunk_id, texto_chunk_denso
        FROM main.chunks_contratos
        WHERE vetor_embedding IS NULL
          AND texto_chunk_denso IS NOT NULL
        ORDER BY contrato_id, chunk_index
    """
    df_pendentes = conn.execute(query_pendentes).df()
    total_pendentes = len(df_pendentes)
    print(f"[INFO] Chunks pendentes de vetorização: {total_pendentes}")

    if total_pendentes == 0:
        print("[SUCESSO] Todos os chunks já estão vetorizados no DuckDB!")
        conn.close()
        return

    # Processamento em Batches com persistência incremental
    for i in tqdm(range(0, total_pendentes, tamanho_lote), desc="Gerando Embeddings Azure"):
        lote_df = df_pendentes.iloc[i : i + tamanho_lote].copy()
        textos = lote_df['texto_chunk_denso'].tolist()
        ids = lote_df['chunk_id'].tolist()

        vetores = obter_embeddings_lote(textos)
        
        # Cria DataFrame temporário para o lote
        df_lote_atualizar = pd.DataFrame({
            'chunk_id': ids,
            'vetor_embedding': vetores
        })

        conn.register("lote_temp", df_lote_atualizar)
        conn.execute("""
            UPDATE main.chunks_contratos AS c
            SET vetor_embedding = t.vetor_embedding
            FROM lote_temp AS t
            WHERE c.chunk_id = t.chunk_id;
        """)

    conn.close()
    print("\n[SUCESSO] Todos os embeddings foram gerados e salvos com sucesso no DuckDB!")

if __name__ == "__main__":
    processar_embeddings(tamanho_lote=128)