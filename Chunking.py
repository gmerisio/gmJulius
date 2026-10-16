import os
import duckdb
import pandas as pd
import tiktoken
from tqdm import tqdm
import re
import nltk
from nltk.corpus import stopwords
from nltk.stem import RSLPStemmer

# CONFIGURAÇÕES E AMBIENTE
DIRETORIO_SCRIPT = os.path.dirname(os.path.abspath(__file__))
CAMINHO_BANCO = os.path.join(DIRETORIO_SCRIPT, "bds", "convenios.duckdb")

if not os.path.exists(CAMINHO_BANCO):
    raise FileNotFoundError(f"Banco não encontrado em: {CAMINHO_BANCO}")

print(f"[INFO] Conectando ao DuckDB em: {CAMINHO_BANCO}")

# Tokenizador 
enc = tiktoken.get_encoding("cl100k_base")

# Recursos para gerar a visão léxica do chunk
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')

try:
    nltk.data.find('stemmers/rslp')
except LookupError:
    nltk.download('rslp')

stop_words_pt = set(stopwords.words('portuguese'))
stop_words_dominio = {
    'prefeitura', 'municipal', 'senhor', 'senhora', 'termo', 'contrato',
    'clausula', 'aditivo', 'apostilamento', 'processo', 'administrativo',
    'gabriel', 'palha', 'espirito', 'santo', 'estado', 'secretaria'
}
todas_stopwords = stop_words_pt.union(stop_words_dominio)
stemmer = RSLPStemmer()
re_tokens = re.compile(r'\b[a-záàâãéèêíïóôõöúçñ]{3,}\b', flags=re.IGNORECASE)

def pipeline_bm25_chunk(texto: str) -> str:
    """Aplica o stemmer RSLP no pedaço de texto."""
    if not texto:
        return ""
    tokens = re_tokens.findall(texto.lower())
    radicais = [stemmer.stem(t) for t in tokens if t not in todas_stopwords]
    return " ".join(radicais)

# FUNÇÃO DE FATIAMENTO POR TOKENS
def quebrar_em_chunks(texto: str, max_tokens=500, overlap=60):
    """Fatia o texto denso usando sliding window com sobreposição baseada em tokens."""
    if not texto or not isinstance(texto, str):
        return []

    tokens = enc.encode(texto)
    total_tokens = len(tokens)

    if total_tokens <= max_tokens:
        return [(texto, total_tokens)]

    chunks = []
    inicio = 0
    passo = max_tokens - overlap

    while inicio < total_tokens:
        fim = min(inicio + max_tokens, total_tokens)
        slice_tokens = tokens[inicio:fim]
        texto_chunk = enc.decode(slice_tokens)
        chunks.append((texto_chunk, len(slice_tokens)))
        
        if fim >= total_tokens:
            break
        inicio += passo

    return chunks

# EXECUÇÃO E POPULAÇÃO NO DUCKDB
def executar_chunking():
    conn = duckdb.connect(CAMINHO_BANCO)

    # Carrega dados já limpos
    print("[INFO] Lendo registros limpos da tabela contratos_token...")
    query = """
        SELECT id, status_extracao, texto_limpo_denso
        FROM main.contratos_token
        WHERE status_extracao IN ('SUCESSO_OCR', 'SUCESSO_DIRETO')
          AND texto_limpo_denso IS NOT NULL 
          AND LENGTH(texto_limpo_denso) > 0
    """
    df = conn.execute(query).df()
    print(f"[INFO] {len(df)} documentos para realizar chunking.")

    # Processa o fatiamento
    linhas_chunks = []

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Gerando Chunks"):
        doc_id = row['id']
        status = row['status_extracao']
        texto = row['texto_limpo_denso']

        pedacos = quebrar_em_chunks(texto, max_tokens=500, overlap=60)
        total_p = len(pedacos)

        for idx, (chunk_denso, qtd_tok) in enumerate(pedacos):
            chunk_lexico = pipeline_bm25_chunk(chunk_denso)
            linhas_chunks.append({
                'chunk_id': f"{doc_id}_c{idx}",
                'contrato_id': doc_id,
                'status_extracao': status,
                'chunk_index': idx,
                'total_chunks': total_p,
                'texto_chunk_denso': chunk_denso,
                'texto_chunk_lexico': chunk_lexico,
                'qtd_tokens': qtd_tok
            })

    df_chunks = pd.DataFrame(linhas_chunks)
    print(f"[INFO] Total de chunks gerados a partir do corpus: {len(df_chunks)} fatias.")

    # Criação da tabela no DuckDB
    print("[INFO] Criando e persistindo a tabela main.chunks_contratos...")
    conn.execute("DROP TABLE IF EXISTS main.chunks_contratos;")
    conn.register("df_chunks_temp", df_chunks)
    conn.execute("CREATE TABLE main.chunks_contratos AS SELECT * FROM df_chunks_temp;")

    # Inspeciona contagem por estrato
    resumo = conn.execute("""
        SELECT status_extracao, count(*) as total_chunks, round(avg(qtd_tokens), 2) as media_tokens
        FROM main.chunks_contratos
        GROUP BY status_extracao
    """).df()
    print("\n--- RESUMO DOS CHUNKS GERADOS ---")
    print(resumo.to_string(index=False))

    conn.close()
    print("\n[SUCESSO] Tabela chunks_contratos criada com sucesso!")

if __name__ == "__main__":
    executar_chunking()