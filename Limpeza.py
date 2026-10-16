import os
import re
import duckdb
import pandas as pd
import ftfy
import nltk
from nltk.corpus import stopwords
from nltk.stem import RSLPStemmer
from tqdm import tqdm

# CONFIGURAÇÃO DE DIRETÓRIO E BANCO
DIRETORIO_SCRIPT = os.path.dirname(os.path.abspath(__file__))
CAMINHO_BANCO = os.path.join(DIRETORIO_SCRIPT, "bds", "convenios.duckdb")

if not os.path.exists(CAMINHO_BANCO):
    raise FileNotFoundError(f"Banco não encontrado em: {CAMINHO_BANCO}")

print(f"[INFO] Conectando ao DuckDB em: {CAMINHO_BANCO}")

# CARREGAMENTO DE STOPWORDS E STEMMER
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

# FUNÇÕES DE PROCESSAMENTO LEVE
def limpar_artefatos_brutos(texto: str) -> str:
    """Visão Densa (Embeddings): remove ruídos e preserva estrutura."""
    if not texto or not isinstance(texto, str):
        return ""

    texto = ftfy.fix_text(texto)
    texto = re.sub(r'---\s*Pág\s*\d+\s*---', ' ', texto, flags=re.IGNORECASE)
    padrao_assinatura = r'(Assinado de forma digital por|Dados:\s*\d{4}\.\d{2}\.\d{2}|CPF:\s*\d{3}\.?\d{3}\.?\d{3}-?\d{2})'
    texto = re.sub(padrao_assinatura, ' ', texto, flags=re.IGNORECASE)
    texto = re.sub(r'[\{\}\[\]\(\)\<\>\|\/\\]', ' ', texto)
    texto = re.sub(r'\s+', ' ', texto).strip()
    return texto

def pipeline_bm25_rapido(texto_limpo: str) -> str:
    """Visão Esparsa (BM25): stemmer RSLP ultrarrápido sem alocação de tensores."""
    if not texto_limpo or not isinstance(texto_limpo, str):
        return ""
    
    # Extrai apenas palavras válidas 
    tokens = re_tokens.findall(texto_limpo.lower())
    
    # Aplica remoção de stopwords e extração do radical 
    radicais = [
        stemmer.stem(token) 
        for token in tokens 
        if token not in todas_stopwords
    ]
    return " ".join(radicais)

# EXECUÇÃO E PERSISTÊNCIA NO DUCKDB
def processar_e_atualizar():
    conn = duckdb.connect(CAMINHO_BANCO)

    # 1. Carrega registros
    query_select = """
        SELECT id, status_extracao, texto_conteudo
        FROM main.contratos_token
        WHERE status_extracao IN ('SUCESSO_OCR', 'SUCESSO_DIRETO')
    """
    print("[INFO] Executando consulta no banco de dados...")
    df = conn.execute(query_select).df()
    print(f"[INFO] {len(df)} registros recuperados para processamento.")

    if df.empty:
        print("[AVISO] Nenhum registro encontrado.")
        conn.close()
        return

    # 2. Pipeline Densa (Embeddings)
    tqdm.pandas(desc="Processando Embeddings")
    df['texto_limpo_denso'] = df['texto_conteudo'].progress_apply(limpar_artefatos_brutos)

    # 3. Pipeline Esparsa (BM25 com RSLP)
    tqdm.pandas(desc="Processando BM25 (RSLP)")
    df['texto_limpo_lexico'] = df['texto_limpo_denso'].progress_apply(pipeline_bm25_rapido)

    # 4. Verificação de colunas no DuckDB
    print("[INFO] Verificando estrutura da tabela no DuckDB...")
    colunas_existentes = [col[0] for col in conn.execute("DESCRIBE main.contratos_token").fetchall()]
    
    if 'texto_limpo_denso' not in colunas_existentes:
        conn.execute("ALTER TABLE main.contratos_token ADD COLUMN texto_limpo_denso VARCHAR;")
    if 'texto_limpo_lexico' not in colunas_existentes:
        conn.execute("ALTER TABLE main.contratos_token ADD COLUMN texto_limpo_lexico VARCHAR;")

    # 5. Atualização no DuckDB
    print("[INFO] Gravando atualizações no DuckDB...")
    conn.register("df_processado", df[['id', 'texto_limpo_denso', 'texto_limpo_lexico']])
    
    conn.execute("""
        UPDATE main.contratos_token AS c
        SET 
            texto_limpo_denso = d.texto_limpo_denso,
            texto_limpo_lexico = d.texto_limpo_lexico
        FROM df_processado AS d
        WHERE c.id = d.id;
    """)

    conn.close()
    print("[SUCESSO] Pipeline concluído com sucesso e persistido no DuckDB!")

if __name__ == "__main__":
    processar_e_atualizar()