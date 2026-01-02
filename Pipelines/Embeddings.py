import duckdb
import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
import torch

# Definir o caminho para o arquivo DuckDB
DB_FILE = 'julius.duckdb' 

# Conectar-se ao DuckDB
# Conecta ao arquivo e permite operações de leitura e escrita (read_only=False)
try:
    con = duckdb.connect(database=DB_FILE, read_only=False)
    print(f"Conexão bem-sucedida com {DB_FILE}")
except Exception as e:
    print(f"Erro ao conectar ao DuckDB: {e}")
    exit()

# Ler a Tabela e a Coluna de Texto
QUERY = "SELECT id, texto_extraido FROM dados ORDER BY id"

# fetchdf() carrega o resultado da consulta SQL diretamente em um DataFrame do Pandas
df_dados = con.execute(QUERY).fetchdf()

# Exibir o que foi lido (Verificação)
print("\nDados lidos para processamento:")
print(df_dados.head())

print("\nCarregando o modelo de Embedding...")

# Escolha do Modelo
device = 'cuda' if torch.cuda.is_available() else 'cpu'
model = SentenceTransformer('all-MiniLM-L6-v2', device=device)

# Determinar a Dimensão: Este modelo gera vetores de 384 números.
EMBEDDING_DIM = model.get_sentence_embedding_dimension()

print(f"Modelo carregado. Dispositivo: {device}. Dimensão do Vetor (Embedding): {EMBEDDING_DIM}")

# Extrair apenas a lista de textos que será codificada
texts_to_embed = df_dados['texto_extraido'].tolist()

print(f"Gerando embeddings para {len(texts_to_embed)} documentos...")

#  Ação Principal: Gerar os Vetores
embeddings_matrix = model.encode(
    texts_to_embed, 
    convert_to_numpy=True, 
    show_progress_bar=True
)

print("Geração concluída.")
print(f"A matriz de embeddings tem a forma: {embeddings_matrix.shape}")
# Exemplo de saída: (1000, 384) -> 1000 documentos, 384 dimensões

# Adicionar os Embeddings ao DataFrame
# Para facilitar a escrita no DuckDB, criamos uma lista de listas (Python nativo)
# e anexamos como uma nova coluna no DataFrame original (df_dados).
df_dados['vetor_embedding'] = embeddings_matrix.tolist()

# df_dados agora tem o 'id', o 'texto' e o 'vetor_embedding' correspondente.
print("\nDataFrame com a nova coluna de vetores (primeira linha):")
print(df_dados[['id', 'vetor_embedding']].head(1))

print("\nCriando a coluna 'vetor_embedding' na tabela DuckDB...")

try:
    con.execute(f"ALTER TABLE dados ADD COLUMN vetor_embedding FLOAT[{EMBEDDING_DIM}]")
    print("Coluna criada com sucesso.")
except duckdb.InternalError as e:
    # Captura erro se a coluna já existir (o que é normal)
    if "Column with name vetor_embedding already exists" in str(e):
        print("A coluna 'vetor_embedding' já existe, prosseguindo com a atualização.")
    else:
        raise e
    
# 7. Inserir os Embeddings
print("Inserindo/Atualizando os vetores no DuckDB (via JOIN com Pandas)...")

# O comando usa o DataFrame df_dados como uma tabela temporária de origem e faz o JOIN pelo 'id' para garantir que o vetor vá para a linha certa.
con.execute("""
    UPDATE dados
    SET vetor_embedding = T1.vetor_embedding
    FROM df_dados AS T1
    WHERE dados.id = T1.id;
""")

print("Armazenamento concluído!")

# Fechar a conexão
con.close()