import sqlite3
import pandas as pd
import duckdb

def processar_sqlite_via_pandas(db_path, duckdb_path, tabela_sqlite, tabela_duckdb):
    print("1. Lendo dados do SQLite para o Pandas...")
    
    # Conecta ao SQLite
    conn_sqlite = sqlite3.connect(db_path)
    
    # Lê a tabela inteira para um DataFrame
    query = f"SELECT * FROM {tabela_sqlite}"
    df = pd.read_sql_query(query, conn_sqlite)
    
    conn_sqlite.close()
    
    # ---------------------------------------------------------
    # ÁREA DE MANIPULAÇÃO DE DADOS
    print(f"   -> Dados carregados: {df.shape[0]} linhas e {df.shape[1]} colunas.")
    
    # Exemplo: df['nova_coluna'] = df['valor'] * 2
    # ---------------------------------------------------------

    print("2. Salvando do Pandas para o DuckDB...")
    
    # Conecta ao DuckDB
    con_duck = duckdb.connect(duckdb_path)
    
    # O DuckDB consegue ler o DataFrame 'df' diretamente da memória do Python
    # O comando REGISTER garante que o SQL do DuckDB enxergue a variável 'df'
    con_duck.register('df_view', df)
    
    # Cria a tabela no DuckDB a partir do DataFrame
    con_duck.execute(f"CREATE OR REPLACE TABLE {tabela_duckdb} AS SELECT * FROM df_view")
    
    con_duck.close()
    print("Sucesso! Processo finalizado.")

# --- Configuração ---
arquivo_sqlite = '.../convenios.db'
arquivo_duckdb = 'final.duckdb'
nome_tabela_origem = 'nome_da_tabela_no_sqlite' 
nome_tabela_destino = 'minha_tabela_limpa'

if __name__ == "__main__":
    processar_sqlite_via_pandas(arquivo_sqlite, arquivo_duckdb, nome_tabela_origem, nome_tabela_destino)