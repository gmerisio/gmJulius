import duckdb
import os

# 1. Configuração dos caminhos
diretorio_atual = os.path.dirname(os.path.abspath(__file__))
pasta_bds = os.path.join(diretorio_atual, '..', 'bds')

caminho_duckdb = os.path.join(pasta_bds, 'convenios.duckdb')
caminho_sqlite = os.path.join(pasta_bds, 'ContratosSGDP.db')

# 2. Conectar ao DuckDB
con = duckdb.connect(caminho_duckdb)

try:
    # Garante a extensão e anexa o banco
    con.execute("INSTALL sqlite; LOAD sqlite;")
    
    con.execute(f"ATTACH '{caminho_sqlite}' AS db_sqlite (TYPE SQLITE);")

    # 3. Buscar nomes das tabelas usando a visualização do DuckDB sobre o SQLite
    tabelas_query = con.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_catalog = 'db_sqlite'
    """).fetchall()

    if not tabelas_query:
        print("⚠️ Nenhuma tabela encontrada. Verifique se o nome do arquivo .db está correto.")
    else:
        print(f"🔄 Iniciando a migração de {len(tabelas_query)} tabelas...")
        for (nome_tabela,) in tabelas_query:
            # 4. Criar/Substituir no main do DuckDB
            con.execute(f'CREATE OR REPLACE TABLE main."{nome_tabela}" AS SELECT * FROM db_sqlite."{nome_tabela}"')
            print(f"✅ Tabela '{nome_tabela}' copiada com sucesso.")

    con.execute("DETACH db_sqlite;")
    print("\n✨ Processo concluído!")

except Exception as e:
    print(f"❌ Erro durante o pipeline: {e}")

finally:
    con.close()