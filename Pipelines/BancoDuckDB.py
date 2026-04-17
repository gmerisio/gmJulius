import duckdb

# Ajuste os caminhos conforme necessário
sqlite_file = r"C:\Users\gmeri\OneDrive\Área de Trabalho\Pasta Pessoal\Trabalho\gmJulius\bds\ContratosSGDP.db"
duckdb_file = r"C:\Users\gmeri\OneDrive\Área de Trabalho\Pasta Pessoal\Trabalho\gmJulius\bds\convenios.duckdb"

# Nome da tabela dentro do SQLite
tabela_origem = "contratos"

con = duckdb.connect(duckdb_file)

# Instala/Carrega a extensão SQLite, conecta o arquivo SQLite ao DuckDB (Attach) e cria a tabela 'dados' copiando da origem
con.execute(f"""
    INSTALL sqlite;
    LOAD sqlite;
    ATTACH '{sqlite_file}' AS db_origem (TYPE SQLITE);
    
    CREATE OR REPLACE TABLE dados AS 
    SELECT * FROM db_origem.{tabela_origem};
""")

con.close()

print("OK — Dados migrados do SQLite para:", duckdb_file)