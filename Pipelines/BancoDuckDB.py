import duckdb

parquet_file = r"C:\Users\gmeri\OneDrive\Área de Trabalho\Pasta Pessoal\Trabalho\gmJulius\dados.parquet"
duckdb_file = r"C:\Users\gmeri\OneDrive\Área de Trabalho\Pasta Pessoal\Trabalho\gmJulius\Julius.duckdb"

con = duckdb.connect(duckdb_file)
con.execute(f"""
    CREATE OR REPLACE TABLE dados AS
    SELECT * FROM read_parquet('{parquet_file}');
""")
con.close()

print("OK — Banco criado:", duckdb_file)
