import duckdb
import os

diretorio_atual = os.path.dirname(os.path.abspath(__file__))
caminho_db = os.path.join(diretorio_atual, '..', 'bds', 'convenios.duckdb')

# Conexão
con = duckdb.connect(caminho_db)

try:
    con.execute(f"""
        -- Cria uma tabela temporária com a estrutura correta
        CREATE TABLE tabela_temp AS 
        SELECT 
            row_number() OVER () AS id,
            * EXCLUDE (id, id_linha)
        FROM convenios;

        -- Deleta a tabela antiga
        DROP TABLE convenios;

        -- Renomeia a nova
        ALTER TABLE tabela_temp RENAME TO convenios;
    """)
    
    print(f"✅ Sucesso! Banco em '{caminho_db}' atualizado.")

except Exception as e:
    print(f"❌ Erro ao processar o banco: {e}")

finally:
    con.close()