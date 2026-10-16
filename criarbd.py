import os
import duckdb

# Mapear o diretório base 
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Caminho da subpasta bds 
PASTA_BDS = os.path.join(BASE_DIR, "bds")

# Origem
CAMINHO_BD_ORIGEM = os.path.join(PASTA_BDS, "convenios.duckdb")
# Destino
CAMINHO_BD_DESTINO = os.path.join(PASTA_BDS, "TCCa.duckdb")

def criar_banco_tcca():
    # Validação do arquivo de origem
    if not os.path.exists(CAMINHO_BD_ORIGEM):
        raise FileNotFoundError(f"Arquivo não encontrado em: {CAMINHO_BD_ORIGEM}")

    print("Conectando ao banco convenios.duckdb...")
    con = duckdb.connect(CAMINHO_BD_ORIGEM)

    try:
        # Anexar o novo banco 
        print(f"Criando/Anexando o novo banco em: {CAMINHO_BD_DESTINO}")
        con.execute(f"ATTACH '{CAMINHO_BD_DESTINO}' AS tcca;")

        # Copiar a tabela contratos_token 
        print("Copiando tabela 'contratos_token'...")
        con.execute("""
            CREATE TABLE tcca.contratos_token AS 
            SELECT * FROM main.contratos_token;
        """)

        # Copiar a tabela chunks_contratos
        print("Copiando tabela 'chunks_contratos'...")
        con.execute("""
            CREATE TABLE tcca.chunks_contratos AS 
            SELECT * FROM main.chunks_contratos;
        """)

        # Desconectar o banco secundário para salvar e liberar o arquivo
        con.execute("DETACH tcca;")
        print("\nProcesso concluído! TCCa.duckdb criado com sucesso na pasta 'bds'.")

    finally:
        con.close()

if __name__ == "__main__":
    criar_banco_tcca()