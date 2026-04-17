import sqlite3
import json
from pathlib import Path
import os

def converter_sqlite_para_json_individual(db_path, output_dir):

    conexao = None # Inicializa a conexão como None
    
    db_name = Path(db_path).stem # Ex: de "portaltp_IA.db" pega "portaltp_IA"

    try:
        # A conexão é aberta em modo read-only (leitura) para segurança.
        conexao = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
        cursor = conexao.cursor()
        print(f"✅ Conexão estabelecida com o banco de dados: {db_path}")

        # Obter a lista de todas as tabelas no banco de dados
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        nomes_tabelas = [tabela[0] for tabela in cursor.fetchall()]

        if not nomes_tabelas:
            print("AVISO: Nenhuma tabela foi encontrada no banco de dados.")
            return

        print(f"Tabelas encontradas: {nomes_tabelas}")

        # --- Garante que o diretório de saída exista ---
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        print(f"Diretório de saída '{output_dir}' está pronto.")

        # Iterar sobre cada tabela para extrair os dados e criar um JSON para cada uma
        for nome_tabela in nomes_tabelas:
            print(f"Processando tabela: {nome_tabela}...")
            
            cursor.execute(f"SELECT * FROM {nome_tabela}")
            linhas = cursor.fetchall()
            
            # Se a tabela estiver vazia, avisa e pula para a próxima
            if not linhas:
                print(f"  -> AVISO: A tabela '{nome_tabela}' está vazia. Nenhum JSON será gerado para ela.")
                continue # Pula para a próxima iteração do loop

            nomes_colunas = [descricao[0] for descricao in cursor.description]
            
            dados_tabela = []
            for linha in linhas:
                dados_tabela.append(dict(zip(nomes_colunas, linha)))

            # 1. Define o nome do arquivo JSON de saída para esta tabela
            json_filename = f"{nome_tabela}-{db_name}.json"
            json_path = Path(output_dir) / json_filename

            # 2. Escreve o dicionário da tabela atual no seu próprio arquivo JSON
            try:
                with open(json_path, 'w', encoding='utf-8') as arquivo_json:
                    # O dump é feito com 'dados_tabela', que contém os dados apenas da tabela atual
                    json.dump(dados_tabela, arquivo_json, indent=4, ensure_ascii=False)
                print(f"  -> ✅ Sucesso! Dados exportados para '{json_path}'")
            except IOError as e:
                print(f"  -> 🔴 Erro ao escrever o arquivo JSON para a tabela '{nome_tabela}': {e}")

    except sqlite3.Error as e:
        print(f"🔴 Erro ao acessar o banco de dados SQLite: {e}")
        return
    finally:
        if conexao:
            conexao.close()
            print("\nConexão com o banco de dados fechada.")


if __name__ == "__main__":
    
    # 1. Encontra o caminho absoluto do script atual
    script_path = Path(__file__).resolve()
    # 2. Sobe dois níveis para encontrar a pasta raiz do projeto 
    project_root = script_path.parent.parent
    
    # 3. Constrói o caminho para o banco de dados na pasta "bds"
    caminho_banco_dados = project_root / "bds" / "convenios.db"
    
    # --- Define a pasta de saída para os arquivos JSON ---
    # O diretório "jsons" será criado dentro da mesma pasta do script.
    caminho_pasta_saida = script_path.parent / "jsons"

    print(f"Caminho do Banco de Dados definido como: {caminho_banco_dados}")
    print(f"Arquivos JSON de saída serão salvos em: {caminho_pasta_saida}")
    
    # Chama a função principal com os caminhos corretos
    converter_sqlite_para_json_individual(caminho_banco_dados, caminho_pasta_saida)