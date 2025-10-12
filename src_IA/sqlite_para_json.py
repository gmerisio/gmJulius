import sqlite3
import json
from pathlib import Path

def converter_sqlite_para_json(db_path, json_path):
    dados_finais = {}
    conexao = None # Inicializa a conexão como None

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

        # Iterar sobre cada tabela para extrair os dados
        for nome_tabela in nomes_tabelas:
            print(f"Processando tabela: {nome_tabela}...")
            
            # Executa a consulta PARA A TABELA ATUAL DENTRO DO LOOP
            cursor.execute(f"SELECT * FROM {nome_tabela}")
            
            # Busca todos os resultados da consulta
            linhas = cursor.fetchall()
            
            # Obter os nomes das colunas a partir do 'cursor.description'
            nomes_colunas = [descricao[0] for descricao in cursor.description]
            
            # Lista para armazenar os dados da tabela formatados como dicionários
            dados_tabela = []
            for linha in linhas:
                # Cria um dicionário combinando os nomes das colunas com os valores da linha
                dados_tabela.append(dict(zip(nomes_colunas, linha)))
            
            # Adiciona a lista de dados da tabela ao dicionário principal
            dados_finais[nome_tabela] = dados_tabela

    except sqlite3.Error as e:
        print(f"🔴 Erro ao acessar o banco de dados SQLite: {e}")
        return
    finally:
        # Fechar a conexão com o banco de dados
        if conexao:
            conexao.close()
            print("Conexão com o banco de dados fechada.")

    # Se não houver dados, não criar o arquivo JSON vazio
    if not dados_finais:
        print("Nenhum dado foi extraído, o arquivo JSON não será criado.")
        return

    # Escrever o dicionário python no arquivo JSON
    try:
        with open(json_path, 'w', encoding='utf-8') as arquivo_json:
            json.dump(dados_finais, arquivo_json, indent=4, ensure_ascii=False)
        print(f"✅ Sucesso! Dados exportados para '{json_path}'")
    except IOError as e:
        print(f"🔴 Erro ao escrever o arquivo JSON: {e}")


if __name__ == "__main__":
    # --- Construção robusta dos caminhos ---
    
    # 1. Encontra o caminho absoluto do script atual
    script_path = Path(__file__).resolve()
    # 2. Sobe dois níveis para encontrar a pasta raiz do projeto 
    project_root = script_path.parent.parent
    
    # 3. Constrói o caminho para o banco de dados na pasta "bds"
    caminho_banco_dados = project_root / "bds" / "portaltp_IA.db"
    
    # 4. Define que o arquivo JSON de saída será salvo na raiz do projeto
    caminho_arquivo_json = script_path.parent / "tabelas.json"

    print(f"Caminho do Banco de Dados definido como: {caminho_banco_dados}")
    print(f"Arquivo JSON de saída será salvo em: {caminho_arquivo_json}")
    
    # Chama a função principal com os caminhos corretos
    converter_sqlite_para_json(caminho_banco_dados, caminho_arquivo_json)