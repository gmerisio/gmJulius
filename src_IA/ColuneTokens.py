import duckdb
import os
import tiktoken
import pandas as pd

# 1. CONFIGURAÇÃO DE CAMINHOS
diretorio_script = os.path.dirname(os.path.abspath(__file__))
caminho_banco = os.path.join(diretorio_script, '..', 'bds', 'convenios.duckdb')
caminho_banco = os.path.normcase(os.path.normpath(caminho_banco))

# 2. MODELO DE CODIFICAÇÃO 
encoder = tiktoken.get_encoding("cl100k_base")

def atualizar_tokens_conteudo():
    print(f"Conectando em: {caminho_banco}")
    con = duckdb.connect(caminho_banco, read_only=False)
    
    try:
        # 1. BUSCA DE DADOS
        print("Lendo 'texto_conteudo' do banco...")
        query_selecao = "SELECT rowid, texto_conteudo FROM convenios WHERE qtd_tokens IS NULL"
        df = con.execute(query_selecao).df()
        
        if df.empty:
            print("Nenhum registro encontrado com 'qtd_tokens' vazio.")
            return

        print(f"Calculando tokens para {len(df)} linhas...")

        # 2. CÁLCULO DE TOKENS
        def calcular(texto):
            if not texto or str(texto).lower() == 'nan':
                return 0
            # Garante que o conteúdo seja string
            return len(encoder.encode(str(texto)))

        df['tokens_calculados'] = df['texto_conteudo'].apply(calcular)

        # 3. ATUALIZAÇÃO EM MASSA
        # Registra o DataFrame como uma tabela temporária no DuckDB
        con.register('dados_novos', df)
        
        print("Atualizando coluna 'qtd_tokens' no banco...")
        con.execute("""
            UPDATE convenios 
            SET qtd_tokens = dados_novos.tokens_calculados
            FROM dados_novos 
            WHERE convenios.rowid = dados_novos.rowid
        """)
        
        # 4. VERIFICAÇÃO FINAL
        total_linhas = len(df)
        soma_tokens = df['tokens_calculados'].sum()
        print(f"Sucesso! {total_linhas} linhas atualizadas.")
        print(f"Total de tokens adicionados: {soma_tokens}")

    except Exception as e:
        print(f"Erro durante a execução: {e}")
    finally:
        con.close()
        print("Conexão fechada.")

if __name__ == "__main__":
    atualizar_tokens_conteudo()