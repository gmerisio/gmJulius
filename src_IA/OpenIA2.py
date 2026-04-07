import duckdb
import os
from openai import AzureOpenAI

# CONFIGURAÇÃO 

client = AzureOpenAI(
    api_key = os.getenv("AZURE_OPENAI_KEY"),  
    api_version = "2025-01-01-preview", 
    azure_endpoint = os.getenv("ENDPOINT_EMBEDDING_SMALL") 
)
DEPLOYMENT_CHAT = "gpt-5-nano" 

# CONEXÃO
diretorio_script = os.path.dirname(os.path.abspath(__file__))
caminho_banco = os.path.join(diretorio_script, '..', 'bds', 'convenios.duckdb')
caminho_banco = os.path.normpath(caminho_banco)

con = duckdb.connect(caminho_banco, read_only=True)

# Sinônimos

def busca_inteligente(pergunta_original):
    print(f"\n--- INÍCIO DO PROCESSO ---")
    print(f"Pergunta Original: '{pergunta_original}'")

    prompt = f"""
    Você é um especialista em licitações e contratos públicos.PermissionError

    Tarefa:
    O usuário fez uma busca simples: "{pergunta_original}"
    Escreva uma string de busca otimizada para encontrar documentos oficiais.
    Inclua sinônimos técnicos, termos jurídicos relacionados e palavras-chave que costumam aparecer em contratos desse tema.

    Regras:
    - Retorne APENAS as palavras-chave separadas por espaço. 
    - Não explique nada. 
    - Exemplo: Se o usuário digitar "buraco na rua", retorne "pavimentação asfáltica tapa-buraco recapeamento via pública manutenção viária". 
    """

    try:
        response_opt = client.chat.completions.create(
            model=DEPLOYMENT_CHAT,
            messages=[{"role": "user", "content": prompt}],
            temperature=1
        )
        query_expandida = response_opt.choices[0].message.content.strip()
        print(f"   -> Termos Otimizados: '{query_expandida}'")

    except Exception as e:
        print(f"   -> Erro na otimização, usando pergunta original. Erro: {e}")
        query_expandida = pergunta_original

    # Executando a busca

    sql = f"""
    SELECT 
        c.id,
        c.numero_arquivo,
        substring(c.descricao, 1, 4000) as descricao,
        c.url_documento,
        c.publicado_em,
        fts_main_convenios.match_bm25(c.id, ?) AS score
    FROM convenios c
    WHERE score IS NOT NULL
    ORDER BY score DESC
    LIMIT 50
    """

    try:
        df_resultados = con.execute(sql, [query_expandida]).df()

        if df_resultados.empty:
            return "Não encontrei documentos relevantes mesmo expandindo a busca."
            
    except Exception as e:
        return f"Erro na busca SQL: {e}"

    contexto = df_resultados.to_string(index=False)
    
    prompt_final = f"""
    Você é um consultor jurídico da Prefeitura.
    
    CONTEXTO (Top 5 documentos encontrados sobre o tema):
    {contexto}
    
    PERGUNTA DO USUÁRIO: 
    "{pergunta_original}"
    
    INSTRUÇÃO:
    Com base EXCLUSIVAMENTE nos documentos acima, responda à pergunta do usuário.
    Cite o 'numero_arquivo' dos contratos que embasam sua resposta.
    Se os documentos não responderem à pergunta exata, diga o que eles abordam sobre o tema.
    """

    try:
        response_final = client.chat.completions.create(
            model=DEPLOYMENT_CHAT,
            messages=[{"role": "user", "content": prompt_final}],
            temperature=1 
        )
        return response_final.choices[0].message.content
        
    except Exception as e:
        return f"Erro ao gerar resposta final: {e}" 

# TESTE

resposta = busca_inteligente("Empresas contratadas para fornecer medicamentos para o município")
print("\nCHATBOT:\n", resposta) 