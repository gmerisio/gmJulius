import os
import gradio as gr
from pathlib import Path
from dotenv import load_dotenv
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import create_sql_agent
from langchain_openai import AzureChatOpenAI

# 1. CARREGAMENTO DAS CONFIGURAÇÕES
load_dotenv() 

# Caminhos
current_dir = Path(__file__).resolve().parent
db_path = current_dir.parent / "bds" / "convenios.duckdb"

# 2. CONEXÃO 
db_uri = f"duckdb:///{db_path}"
db = SQLDatabase.from_uri(db_uri)

# 3. CONFIGURAÇÃO DA LLM 
llm = AzureChatOpenAI(
    azure_deployment=os.getenv("AZURE_DEPLOYMENT_NAME"),
    api_version=os.getenv("OPENAI_API_VERSION"),
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    temperature=1
)

# 4. CRIAÇÃO DO AGENTE SQL
agent_executor = create_sql_agent(
    llm, 
    db=db, 
    agent_type="openai-tools", 
    verbose=True
)

# 5. CHATBOT
def responder_chat(mensagem, historico):
    try:
        resultado = agent_executor.invoke({"input": mensagem})
        return resultado["output"]
    except Exception as e:
        return f"⚠️ Erro no processamento: {str(e)}"

# 6. INTERFACE GRADIO
with gr.Blocks(theme=gr.themes.Default()) as demo:
    gr.Markdown(f"## 📊 Assistente de Dados (DuckDB)")
    gr.Markdown(f"Conectado ao arquivo: `{db_path.name}`")
    
    gr.ChatInterface(
        fn=responder_chat,
        description="Pergunte sobre as tabelas, médias, contagens ou filtros específicos.",
        examples=["Quais tabelas estão disponíveis?", "Faça um resumo da tabela de vendas"],
    )

if __name__ == "__main__":
    if not db_path.exists():
        print(f"❌ ALERTA: Banco não encontrado em: {db_path}")
    else:
        print(f"✅ Banco carregado. Iniciando Gradio...")
        demo.launch()