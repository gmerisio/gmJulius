import os
import gradio as gr
import duckdb
from pathlib import Path
from dotenv import load_dotenv
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import create_sql_agent
from langchain_openai import AzureChatOpenAI

# CARREGAMENTO DAS CONFIGURAÇÕES
load_dotenv() 

# Caminhos
current_dir = Path(__file__).resolve().parent
db_path = current_dir.parent / "bds" / "convenios.duckdb"

def explicar_bd():
    """Inserindo comentários nas tabelas do banco de dados para melhor contexto na busca"""
    if not db_path.exists():
        return
    
    with duckdb.connect(str(db_path)) as con:
        con.execute("COMMENT ON TABLE atas IS 'Processos de atas de registro, contendo informações sobre o número do documento, a quem se refere, quando foi assinada, onformações sobre a ata, situação que a ata se encontra, o valor dela, entre outras coisas.'")
        con.execute("COMMENT ON TABLE bens_consolidado IS 'Tabela contendo bens móveis e bens imóveis, havendo informações sobre o ben, onde ele se encontra, o estado dele, qual o movimento (aquisição/baixa/etc), o valor dele, entre outras coisas.'")
        con.execute("COMMENT ON TABLE bens_moveis IS 'Tabela específica para os bens móveis, informações sobre eles, movimento, o tombamento, classificação do bem, localidade, entre outras coisas.'")
        con.execute("COMMENT ON TABLE bens_imoveis IS 'Tabela específica para bens imóveis, informações sobre o movimento, a data, explicação do que esta sendo feito, valores, entre outros.'")
        con.execute("COMMENT ON TABLE cargos_confianca IS 'Tabela referente à servidores e seus cargos'")
        con.execute("COMMENT ON TABLE contratos IS 'Tabela que se refere aos contratos, o número do contrato, o número do processo que ele se refere, data, documento favorecido, pessoa/empresa favorecida, categoria do contrato,, objeto, situação, valores, entre outros.'")
        con.execute("COMMENT ON TABLE convenios_firmados IS 'Tabela referente a convênios, data do convênio, tipo do convênio, documento participante, nome participante, objetivo do convênio, valores, entre outras coisas.'")
        con.execute("COMMENT ON TABLE contratos_token IS 'Tabela referente a contratos originalmente em pdf que foram feitos OCR, várias colunas referentes aos contratos.'")
        con.execute("COMMENT ON TABLE diarias IS 'Tabela que se refere à diárias concebidas a servidores ou prestadores de serviços, algumas colunas destrinchando as diárias como valor, a quem foi concedido, quando, etc.'")
        con.execute("COMMENT ON TABLE dispensas_inexigibilidades IS 'Tabela que se refere a dispensas em processos, tem o número da dispensa e do processo, o porque ocorreu, a base legal, data, valor, entre outras coisas.'")
        con.execute("COMMENT ON TABLE duodecimo IS 'VPA/VPD, conta, a fonte do recurso, documento favorecido, valores, entre outras coisas.'")
        con.execute("COMMENT ON TABLE empenhos IS 'Empenhos, tipos de empenhos, sas funções e subfunções, fonte do recurso, documento favorecido, valores, entre outros.'")
        con.execute("COMMENT ON TABLE estagiarios IS 'Tabela contendo todas as informações do estagiários, tal como, onde trabalham, vínculo, valores, entre outros.'")
        con.execute("COMMENT ON TABLE execucao_receitas IS 'Tabela sobre execução de receitas e informações sobre, como valores, fonte do recurso, origem, receita, valores, entre outros.'")
        con.execute("COMMENT ON TABLE frota_veiculos IS 'Tabela referente a frotas, situação do veículo (aluguel/oficial/etc), descrições dos veículos, onde estão, entre outros.'")
        con.execute("COMMENT ON TABLE liquidacoes IS 'Liquidações, sua categoria, subcategoria, fonte do recurso, despesas, a quem se favorece, valores, entre outros.'")
        con.execute("COMMENT ON TABLE materiais_entradas IS 'Almoxarifado, notas fiscais, data e tipo da entrada, favorecidos, valores, entre outros.'")
        con.execute("COMMENT ON TABLE materiais_saidas IS 'Almoxarifado, a requisição, data, categoria, requisitante, valores, entre outros.'")
        con.execute("COMMENT ON TABLE obras IS 'Tabela sobre obras, data, espécie, despesa, favorecidos, valores, entre outros.'")
        con.execute("COMMENT ON TABLE orcamento_despesas IS 'Tabela sobre despesas, com função e subfunção, recursos, valores, entre outros.'")
        con.execute("COMMENT ON TABLE orcamento_receitas IS 'Tabela referente a receitas, sua origem, espécie, alinea e subalinea, caregoria, valor, entre outras coisas'")
        con.execute("COMMENT ON TABLE ordemcompras IS 'Tabele se refere a ordem de compras, a quem favorece e o valor'")
        con.execute("COMMENT ON TABLE pagamentos IS 'Tabela referente a pagamentos, sua categoria, funçao, subfunção,  de onde veio, a quem vai, valores,  entre outros.'")
        con.execute("COMMENT ON TABLE restos_pagar IS 'Tabela contendo o empenho, a categoria dele, função,  subfunção,  fonte do recurso, a qiem esta sendo pago,  valor, entre outros'")
        con.execute("COMMENT ON TABLE subvencoes  IS 'Contem a liquidação,  o pagamento, o tipo de pagamento, função, subfunção,  fonte do recurso, favorecido,  valor, entre outros.'")
        con.execute("COMMENT ON TABLE transf_extraorcamentarias IS 'Contem o tipo da transferência, origem, numero, conta, fonte do recurso,  documento favorecido, valores, entre outros.'")
        con.execute("COMMENT ON TABLE transf_intraorcamentarias IS 'Contem tipo da transferência, origem, numero, conta contabil, a fonte do recurso, documento favorecido, valores, entre outros.'")
        con.execute("COMMENT ON TABLE vagas_cargos IS 'Tabela com nome do cargo, vagas disponiveis e ocupadas.'")


        tabelas_vazias = ['despesas_emergenciais', 'convenios_recebidos', 'dispensas_emergenciais', 'divida_ativa', 'licitacoes', 'receitas_emergenciais', 'servidores']
        for tab in tabelas_vazias:
            con.execute(f"COMMENT ON TABLE {tab} IS 'Tabela atualmente sem registros.'")

explicar_bd()

# CONEXÃO 
db_uri = f"duckdb:///{db_path}"
db = SQLDatabase.from_uri(db_uri)

# CONFIGURAÇÃO DA LLM 
llm = AzureChatOpenAI(
    azure_deployment=os.getenv("AZURE_DEPLOYMENT_NAME"),
    api_version=os.getenv("OPENAI_API_VERSION"),
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    temperature=1
)

# CRIAÇÃO DO AGENTE SQL

instrucao = """
Sempre verifique se a informação solicitada pode estar em tabelas similares 
(ex: contratos e convenios). Se o usuário pedir valores, busque em todas as 
tabelas que possuem a coluna 'valor' ou 'valor_padrao'.
"""

agent_executor = create_sql_agent(
    llm, 
    db=db, 
    agent_type="openai-tools", 
    verbose=True,
    suffix=instrucao
)

# CHATBOT
def responder_chat(mensagem, historico):
    try:
        resultado = agent_executor.invoke({"input": mensagem})
        return resultado["output"]
    except Exception as e:
        return f"⚠️ Erro no processamento: {str(e)}"

# INTERFACE GRADIO
with gr.Blocks(theme=gr.themes.Default()) as demo:
    gr.Markdown(f"## 📊 Julius IA")
    
    gr.ChatInterface(
        fn=responder_chat,
        description="Assistente de dados.",
        examples=["Quais informações posso buscar no Julius?", "Qual o periodo de tempo o Julius contempla?"],
    )

if __name__ == "__main__":
    if not db_path.exists():
        print(f"❌ ALERTA: Banco não encontrado em: {db_path}")
    else:
        print(f"✅ Banco carregado. Iniciando Gradio...")
        demo.launch()