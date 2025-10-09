import os
import sqlite3

# --- CONFIGURAÇÕES ---
PASTA_BDS = "bds"
NOME_BANCO = "convenios.db"
NOME_TABELA_DB = "convenios"
CAMINHO_BANCO_COMPLETO = os.path.join(PASTA_BDS, NOME_BANCO)

# --- MAPEAMENTO DE IDS PARA CATEGORIAS ---
# Dicionário que relaciona o id_pagina com a sua respectiva descrição.
MAPEAMENTO_IDS = {
    136: "Contratações e Aquisições (COVID-19)",
    143: "Receitas (COVID-19)",
    156: "Conselho ou Comissão (COVID-19)",
    132: "Coronavírus (COVID-19)",
    222: "Vacinação (COVID-19)",
    8: "Contratos e Aditivos",
    9: "Licitações, Dispensas e Outros",
    41: "Atas de Registro de Preço",
    1012: "Plano Anual de Contratações",
    77: "Atas de Adesão",
    55: "Atas das Licitações",
    219: "Fiscal de Contratos",
    34: "Editais",
    371: "Legislação Tributária Municipal",
    631: "Renúncias Fiscais",
    153: "Despesas com Obras",
    76: "Ordem Cronológica dos Pagamentos",
    311: "Tabela de Diárias (Valores)",
    33: "Termo de Convênios",
    106: "Prestação de Contas de OSCIPS",
    248: "Emendas Parlamentares",
    481: "Emendas Parlamentares Recebidas",
    104: "Termo de Fomento, Colaboração e/ou Adesão de Cooperação",
    458: "Audiências Públicas",
    881: "Consultas Públicas",
    1: "Plano Plurianual (PPA)",
    2: "Lei de Diretrizes Orçamentárias (LDO)",
    3: "Lei Orçamentária Anual (LOA)",
    4: "Relatório Res. Execução Orçamentária (RREO)",
    5: "Relatório Gestão Fiscal (RGF)",
    7: "Balanço Anual",
    6: "Balancetes Mensais",
    60: "Créditos Suplementares",
    590: "Relatório Circunstanciado de Anos Anteriores",
    64: "Concursos em Andamento",
    1390: "Resultados dos Concursos",
    1342: "Listagem dos Terceirizados",
    39: "Relatório Estatístico do e-SIC",
    555: "Relatório estatístico de ouvidoria",
    1255: "Pesquisa de satisfação",
    99: "Plano Anual de Auditoria Interna (PAAI)",
    57: "Instruções Normativas (INs)",
    56: "Auditorias e Inspeções",
    63: "Recomendações e Pareceres Técnicos",
    58: "Relatórios do Controle Interno",
    59: "Legislação Específica",
    36: "Prestação de Contas Anual (PCA)",
    61: "Parecer do Tribunal de Contas",
    1074: "Julgamento das Contas da Câmara",
    1241: "Plano Estratégico Institucional",
    230: "Plano de Ação (SIAFIC)",
    2019: "Plano de Saúde",
    557: "Programação Anual de Saúde - PAS",
    558: "Relatório Anual de Gestão - RAG",
    514: "Agenda da Secretária de Saúde",
    515: "Ações de Controle Interno do FMS",
    914: "Contratos de Gestão com Organizações Sociais",
    547: "Conferência de Saúde",
    1346: "Lista de espera nas Creches Públicas",
    656: "Legislação Específica da Ouvidoria",
    747: "Dúvidas Frequentes",
    2172: "Plano de Dados Abertos - PDA",
    272: "Conselho Municipal de Defesa Civil",
    277: "Conselho do Fundo de Desenvolvimento Municipal",
    454: "Conselho Municipal de Cultura",
    605: "Conselho Municipal de Saúde",
    606: "Conselho Municipal de Educação",
    611: "Conselho Mun. de Defesa dos Direitos da Pessoa Idosa",
    868: "Conselho dos Direitos da Criança e do Adolescente",
    874: "Conselho Municipal de Desenvolvimento Rural",
    1307: "Quantitativos Executados e Preços Praticados",
    2167: "Estudo de Impactos das Obras"
}

def ajustar_banco_de_dados():
    
    if not os.path.exists(CAMINHO_BANCO_COMPLETO):
        print(f"🔴 ERRO: O banco de dados '{CAMINHO_BANCO_COMPLETO}' não foi encontrado.")
        print("Certifique-se de que o script de extração já foi executado.")
        return

    print(f"Buscando o banco de dados em: '{CAMINHO_BANCO_COMPLETO}'")
    conn = sqlite3.connect(CAMINHO_BANCO_COMPLETO)
    cursor = conn.cursor()
    print("✅ Conexão com o banco de dados estabelecida.")

    # --- 1. Adicionar a nova coluna 'nome_categoria' ---
    try:
        cursor.execute(f"ALTER TABLE {NOME_TABELA_DB} ADD COLUMN nome_categoria TEXT")
        print("  -> Coluna 'nome_categoria' adicionada com sucesso.")
        conn.commit()
    except sqlite3.OperationalError as e:
        if "duplicate column name" in str(e):
            print("  -> AVISO: A coluna 'nome_categoria' já existe.")
        else:
            print(f"  🔴 ERRO ao adicionar coluna: {e}")
            conn.close()
            return
            
    # --- 2. Preencher a nova coluna com base no mapeamento ---
    print("\n🔄 Atualizando as categorias com base no 'id_pagina'...")
    total_atualizado = 0
    for id_pagina, nome_categoria in MAPEAMENTO_IDS.items():
        cursor.execute(
            f"UPDATE {NOME_TABELA_DB} SET nome_categoria = ? WHERE id_pagina = ?",
            (nome_categoria, id_pagina)
        )
        # Acumula o número de linhas afetadas em cada update
        total_atualizado += cursor.rowcount
        
    conn.commit()
    print(f"  -> ✅ {total_atualizado} registros foram atualizados com a nova categoria.")

    # --- 3. Remover linhas com 'descricao' vazia ou com erro ---
    print("\n🗑️  Procurando por registros com descrição vazia ou com erro para remover...")
    
    # Contar quantos registros serão removidos antes de apagar
    cursor.execute(f"SELECT COUNT(*) FROM {NOME_TABELA_DB} WHERE descricao IS NULL OR trim(descricao) = '' OR descricao LIKE 'ERRO:%'")
    num_linhas_para_remover = cursor.fetchone()[0]

    if num_linhas_para_remover > 0:
        print(f"  -> {num_linhas_para_remover} registros encontrados. Removendo...")
        cursor.execute(f"DELETE FROM {NOME_TABELA_DB} WHERE descricao IS NULL OR trim(descricao) = '' OR descricao LIKE 'ERRO:%'")
        conn.commit()
        print(f"  -> ✅ {num_linhas_para_remover} registros foram removidos com sucesso.")
    else:
        print("  -> Nenhum registro com descrição vazia ou com erro foi encontrado.")

    # --- Finalização ---
    conn.close()
    print("\n🎉 Processo de ajuste do banco de dados concluído com sucesso!")


if __name__ == "__main__":
    ajustar_banco_de_dados()