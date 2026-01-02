import os
import time
import requests
import pandas as pd
import sqlite3
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# --- Importações do Selenium ---
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

def extrair_e_baixar_documentos_com_selenium(url_pagina, base_url, pasta_downloads, banco_de_dados, id_pagina):

    print(f"📄 Iniciando processo com Selenium para a página: {url_pagina}")
    
    # Criar pasta específica para o ID
    pasta_id = os.path.join(pasta_downloads, f"id_{id_pagina}")
    os.makedirs(pasta_id, exist_ok=True)
    
    metadata_list = []

    service = Service(ChromeDriverManager().install())
    chrome_options = Options()
    chrome_options.page_load_strategy = "normal" # Alterado para normal para garantir carregamento
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")

    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    try:
        # --- LÓGICA DE INTERAÇÃO ---
        print("  -> Etapa 1: Abrindo a página no navegador...")
        driver.get(url_pagina)
        driver.maximize_window()
        wait = WebDriverWait(driver, 30)
        
        try:
            wait.until(EC.frame_to_be_available_and_switch_to_it((By.ID, "dados")))
            print("  -> Contexto do driver mudou para o iframe 'dados'.")
        except TimeoutException:
            print("  -> Iframe 'dados' não encontrado, continuando na página principal.")
            pass
        
        print("  -> Etapa 2: Procurando o botão de expansão do paginador...")
        seletor_botao_xpath = "//div[contains(@id, 'DXPagerBottom')]//span[contains(@class, 'dropdown-toggle')]"
        botao_paginador = wait.until(EC.presence_of_element_located((By.XPATH, seletor_botao_xpath)))
        
        print("  -> Etapa 3: Rolando a página para centralizar o botão...")
        driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'center'});", botao_paginador)
        time.sleep(2)

        print("  -> Etapa 4: Clicando para expandir o menu (via JavaScript)...")
        driver.execute_script("arguments[0].click();", botao_paginador)
        time.sleep(2)

        print("  -> Etapa 5: Procurando e clicando na opção 'Todos'...")
        opcao_todos = wait.until(EC.presence_of_element_located((By.XPATH, "//*[text()='Todos']")))
        driver.execute_script("arguments[0].click();", opcao_todos)
        
        print("  -> Etapa 6: Aguardando a tabela atualizar...")
        
        time.sleep(10) # Aumentado levemente para garantir o render completo
        
        # Validação simples: verificar se a tabela principal está no DOM, sem iterar linhas
        id_tabela = "ctl00_containerCorpo_grdData_DXMainTable"
        try:
            wait.until(EC.presence_of_element_located((By.ID, id_tabela)))
            print("     - Elemento da tabela detectado no DOM.")
        except TimeoutException:
            print("  -> AVISO: Elemento da tabela principal não encontrado.")
            return [], 0, 0, 0

        # --- LÓGICA DE EXTRAÇÃO DOS DADOS  ---
        print("  -> Etapa 7: Extraindo HTML e processando com BeautifulSoup...")
        
        # Pega o HTML snapshot agora. Se algo for "stale" depois disso, não importa.
        html_completo = driver.page_source
        soup_completo = BeautifulSoup(html_completo, 'lxml')
        
        # Encontrar a tabela principal
        tabela = soup_completo.find('table', {'id': id_tabela})
        if not tabela:
            print("🔴 ERRO: Tabela principal não foi encontrada no HTML extraído.")
            return [], 0, 0, 0
        
        # Encontrar TODAS as linhas de dados pela classe do DevExpress
        linhas = tabela.find_all('tr', class_='dxgvDataRow')
        
        # Fallback: Se não encontrar pelas classes específicas, pegar genérico
        if not linhas:
            linhas = [tr for tr in tabela.find_all('tr') if len(tr.find_all('td')) >= 5]
        
        print(f"  -> Encontradas {len(linhas)} linhas de dados para processar")
        
        if not linhas:
            print("🟡 AVISO: Nenhuma linha com dados encontrada na tabela.")
            return [], 0, 0, 0

        # --- VERIFICAR DOCUMENTOS JÁ BAIXADOS ---
        documentos_existentes = set()
        if os.path.exists(pasta_id):
            for arquivo in os.listdir(pasta_id):
                if arquivo.endswith('.pdf'):
                    documentos_existentes.add(arquivo)
        print(f"  -> {len(documentos_existentes)} documentos já existem na pasta id_{id_pagina}")

        # Processar cada linha da tabela
        session = requests.Session()
        # Adicionar headers para parecer um browser real no requests
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })

        documentos_baixados = 0
        documentos_pulados = 0
        documentos_com_erro = 0
        
        for i, linha in enumerate(linhas, 1):
            celulas = linha.find_all('td')
            
            if len(celulas) < 6: 
                continue
            
            try:
                # Extrair dados
                numero_arquivo = celulas[0].get_text(strip=True)
                periodicidade = celulas[1].get_text(strip=True)
                publicado_em = celulas[2].get_text(strip=True)
                ano = celulas[3].get_text(strip=True)
                mes = celulas[4].get_text(strip=True)
                descricao = celulas[5].get_text(strip=True)
                
                # Procurar link de download
                link_tag = None
                for col_idx in [5, 6]:
                    if len(celulas) > col_idx:
                        link_tag = celulas[col_idx].find('a', href=True)
                        if link_tag:
                            break
                
                url_documento = ""
                caminho_arquivo = ""
                tamanho = ""
                
                # Tentar pegar tamanho
                for col_idx in [6, 7]:
                    if len(celulas) > col_idx:
                        tamanho_temp = celulas[col_idx].get_text(strip=True)
                        if tamanho_temp and 'MB' in tamanho_temp.upper():
                            tamanho = tamanho_temp
                            break
                
                if link_tag and link_tag.get('href'):
                    url_documento = urljoin(base_url, link_tag['href'])
                    
                    if url_documento.startswith('javascript:'):
                        # Logica para Javascript links (ignorar por enquanto ou implementar postback)
                        metadata_list.append({
                            'id_linha': i, 'numero_arquivo': numero_arquivo, 'descricao': descricao,
                            'periodicidade': periodicidade, 'publicado_em': publicado_em, 'ano': ano, 'mes': mes,
                            'tamanho_arquivo': tamanho, 'arquivo_salvo_em': "LINK_JAVASCRIPT",
                            'url_documento': url_documento, 'data_extracao': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
                            'id_pagina': id_pagina
                        })
                        continue
                    
                    nome_arquivo_seguro = f"{i:03d}_{ano}_{descricao[:40].replace('/', '_').replace(':', '').replace(' ', '_')}.pdf"
                    nome_arquivo_seguro = "".join(c for c in nome_arquivo_seguro if c.isalnum() or c in '._-')
                    caminho_arquivo = os.path.join(pasta_id, nome_arquivo_seguro)
                    
                    if nome_arquivo_seguro in documentos_existentes:
                        print(f"    ({i}/{len(linhas)}) ⏭️  Já existe: {descricao[:40]}...")
                        documentos_pulados += 1
                    else:
                        try:
                            print(f"    ({i}/{len(linhas)}) 📥 Baixando: {descricao[:40]}...")
                            doc_response = session.get(url_documento, timeout=30)
                            
                            if doc_response.status_code == 200 and len(doc_response.content) > 0:
                                with open(caminho_arquivo, 'wb') as f:
                                    f.write(doc_response.content)
                                documentos_baixados += 1
                            else:
                                print(f"      ⚠️ Falha download. Status: {doc_response.status_code}")
                                caminho_arquivo = f"ERRO: Status {doc_response.status_code}"
                                documentos_com_erro += 1
                            
                        except Exception as e:
                            print(f"      🔴 Erro download: {e}")
                            caminho_arquivo = f"ERRO: {e}"
                            documentos_com_erro += 1
                
                metadata_list.append({
                    'id_linha': i,
                    'numero_arquivo': numero_arquivo,
                    'descricao': descricao,
                    'periodicidade': periodicidade,
                    'publicado_em': publicado_em,
                    'ano': ano,
                    'mes': mes,
                    'tamanho_arquivo': tamanho,
                    'arquivo_salvo_em': caminho_arquivo,
                    'url_documento': url_documento,
                    'data_extracao': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'id_pagina': id_pagina
                })

            except Exception as e:
                print(f"      🔴 Erro na linha {i}: {e}")
                documentos_com_erro += 1
                continue

        print(f"✅ Extração finalizada! Baixados: {documentos_baixados}, Pulados: {documentos_pulados}, Erros: {documentos_com_erro}")
        return metadata_list, documentos_baixados, documentos_pulados, documentos_com_erro

    except Exception as e:
        print(f"🔴 Erro crítico: {e}")
        return [], 0, 0, 0
    finally:
        print("  -> Fechando navegador.")
        if 'driver' in locals():
            driver.quit()

def criar_banco_dados(caminho_banco, nome_tabela):
    """Cria a estrutura do banco de dados SQLite"""
    # Garantir que a pasta bds existe
    os.makedirs(os.path.dirname(caminho_banco), exist_ok=True)
    
    conn = sqlite3.connect(caminho_banco)
    cursor = conn.cursor()
    
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {nome_tabela} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        id_linha INTEGER,
        numero_arquivo TEXT,
        descricao TEXT,
        periodicidade TEXT,
        publicado_em TEXT,
        ano INTEGER,
        mes TEXT,
        tamanho_arquivo TEXT,
        arquivo_salvo_em TEXT,
        url_documento TEXT,
        data_extracao TEXT,
        id_pagina INTEGER
    )
    """)
    
    conn.commit()
    conn.close()
    print(f"✅ Tabela '{nome_tabela}' criada/verificada no banco de dados: {caminho_banco}")

def processar_lista_ids(lista_ids, base_url, pasta_downloads, caminho_banco, nome_tabela):
    """Processa uma lista de IDs sequencialmente"""
    
    # Criar estrutura do banco de dados
    criar_banco_dados(caminho_banco, nome_tabela)
    
    # Estatísticas totais
    estatisticas_totais = {
        'total_ids': len(lista_ids),
        'ids_processados': 0,
        'ids_com_erro': 0,
        'total_documentos_baixados': 0,
        'total_documentos_pulados': 0,
        'total_documentos_com_erro': 0
    }
    
    # Conectar ao banco para verificar IDs já processados
    conn = sqlite3.connect(caminho_banco)
    cursor = conn.cursor()
    
    # Verificar quais IDs já foram processados
    cursor.execute(f"SELECT DISTINCT id_pagina FROM {nome_tabela}")
    ids_processados = set([row[0] for row in cursor.fetchall()])
    conn.close()
    
    print(f"📊 IDs já processados anteriormente: {ids_processados}")
    
    for id_pagina in lista_ids:
        print(f"\n{'='*80}")
        print(f"🔄 PROCESSANDO ID: {id_pagina}")
        print(f"{'='*80}")
        
        # Verificar se já foi processado
        if id_pagina in ids_processados:
            print(f"⏭️  ID {id_pagina} já foi processado anteriormente. Pulando...")
            estatisticas_totais['ids_processados'] += 1
            continue
        
        # Construir URL para o ID atual
        url_pagina = f"https://saogabrieldapalha-es.portaltp.com.br/consultas/documentos.aspx?id={id_pagina}"
        
        try:
            # Extrair dados para o ID atual
            dados_coletados, baixados, pulados, erros = extrair_e_baixar_documentos_com_selenium(
                url_pagina, base_url, pasta_downloads, caminho_banco, id_pagina
            )
            
            if dados_coletados:
                print(f"\n💾 Salvando metadados do ID {id_pagina} no banco de dados...")
                df = pd.DataFrame(dados_coletados)
                
                # Salvar no SQLite (append para adicionar à tabela existente)
                try:
                    conn = sqlite3.connect(caminho_banco)
                    df.to_sql(nome_tabela, conn, if_exists="append", index=False)
                    conn.close()
                    print(f"  -> Dados do ID {id_pagina} salvos no banco de dados")
                    
                    # Atualizar estatísticas
                    estatisticas_totais['ids_processados'] += 1
                    estatisticas_totais['total_documentos_baixados'] += baixados
                    estatisticas_totais['total_documentos_pulados'] += pulados
                    estatisticas_totais['total_documentos_com_erro'] += erros
                    
                except Exception as e:
                    print(f"  🔴 Erro ao salvar dados do ID {id_pagina} no banco: {e}")
                    estatisticas_totais['ids_com_erro'] += 1
            else:
                print(f"  ⚠️  Nenhum dado extraído para o ID {id_pagina}")
                estatisticas_totais['ids_com_erro'] += 1
                
        except Exception as e:
            print(f"🔴 Erro crítico ao processar ID {id_pagina}: {e}")
            estatisticas_totais['ids_com_erro'] += 1
        
        # Pequena pausa entre processamentos para não sobrecarregar o servidor
        print("⏳ Aguardando 3 segundos antes do próximo ID...")
        time.sleep(3)
    
    return estatisticas_totais

def main():
    # --- CONFIGURAÇÕES ---
    BASE_URL = "https://saogabrieldapalha-es.portaltp.com.br/"
    
    # Lista completa de IDs para processar
    LISTA_IDS = [8]
    # LISTA_IDS = [
       # 136, 143, 156, 132, 222, 8, 9, 41, 1012, 77, 55, 219, 34, 371, 631, 
       # 153, 76, 311, 33, 106, 248, 481, 104, 458, 881, 1, 2, 3, 4, 5, 7, 
       # 6, 60, 590, 64, 1390, 1342, 39, 555, 1255, 99, 57, 56, 63, 58, 59, 
       # 36, 61, 1074, 1241, 230, 2019, 557, 558, 514, 515, 914, 547, 1346, 
       # 39, 656, 747, 2172, 272, 277, 454, 605, 606, 611, 868, 874, 1307, 2167
    #]
    
    # Organização das pastas conforme estrutura
    PASTA_DOWNLOADS = "documentos_convenios" 
    PASTA_BDS = "bds"  
    NOME_BANCO = "convenios.db"  
    NOME_TABELA_DB = "convenios"
    
    # Caminhos completos
    CAMINHO_BANCO_COMPLETO = os.path.join(PASTA_BDS, NOME_BANCO)
    
    print("⚙️  Configurações:")
    print(f"  - Pasta base de downloads: {PASTA_DOWNLOADS}")
    print(f"  - Banco de dados: {CAMINHO_BANCO_COMPLETO}")
    print(f"  - Tabela: {NOME_TABELA_DB}")
    print(f"  - Total de IDs para processar: {len(LISTA_IDS)}")
    
    # Processar todos os IDs
    estatisticas = processar_lista_ids(LISTA_IDS, BASE_URL, PASTA_DOWNLOADS, CAMINHO_BANCO_COMPLETO, NOME_TABELA_DB)
    
    # Relatório final
    print(f"\n{'='*80}")
    print("📊 RELATÓRIO FINAL")
    print(f"{'='*80}")
    print(f"✅ IDs processados com sucesso: {estatisticas['ids_processados']}")
    print(f"❌ IDs com erro: {estatisticas['ids_com_erro']}")
    print(f"📥 Total de documentos baixados: {estatisticas['total_documentos_baixados']}")
    print(f"⏭️  Total de documentos pulados: {estatisticas['total_documentos_pulados']}")
    print(f"⚠️  Total de documentos com erro: {estatisticas['total_documentos_com_erro']}")
    print(f"{'='*80}")

if __name__ == "__main__":
    main()