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
    chrome_options.page_load_strategy = "normal" 
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
        
        print("  -> Etapa 2: Verificando paginação...")
        
        seletor_botao_xpath = "//div[contains(@id, 'DXPagerBottom')]//span[contains(@class, 'dropdown-toggle')]"
        
        try:
            # Tenta encontrar o paginador por 5 segundos. Se não achar, assume que é página única.
            wait_paginador = WebDriverWait(driver, 5)
            botao_paginador = wait_paginador.until(EC.presence_of_element_located((By.XPATH, seletor_botao_xpath)))
            
            print("     - Botão de paginação encontrado. Expandindo para 'Todos'...")
            
            print("  -> Etapa 3: Rolando a página...")
            driver.execute_script("arguments[0].scrollIntoView({block: 'center', inline: 'center'});", botao_paginador)
            time.sleep(1)

            print("  -> Etapa 4: Clicando para expandir o menu...")
            driver.execute_script("arguments[0].click();", botao_paginador)
            time.sleep(1)

            print("  -> Etapa 5: Clicando na opção 'Todos'...")
            opcao_todos = wait.until(EC.presence_of_element_located((By.XPATH, "//*[text()='Todos']")))
            driver.execute_script("arguments[0].click();", opcao_todos)
            
            print("  -> Etapa 6: Aguardando a tabela atualizar (modo 'Todos')...")
            time.sleep(10) 
            
        except TimeoutException:
            print("     - ⚠️ Nenhum paginador encontrado. Assumindo página única (poucos resultados).")
            pass
        
        # Validação simples
        id_tabela = "ctl00_containerCorpo_grdData_DXMainTable"
        try:
            wait.until(EC.presence_of_element_located((By.ID, id_tabela)))
            print("     - Elemento da tabela detectado no DOM.")
        except TimeoutException:
            print("  -> AVISO: Elemento da tabela principal não encontrado.")
            return [], 0, 0, 0

        # --- LÓGICA DE EXTRAÇÃO DOS DADOS  ---
        print("  -> Etapa 7: Extraindo HTML e processando com BeautifulSoup...")
        
        html_completo = driver.page_source
        soup_completo = BeautifulSoup(html_completo, 'lxml')
        
        # Encontrar a tabela principal
        tabela = soup_completo.find('table', {'id': id_tabela})
        if not tabela:
            print("🔴 ERRO: Tabela principal não foi encontrada no HTML extraído.")
            return [], 0, 0, 0
        
        # Encontrar linhas
        linhas = tabela.find_all('tr', class_='dxgvDataRow')
        
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
        print(f"  -> {len(documentos_existentes)} documentos já existem na pasta.")

        session = requests.Session()
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
                periodicidade = celulas[1].get_text(strip=True)
                publicado_em = celulas[2].get_text(strip=True)
                ano = celulas[3].get_text(strip=True)
                mes = celulas[4].get_text(strip=True)
                descricao = celulas[5].get_text(strip=True)
                
                url_documento = ""
                caminho_arquivo = ""
                tamanho = ""
                nome_arquivo_pdf = "" 
                
                # Procurar link
                link_tag = None
                for col_idx in [5, 6]:
                    if len(celulas) > col_idx:
                        link_tag = celulas[col_idx].find('a', href=True)
                        if link_tag:
                            break
                
                # Procurar tamanho
                for col_idx in [6, 7]:
                    if len(celulas) > col_idx:
                        tamanho_temp = celulas[col_idx].get_text(strip=True)
                        if tamanho_temp and 'MB' in tamanho_temp.upper():
                            tamanho = tamanho_temp
                            break
                
                if link_tag and link_tag.get('href'):
                    url_documento = urljoin(base_url, link_tag['href'])
                    
                    if url_documento.startswith('javascript:'):
                        metadata_list.append({
                            'id_linha': i, 'numero_arquivo': "LINK_JS", 'descricao': descricao,
                            'periodicidade': periodicidade, 'publicado_em': publicado_em, 'ano': ano, 'mes': mes,
                            'tamanho_arquivo': tamanho, 'arquivo_salvo_em': "LINK_JAVASCRIPT",
                            'url_documento': url_documento, 'data_extracao': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
                            'id_pagina': id_pagina
                        })
                        continue
                    
                    # Gera nome do arquivo
                    nome_arquivo_seguro = f"{i:03d}_{ano}_{descricao[:40].replace('/', '_').replace(':', '').replace(' ', '_')}.pdf"
                    nome_arquivo_seguro = "".join(c for c in nome_arquivo_seguro if c.isalnum() or c in '._-')
                    
                    nome_arquivo_pdf = nome_arquivo_seguro # Variável para salvar no banco
                    caminho_arquivo = os.path.join(pasta_id, nome_arquivo_seguro)
                    
                    if nome_arquivo_seguro in documentos_existentes:
                        documentos_pulados += 1
                    else:
                        try:
                            # Log de progresso a cada 10 arquivos
                            if i % 10 == 0 or i == 1:
                                print(f"    Processando linha {i}/{len(linhas)}...")
                                
                            doc_response = session.get(url_documento, timeout=60)
                            
                            if doc_response.status_code == 200 and len(doc_response.content) > 0:
                                with open(caminho_arquivo, 'wb') as f:
                                    f.write(doc_response.content)
                                documentos_baixados += 1
                            else:
                                print(f"      ⚠️ Falha download ({doc_response.status_code}): {descricao[:30]}")
                                caminho_arquivo = f"ERRO: Status {doc_response.status_code}"
                                documentos_com_erro += 1
                            
                        except Exception as e:
                            print(f"      🔴 Erro download: {e}")
                            caminho_arquivo = f"ERRO: {e}"
                            documentos_com_erro += 1
                
                metadata_list.append({
                    'id_linha': i,
                    'numero_arquivo': nome_arquivo_pdf, 
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
        id_pagina INTEGER,
        prefeitura TEXT
    )
    """)
    conn.commit()
    conn.close()
    print(f"✅ Tabela '{nome_tabela}' verificada.")

def processar_lista_ids(lista_ids, base_url, pasta_downloads, caminho_banco, nome_tabela):
    criar_banco_dados(caminho_banco, nome_tabela)
    
    estatisticas_totais = {
        'ids_processados': 0,
        'ids_com_erro': 0,
        'total_documentos_baixados': 0,
        'total_documentos_pulados': 0,
        'total_documentos_com_erro': 0
    }
    
    nome_prefeitura = base_url.split('//')[1].split('-')[0]

    conn = sqlite3.connect(caminho_banco)
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT DISTINCT id_pagina FROM {nome_tabela} WHERE prefeitura = '{nome_prefeitura}'")
        ids_processados = set([row[0] for row in cursor.fetchall()])
    except Exception as e:
        print(f"Aviso na verificação de IDs: {e}")
        ids_processados = set()
    conn.close()
    
    for id_pagina in lista_ids:
        print(f"\n{'='*80}\n🔄 PROCESSANDO ID: {id_pagina}\n{'='*80}")
        
        if id_pagina in ids_processados:
            print(f"⏭️  ID {id_pagina} já foi processado. Pulando...")
            estatisticas_totais['ids_processados'] += 1
            continue
        
        url_pagina = f"https://Viana-es.portaltp.com.br/consultas/documentos.aspx?id={id_pagina}"
        
        try:
            dados, baixados, pulados, erros = extrair_e_baixar_documentos_com_selenium(
                url_pagina, base_url, pasta_downloads, caminho_banco, id_pagina
            )
            
            if dados:
                print(f"💾 Salvando {len(dados)} registros no banco...")
                df = pd.DataFrame(dados)
                
                df['prefeitura'] = nome_prefeitura
                
                conn = sqlite3.connect(caminho_banco)
                df.to_sql(nome_tabela, conn, if_exists="append", index=False)
                conn.close()
                
                estatisticas_totais['ids_processados'] += 1
                estatisticas_totais['total_documentos_baixados'] += baixados
                estatisticas_totais['total_documentos_pulados'] += pulados
                estatisticas_totais['total_documentos_com_erro'] += erros
            else:
                estatisticas_totais['ids_com_erro'] += 1
                
        except Exception as e:
            print(f"🔴 Erro crítico ID {id_pagina}: {e}")
            estatisticas_totais['ids_com_erro'] += 1
        
        time.sleep(3)
    
    return estatisticas_totais

def main():
    BASE_URL = "https://Viana-es.portaltp.com.br/"
    LISTA_IDS = [
    1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 33, 34, 41, 55, 57, 59, 60, 61, 76, 77, 
    81, 99, 104, 123, 132, 136, 138, 147, 155, 156, 160, 162, 180, 222, 
    229, 239, 371, 379, 399, 454, 458, 475, 478, 492, 493, 512, 536, 539, 
    548, 549, 557, 558, 561, 576, 605, 606, 612, 613, 656, 660, 715, 725, 
    791, 800, 824, 837, 844, 914, 944, 966, 980, 986, 989, 990, 991, 993, 
    994, 995, 998, 1004, 1009, 1012, 1029, 1037, 1047, 1050, 1099, 1101, 
    1117, 1139, 1181, 1221, 1226, 1227, 1252, 1257, 1258, 1260, 1261, 1262, 
    1263, 1269, 1272, 1314, 1342, 1344, 1391, 1393, 1398, 1399, 1403, 1415, 
    1416, 1440, 1445, 1456, 1462, 1468, 1543, 1583, 1584, 1614, 1617, 1866, 
    1870, 1879, 1945, 1951, 1954, 1994, 1995, 2008, 2025, 2042, 2050, 2150, 
    2152, 2171, 2182, 2195, 2326, 2328, 2394, 2425, 3183, 3282
]
    PASTA_DOWNLOADS = "documentos_convenios" 
    PASTA_BDS = "bds"  
    NOME_BANCO = "bdTCC.db"  
    NOME_TABELA_DB = "convenios"
    CAMINHO_BANCO_COMPLETO = os.path.join(PASTA_BDS, NOME_BANCO)

    nome_prefeitura = BASE_URL.split('//')[1].split('-')[0]
    
    estatisticas = processar_lista_ids(LISTA_IDS, BASE_URL, PASTA_DOWNLOADS, CAMINHO_BANCO_COMPLETO, NOME_TABELA_DB)
    
    print(f"\n{'='*80}\n📊 RELATÓRIO FINAL\n{'='*80}")
    print(f"✅ Sucesso: {estatisticas['ids_processados']} | ❌ Erros: {estatisticas['ids_com_erro']}")
    print(f"📥 Baixados: {estatisticas['total_documentos_baixados']} | ⏭️ Pulados: {estatisticas['total_documentos_pulados']}")

if __name__ == "__main__":
    main()