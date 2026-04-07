import requests
import pandas as pd
import sqlite3
from time import sleep, time
import os
from datetime import datetime
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
import re

# --- CONFIGURAÇÃO ---
ENDPOINTS_ANUAIS = ['contratos', 'dispensas_emergenciais', 'receitas_emergenciais', 'atas', 'bens_consolidado', 'bens_moveis', 'bens_imoveis', 'frota_veiculos', 'divida_ativa', 'orcamento_receitas', 'orcamento_despesas', 'ordem_cronologica_a_pagar', 'programa_acao']

def is_endpoint_anual(endpoint_name):
    return any(key in endpoint_name.lower() for key in ENDPOINTS_ANUAIS)

def normalizar_url(url):
    url = url.strip()
    if not url.startswith('http'):
        url = 'https://' + url.strip().lstrip('/')
    return url.rstrip('/')

def get_retry_session():
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=3, status_forcelist=[500, 502, 503, 504])
    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))
    
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    })
    return session

def extrair_json_de_xml(content_text):
    try:
        # Tenta remover tags XML <string...> e </string>
        if "<string" in content_text:
            # Regex para pegar tudo dentro da tag string, ignorando atributos xmlns
            match = re.search(r'<string[^>]*>(.*)</string>', content_text, re.DOTALL)
            if match:
                return json.loads(match.group(1))
        
        # Tenta limpar qualquer coisa antes do primeiro '[' ou '{'
        idx_brace = content_text.find('{')
        idx_bracket = content_text.find('[')
        
        start_idx = -1
        if idx_brace != -1 and idx_bracket != -1:
            start_idx = min(idx_brace, idx_bracket)
        elif idx_brace != -1:
            start_idx = idx_brace
        elif idx_bracket != -1:
            start_idx = idx_bracket
            
        if start_idx != -1:
            return json.loads(content_text[start_idx:])
            
    except Exception:
        pass
    return None

def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, 'data')
    bds_dir = os.path.join(base_dir, 'bds')
    logs_dir = os.path.join(base_dir, 'logs')

    endpoints_file = os.path.join(data_dir, 'endpoints_exclusivo.txt')
    prefeituras_file = os.path.join(data_dir, 'prefeiturasExclusivo.csv')
    db_file = os.path.join(bds_dir, 'ContratosSGDP.db')
    error_log_file = os.path.join(logs_dir, 'portaltp_errors.log')
    execution_log_file = os.path.join(logs_dir, 'portaltp_execution.log')
    last_run_file = os.path.join(logs_dir, 'portaltp_last_run.txt')

    os.makedirs(bds_dir, exist_ok=True)
    os.makedirs(logs_dir, exist_ok=True)

    while True:
        print("\n" + "="*50)
        print("MENU PRINCIPAL - PORTALTP DATA EXTRACTOR")
        print("="*50)
        print("1. Rodar código para um período específico")
        print("2. Rodar URLs que falharam (do arquivo de log)")
        print("3. Continuar extração desde a última data")
        print("4. Sair")

        choice = input("\nEscolha uma opção (1-4): ")

        if choice == '1':
            start_time = time()
            log_execution(execution_log_file, "Opção 1: Rodar código para período específico")
            data_inicio, data_fim = get_periodo_usuario()
            run_extraction(data_inicio, data_fim, endpoints_file, prefeituras_file, db_file, error_log_file)
            save_last_run(last_run_file, data_fim)
            log_execution_time(execution_log_file, start_time)

        elif choice == '2':
            start_time = time()
            log_execution(execution_log_file, "Opção 2: Rodar URLs que falharam")
            run_failed_urls(error_log_file, endpoints_file, prefeituras_file, db_file)
            log_execution_time(execution_log_file, start_time)

        elif choice == '3':
            start_time = time()
            log_execution(execution_log_file, "Opção 3: Continuar desde última data")
            data_inicio = get_last_run(last_run_file)
            if data_inicio is None:
                print("\n🔴 Nenhuma execução anterior encontrada. Use a opção 1 primeiro.")
                continue

            data_fim = (datetime.now().year, datetime.now().month)
            run_extraction(data_inicio, data_fim, endpoints_file, prefeituras_file, db_file, error_log_file)
            save_last_run(last_run_file, data_fim)
            log_execution_time(execution_log_file, start_time)

        elif choice == '4':
            print("\nSaindo...")
            break

        else:
            print("\n🔴 Opção inválida. Tente novamente.")

def get_periodo_usuario():
    print("\n" + "="*50)
    print("DEFINIR PERÍODO DE EXTRAÇÃO")
    print("="*50)

    while True:
        try:
            inicio = input("Data inicial (MM/AAAA): ").split('/')
            mes_inicio = int(inicio[0])
            ano_inicio = int(inicio[1])

            fim = input("Data final (MM/AAAA): ").split('/')
            mes_fim = int(fim[0])
            ano_fim = int(fim[1])

            if (ano_inicio > ano_fim) or (ano_inicio == ano_fim and mes_inicio > mes_fim):
                print("🔴 Data inicial deve ser anterior à data final. Tente novamente.")
                continue

            if mes_inicio < 1 or mes_inicio > 12 or mes_fim < 1 or mes_fim > 12:
                print("🔴 Mês inválido. Deve ser entre 1 e 12. Tente novamente.")
                continue

            return (ano_inicio, mes_inicio), (ano_fim, mes_fim)

        except (ValueError, IndexError):
            print("🔴 Formato inválido. Use MM/AAAA (ex: 01/2024). Tente novamente.")

def run_extraction(data_inicio, data_fim, endpoints_file, prefeituras_file, db_file, error_log_file):
    endpoints = load_endpoints(endpoints_file)
    prefeituras = load_prefeituras(prefeituras_file)
    prefeituras_portaltp = prefeituras[prefeituras['empresa'] == 'portaltp']

    if prefeituras_portaltp.empty:
        print("\n🔴 Nenhuma prefeitura com empresa 'portaltp' encontrada.")
        return

    session = get_retry_session()
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    for endpoint in endpoints:
        endpoint_name = endpoint.split('/')[-1].replace('Get', '').replace('json_', '').lower()
        eh_anual = is_endpoint_anual(endpoint_name)
        tipo_str = "ANUAL (mes=0)" if eh_anual else "MENSAL"
        
        print(f"\n{'='*50}\n🔧 Processando endpoint: {endpoint_name} [{tipo_str}]")

        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {endpoint_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                municipio TEXT,
                prefeitura TEXT,
                ano INTEGER,
                mes INTEGER
            )
        ''')
        conn.commit()

        for _, prefeitura in prefeituras_portaltp.iterrows():
            municipio = prefeitura['municipio']
            prefeitura_nome = prefeitura['prefeitura']
            base_url = normalizar_url(prefeitura['url'])
            
            if base_url.endswith('/api'):
                base_url = base_url[:-4]

            print(f"\n🏛️ Prefeitura: {prefeitura_nome} ({municipio})")

            anos_processados = set()

            for ano, mes in generate_months_range(data_inicio, data_fim):
                
                # Se for anual, só processa se ainda não processou este ano
                if eh_anual:
                    if ano in anos_processados:
                        continue
                    anos_processados.add(ano)
                    mes_request = 0 
                    print(f"📅 {ano} (Anual)", end=' ', flush=True)
                else:
                    mes_request = mes
                    print(f"📅 {mes:02d}/{ano}", end=' ', flush=True)

                cursor.execute(f'''
                    SELECT 1 FROM {endpoint_name} 
                    WHERE municipio = ? AND prefeitura = ? AND ano = ? AND mes = ?
                    LIMIT 1
                ''', (municipio, prefeitura_nome, ano, mes_request))

                if cursor.fetchone():
                    print("✅ Já existe no BD", end=' ')
                    continue

                url = f"{base_url}/api/transparencia.asmx/json_{endpoint}?ano={ano}&mes={mes_request:02d}"
                
                try:
                    response = session.get(url, timeout=45)
                    response.raise_for_status()
                    
                    if not response.content.strip():
                        print("🟡 Resposta vazia.", end=' ')
                        continue
                    
                    dados = None
                    # Tenta parsear JSON direto
                    try:
                        dados = response.json()
                    except ValueError:
                        # Se falhar, tenta extrair JSON de dentro do XML/String
                        dados = extrair_json_de_xml(response.text)
                        
                    if dados is None:
                         # Se ainda assim falhar, loga amostra
                        content_sample = response.text[:100].replace('\n', ' ')
                        print(f"🔴 ERRO: JSON inválido. Amostra: {content_sample}...", end=' ')
                        raise ValueError("Falha ao interpretar resposta (não é JSON nem XML encapsulado)")

                    df = pd.DataFrame(dados)

                    if not df.empty:
                        df['municipio'] = municipio
                        df['prefeitura'] = prefeitura_nome
                        df['ano'] = ano
                        df['mes'] = mes_request

                        cursor.execute(f"PRAGMA table_info({endpoint_name})")
                        existing_columns = [col[1] for col in cursor.fetchall()]

                        for column in df.columns:
                            if column not in existing_columns and column != 'id':
                                col_type = 'TEXT'
                                if pd.api.types.is_numeric_dtype(df[column]):
                                    col_type = 'REAL'
                                elif pd.api.types.is_integer_dtype(df[column]):
                                    col_type = 'INTEGER'
                                cursor.execute(f"ALTER TABLE {endpoint_name} ADD COLUMN {column} {col_type}")
                                conn.commit()

                        df.to_sql(endpoint_name, conn, if_exists='append', index=False)
                        print("✅ Dados salvos", end=' ')
                    else:
                        print("⚪ JSON vazio", end=' ')

                except Exception as e:
                    if "JSON" not in str(e):
                         print(f"🔴 ERRO: {str(e)}", end=' ')
                    
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    with open(error_log_file, 'a') as f:
                        f.write(f"{timestamp}|{url}|{type(e).__name__}|{str(e)}\n")
                sleep(1)

    conn.close()
    print("\n\n✅ EXTRAÇÃO CONCLUÍDA!")

def run_failed_urls(error_log_file, endpoints_file, prefeituras_file, db_file):
    if not os.path.exists(error_log_file):
        print("\n🔴 Nenhum arquivo de log de erros encontrado.")
        return

    failed_urls = []
    with open(error_log_file, 'r') as f:
        for line in f:
            partes = line.strip().split('|')
            if len(partes) >= 2:
                failed_urls.append(partes[1])

    if not failed_urls:
        print("\n✅ Nenhuma URL com erro para reprocessar.")
        return

    print(f"\n🔧 Reprocessando {len(failed_urls)} URLs com erro")
    session = get_retry_session()
    prefeituras = load_prefeituras(prefeituras_file)
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    success_count = 0
    temp_error_file = error_log_file + ".temp"

    for url in failed_urls:
        try:
            print(f"🔁 Tentando novamente: {url}", end=' ', flush=True)
            url = normalizar_url(url).replace('//', '/').replace('https:/', 'https://')
            url = url.replace('/api/api/', '/api/')
            
            parsed = urlparse(url)
            base_url = f"{parsed.scheme}://{parsed.netloc}"
            
            endpoint_name = parsed.path.split('/')[-1].replace('Get', '').replace('json_', '').lower()
            eh_anual = is_endpoint_anual(endpoint_name)

            query_params = parsed.query
            params_dict = dict(param.split('=', 1) for param in query_params.split('&'))
            ano = int(params_dict.get('ano', 0))
            mes = int(params_dict.get('mes', 0))

            # Se for anual e o mês estava errado/ausente, força mes=0 para tentar corrigir
            if eh_anual and mes != 0:
                base_path = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                url = f"{base_path}?ano={ano}&mes=00"
                mes = 0

            prefeitura_match = prefeituras[prefeituras['url'].str.strip().apply(normalizar_url) == base_url]
            if prefeitura_match.empty:
                 prefeitura_match = prefeituras[prefeituras['url'].str.strip().apply(lambda x: normalizar_url(x).rstrip('/api')) == base_url]

            if prefeitura_match.empty:
                print("🟡 Prefeitura não encontrada. Ignorando.")
                continue

            prefeitura = prefeitura_match.iloc[0]
            municipio = prefeitura['municipio']
            prefeitura_nome = prefeitura['prefeitura']

            response = session.get(url, timeout=60)
            response.raise_for_status()
            
            dados = None
            try:
                dados = response.json()
            except ValueError:
                dados = extrair_json_de_xml(response.text)

            if dados is None:
                 print("🔴 Falha parsing JSON/XML")
                 continue

            df = pd.DataFrame(dados)

            if not df.empty:
                df['municipio'] = municipio
                df['prefeitura'] = prefeitura_nome
                df['ano'] = ano
                df['mes'] = mes

                cursor.execute(f"PRAGMA table_info({endpoint_name})")
                existing_columns = [col[1] for col in cursor.fetchall()]

                for column in df.columns:
                    if column not in existing_columns and column != 'id':
                        col_type = 'TEXT'
                        if pd.api.types.is_numeric_dtype(df[column]):
                            col_type = 'REAL'
                        elif pd.api.types.is_integer_dtype(df[column]):
                            col_type = 'INTEGER'
                        cursor.execute(f"ALTER TABLE {endpoint_name} ADD COLUMN {column} {col_type}")
                        conn.commit()

                df.to_sql(endpoint_name, conn, if_exists='append', index=False)
                success_count += 1
                print("✅ Sucesso")
            else:
                print("⚪ Vazio (Sucesso)")

        except Exception as e:
            print(f"🔴 Falhou novamente: {str(e)}")
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with open(temp_error_file, 'a') as f:
                f.write(f"{timestamp}|{url}|{type(e).__name__}|{str(e)}\n")
        sleep(1)

    conn.close()
    os.replace(temp_error_file, error_log_file)
    print(f"\n✅ Concluído! {success_count}/{len(failed_urls)} URLs reprocessadas com sucesso.")

def generate_months_range(data_inicio, data_fim):
    meses = []
    ano_inicio, mes_inicio = data_inicio
    ano_fim, mes_fim = data_fim
    ano_atual, mes_atual = ano_inicio, mes_inicio

    while (ano_atual < ano_fim) or (ano_atual == ano_fim and mes_atual <= mes_fim):
        meses.append((ano_atual, mes_atual))
        mes_atual += 1
        if mes_atual > 12:
            mes_atual = 1
            ano_atual += 1

    return meses

def save_last_run(last_run_file, data_fim):
    with open(last_run_file, 'w') as f:
        f.write(f"{data_fim[0]},{data_fim[1]}")

def get_last_run(last_run_file):
    if not os.path.exists(last_run_file):
        return None
    with open(last_run_file, 'r') as f:
        ano, mes = map(int, f.read().strip().split(','))
        return (ano, mes)

def log_execution(log_file, message):
    timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    with open(log_file, 'a') as f:
        f.write(f"[{timestamp}] {message}\n")

def log_execution_time(log_file, start_time):
    elapsed = time() - start_time
    minutes, seconds = divmod(elapsed, 60)
    timestamp = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    with open(log_file, 'a') as f:
        f.write(f"[{timestamp}] Tempo de execução: {int(minutes)} minutos e {int(seconds)} segundos\n\n")

def load_prefeituras(filename):
    try:
        return pd.read_csv(filename)
    except Exception as e:
        print(f"\n🔴 ERRO ao ler arquivo de prefeituras: {str(e)}")
        return pd.DataFrame()

def load_endpoints(filename):
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            return [line.strip() for line in file if line.strip()]
    except Exception as e:
        print(f"\n🔴 ERRO ao ler arquivo de endpoints: {str(e)}")
        return []

if __name__ == "__main__":
    main()