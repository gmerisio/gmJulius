import requests
import pandas as pd
import sqlite3
from time import sleep, time
import os
from datetime import datetime
from urllib.parse import urlparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def normalizar_url(url):
    url = url.strip()
    if not url.startswith('http'):
        url = 'https://' + url.strip().lstrip('/')
    return url.rstrip('/')

def get_retry_session():
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
    session.mount('http://', HTTPAdapter(max_retries=retries))
    session.mount('https://', HTTPAdapter(max_retries=retries))
    
    # Adiciona um cabeçalho padrão de navegador
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
    })
    
    return session

def obter_anos_disponiveis(session, base_url, endpoint_name, ano_inicio_padrao, ano_fim_padrao):
    # Trata sub-rotas como 'pessoal/mensal' pegando apenas a raiz 'pessoal'
    endpoint_raiz = endpoint_name.split('/')[0]
    url_anos = f"{base_url}/api/{endpoint_raiz}/anos"
    
    try:
        res = session.get(url_anos, timeout=10)
        if res.status_code == 200 and res.content.strip():
            dados = res.json()
            anos_encontrados = []
            
            if isinstance(dados, list):
                for item in dados:
                    if isinstance(item, dict):
                        # Pega o primeiro valor numérico de 4 dígitos encontrado no dict
                        for v in item.values():
                            if isinstance(v, int) and 1990 <= v <= 2030:
                                anos_encontrados.append(v)
                            elif isinstance(v, str) and v.isdigit() and 1990 <= int(v) <= 2030:
                                anos_encontrados.append(int(v))
                    elif isinstance(item, (int, str)) and str(item).isdigit():
                        anos_encontrados.append(int(item))
            
            if anos_encontrados:
                return sorted(list(set(anos_encontrados)))
    except Exception:
        pass
    
    # Se a rota /anos não existir para este assunto, usa o intervalo definido pelo usuário
    return list(range(ano_inicio_padrao, ano_fim_padrao + 1))


def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.join(base_dir, 'data')
    bds_dir = os.path.join(base_dir, 'bds')
    logs_dir = os.path.join(base_dir, 'logs')

    assuntos_file = os.path.join(data_dir, 'assuntos_tectrilha.csv')  # Agora usamos o CSV de assuntos
    prefeituras_file = os.path.join(data_dir, 'prefeituras.csv')
    db_file = os.path.join(bds_dir, 'tectrilha.db')
    error_log_file = os.path.join(logs_dir, 'tectrilha_errors.log')
    execution_log_file = os.path.join(logs_dir, 'tectrilha_execution.log')
    last_run_file = os.path.join(logs_dir, 'tectrilha_last_run.txt')

    os.makedirs(bds_dir, exist_ok=True)
    os.makedirs(logs_dir, exist_ok=True)

    while True:
        print("\n" + "="*50)
        print("MENU PRINCIPAL - TECTRILHA DATA EXTRACTOR")
        print("="*50)
        print("1. Rodar código para um período específico")
        print("2. Rodar URLs que falharam (do arquivo de log)")
        print("3. Continuar extração desde o último ano")
        print("4. Sair")

        choice = input("\nEscolha uma opção (1-4): ")

        if choice == '1':
            start_time = time()
            log_execution(execution_log_file, "Opção 1: Rodar código para período específico")
            ano_inicio, ano_fim = get_periodo_usuario()
            run_extraction(ano_inicio, ano_fim, assuntos_file, prefeituras_file, db_file, error_log_file)
            save_last_run(last_run_file, ano_fim)
            log_execution_time(execution_log_file, start_time)

        elif choice == '2':
            start_time = time()
            log_execution(execution_log_file, "Opção 2: Rodar URLs que falharam")
            run_failed_urls(error_log_file, assuntos_file, prefeituras_file, db_file)
            log_execution_time(execution_log_file, start_time)

        elif choice == '3':
            start_time = time()
            log_execution(execution_log_file, "Opção 3: Continuar desde último ano")
            last_year = get_last_run(last_run_file)
            if last_year is None:
                print("\n🔴 Nenhuma execução anterior encontrada. Use a opção 1 primeiro.")
                continue

            current_year = datetime.now().year
            run_extraction(last_year + 1, current_year, assuntos_file, prefeituras_file, db_file, error_log_file)
            save_last_run(last_run_file, current_year)
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
            ano_inicio = int(input("Ano inicial (AAAA): "))
            ano_fim = int(input("Ano final (AAAA): "))

            if ano_inicio > ano_fim:
                print("🔴 Ano inicial deve ser anterior ou igual ao ano final. Tente novamente.")
                continue

            current_year = datetime.now().year
            if ano_inicio < 2000 or ano_inicio > current_year or ano_fim < 2000 or ano_fim > current_year:
                print(f"🔴 Ano inválido. Deve ser entre 2000 e {current_year}. Tente novamente.")
                continue

            return ano_inicio, ano_fim

        except ValueError:
            print("🔴 Formato inválido. Use AAAA (ex: 2024). Tente novamente.")

def run_extraction(ano_inicio, ano_fim, assuntos_file, prefeituras_file, db_file, error_log_file):
    assuntos = load_assuntos(assuntos_file)
    prefeituras = load_prefeituras(prefeituras_file)
    prefeituras_tectrilha = prefeituras[prefeituras['empresa'] == 'tectrilha']

    if prefeituras_tectrilha.empty:
        print("\n🔴 Nenhuma prefeitura com empresa 'tectrilha' encontrada.")
        return

    session = get_retry_session()
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    for _, assunto in assuntos.iterrows():
        endpoint_name = assunto['assunto']
        table_name = endpoint_name.replace('/', '_').replace('-', '_')
        parametros = assunto['parametros'].strip() if pd.notna(assunto['parametros']) else ""
        
        print(f"\n{'='*50}\n🔧 Processando endpoint: {endpoint_name}")

        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                municipio TEXT,
                prefeitura TEXT,
                unidadegestora TEXT,
                ano INTEGER
            )
        ''')
        conn.commit()

        for _, prefeitura in prefeituras_tectrilha.iterrows():
            municipio = prefeitura['municipio']
            prefeitura_nome = prefeitura['prefeitura']
            unidade_gestora = str(int(prefeitura['unidadegestora']))
            base_url = normalizar_url(prefeitura['url']).rstrip('/api')
            print(f"\n🏛️ Prefeitura: {prefeitura_nome} ({municipio}) - UG: {unidade_gestora}")

            # Busca dinâmica dos anos disponíveis via API
            anos_disponiveis = obter_anos_disponiveis(session, base_url, endpoint_name, ano_inicio, ano_fim)
            anos_para_processar = [a for a in anos_disponiveis if ano_inicio <= a <= ano_fim]

            if not anos_para_processar:
                print(" ⚠️ Nenhum ano com dados disponível no período selecionado.")
                continue

            for ano in anos_para_processar:
                print(f"📅 {ano}", end=' ', flush=True)
                cursor.execute(f'''
                    SELECT 1 FROM {table_name} 
                    WHERE municipio = ? AND prefeitura = ? AND unidadegestora = ? AND ano = ?
                    LIMIT 1
                ''', (municipio, prefeitura_nome, unidade_gestora, ano))

                if cursor.fetchone():
                    print("✅ Já existe no BD", end=' ')
                    continue

                url_params = parametros.format(
                    unidadeGestoraId=unidade_gestora,
                    exercicio=ano,
                    periodo=""
                ).strip()

                if url_params:
                    if not url_params.startswith('?'):
                        url_params = '?' + url_params
                    url_params = url_params.replace(' ', '')

                url = f"{base_url}/api/{endpoint_name}{url_params}"
                try:
                    response = session.get(url, timeout=30)
                    response.raise_for_status()
                    if not response.content.strip():
                        print("🟡 Resposta vazia. Ignorando.", end=' ')
                        continue
                    dados = response.json()
                    df = pd.DataFrame(dados)

                    if not df.empty:
                        df = df.drop(columns=['municipio', 'prefeitura', 'unidadegestora', 'ano'], errors='ignore')

                        df['municipio'] = municipio
                        df['prefeitura'] = prefeitura_nome
                        df['unidadegestora'] = unidade_gestora
                        df['ano'] = ano

                        cursor.execute(f"PRAGMA table_info({table_name})")
                        existing_columns = [col[1] for col in cursor.fetchall()]

                        for column in df.columns:
                            if column.lower() not in [col.lower() for col in existing_columns] and column != 'id':
                                col_type = 'TEXT'
                                if pd.api.types.is_numeric_dtype(df[column]):
                                    col_type = 'REAL'
                                elif pd.api.types.is_integer_dtype(df[column]):
                                    col_type = 'INTEGER'
                                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column} {col_type}")
                                conn.commit()

                        df.to_sql(table_name, conn, if_exists='append', index=False)
                        print("✅ Dados salvos", end=' ')

                except Exception as e:
                    print(f"🔴 ERRO: {str(e)}", end=' ')
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    with open(error_log_file, 'a') as f:
                        f.write(f"{timestamp}|{url}|{type(e).__name__}|{str(e)}\n")
                sleep(1)

    conn.close()
    print("\n\n✅ EXTRAÇÃO CONCLUÍDA!")

def run_failed_urls(error_log_file, assuntos_file, prefeituras_file, db_file):
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
    assuntos = load_assuntos(assuntos_file)
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    success_count = 0
    temp_error_file = error_log_file + ".temp"

    for url in failed_urls:
        try:
            print(f"🔁 Tentando novamente: {url}", end=' ', flush=True)
            url = normalizar_url(url).replace('//', '/').replace('https:/', 'https://')
            parsed = urlparse(url)
            base_url = f"{parsed.scheme}://{parsed.netloc}"
            
            if '?' not in url:
                print("🟡 URL sem parâmetros. Ignorando.")
                continue

            # Extrai o endpoint_name do path (parte após /api/)
            path_parts = parsed.path.split('/')
            if 'api' in path_parts:
                api_index = path_parts.index('api')
                if api_index + 1 < len(path_parts):
                    endpoint_name = path_parts[api_index + 1].split('?')[0]
                else:
                    print("🟡 URL malformada. Ignorando.")
                    continue
            else:
                print("🟡 URL não contém 'api'. Ignorando.")
                continue

            query_params = parsed.query
            params_dict = dict(param.split('=', 1) for param in query_params.split('&') if '=' in param)
            
            ano = int(params_dict.get('exercicio', 0))
            unidade_gestora = params_dict.get('unidadegestoraId', '')

            prefeitura_match = prefeituras[
                (prefeituras['url'].str.strip().apply(normalizar_url) == base_url) & 
                (prefeituras['unidadegestora'] == unidade_gestora)
            ]
            
            if prefeitura_match.empty:
                print("🟡 Prefeitura não encontrada. Ignorando.")
                continue

            prefeitura = prefeitura_match.iloc[0]
            municipio = prefeitura['municipio']
            prefeitura_nome = prefeitura['prefeitura']

            response = session.get(url, timeout=60)
            response.raise_for_status()
            if not response.content.strip():
                print("🟡 Resposta vazia. Ignorando.")
                continue

            dados = response.json()
            df = pd.DataFrame(dados)

            if not df.empty:
                df['municipio'] = municipio
                df['prefeitura'] = prefeitura_nome
                df['unidadegestora'] = unidade_gestora
                df['ano'] = ano

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

        except Exception as e:
            print(f"🔴 Falhou novamente: {str(e)}")
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with open(temp_error_file, 'a') as f:
                f.write(f"{timestamp}|{url}|{type(e).__name__}|{str(e)}\n")
        sleep(1)

    conn.close()
    os.replace(temp_error_file, error_log_file)
    print(f"\n✅ Concluído! {success_count}/{len(failed_urls)} URLs reprocessadas com sucesso.")

def save_last_run(last_run_file, ano):
    with open(last_run_file, 'w') as f:
        f.write(str(ano))

def get_last_run(last_run_file):
    if not os.path.exists(last_run_file):
        return None
    with open(last_run_file, 'r') as f:
        return int(f.read().strip())

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

def load_assuntos(filename):
    try:
        df = pd.read_csv(filename)
        # Garante que os parâmetros sejam strings e trata valores NaN
        df['parametros'] = df['parametros'].fillna('').astype(str)
        return df
    except Exception as e:
        print(f"\n🔴 ERRO ao ler arquivo de assuntos: {str(e)}")
        return pd.DataFrame(columns=['assunto', 'parametros'])

if __name__ == "__main__":
    main()