import fitz  # PyMuPDF
import os
import glob
import shutil
import sqlite3
import re
import duckdb
import pandas as pd
from tqdm import tqdm

# NOVAS IMPORTAÇÕES PARA OCR
try:
    import pytesseract
    from PIL import Image
    from io import BytesIO

    pytesseract.pytesseract.tesseract_cmd = r'C:/Program Files/Tesseract-OCR/tesseract.exe'
    TESSERACT_DISPONIVEL = True
except ImportError:
    print("⚠️ Aviso: pytesseract e/ou Pillow não encontrados. Apenas a extração direta funcionará.")
    TESSERACT_DISPONIVEL = False
except Exception as e:
    print(f"⚠️ Aviso: Erro ao configurar o Tesseract: {e}")
    TESSERACT_DISPONIVEL = False

# --- CONFIGURAÇÃO ---

PASTA_RAIZ_DOCUMENTOS = r"C:/Users/gmeri/OneDrive/Área de Trabalho/Pasta Pessoal/Trabalho/gmJulius\documentos_convenios"

# Caminho para o banco de dados que tem as informações (metadados)
CAMINHO_BD_SQLITE_ORIGEM = r"C:/Users/gmeri/OneDrive/Área de Trabalho/Pasta Pessoal/Trabalho/gmJulius/bds/convenios.db"

# Nome da tabela dentro deste banco SQLite
TABELA_SQLITE_ORIGEM = "convenios"

# Nome da coluna no SQLite que contém o NOME DO ARQUIVO PDF
COLUNA_LIGACAO_SQLITE = "numero_arquivo" 
# ----------------------------------------------------------------------

# --- CONFIGURAÇÃO NOVA ---
# Caminho para o arquivo DuckDB que será criado/alimentado
CAMINHO_DUCKDB_FINAL = r"C:/Users/gmeri/OneDrive/Área de Trabalho/Pasta Pessoal/Trabalho/gmJulius/bds/convenios.duckdb"

# Nome da tabela a ser criada no DuckDB
TABELA_DUCKDB_DESTINO = "convenios"
# -----------------------------------------------


def extrair_texto_pdf(caminho_pdf):
    texto_total = []
    
    # TENTATIVA DIRETA
    try:
        with fitz.open(caminho_pdf) as pdf:
            for pagina in pdf:
                texto_pagina = pagina.get_text("text").strip()
                if texto_pagina:
                    texto_total.append(texto_pagina)
        
        texto_combinado = "\n".join(texto_total).strip()
        if len(texto_combinado) > 50:
            return "SUCESSO_DIRETO", texto_combinado
    except Exception as e:
        pass # Falha silenciosa para tentar OCR

    # FALLBACK OCR
    if TESSERACT_DISPONIVEL:
        try:
            texto_ocr = []
            with fitz.open(caminho_pdf) as pdf:
                for num_pagina, pagina in enumerate(pdf):
                    pix = pagina.get_pixmap(matrix=fitz.Matrix(300/72, 300/72))
                    img = Image.open(BytesIO(pix.tobytes("png")))
                    
                    config_ocr = '-l por+eng --oem 3 --psm 3'
                    texto = pytesseract.image_to_string(img, config=config_ocr).strip()
                    if texto:
                        texto_ocr.append(f"--- Pág {num_pagina + 1} ---\n{texto}")

            texto_combinado_ocr = "\n".join(texto_ocr).strip()
            if len(texto_combinado_ocr) > 50:
                return "SUCESSO_OCR", texto_combinado_ocr
        except Exception as e:
            print(f"\n🚨 Erro no OCR: {e}")

    return "FALHA_TOTAL", ""


def buscar_metadados_sqlite(conn_sqlite, nome_arquivo):
    try:
        query = f"SELECT * FROM {TABELA_SQLITE_ORIGEM} WHERE {COLUNA_LIGACAO_SQLITE} = ?"
        df = pd.read_sql_query(query, conn_sqlite, params=(nome_arquivo,))
        if not df.empty:
            return df
        return None
    except Exception as e:
        print(f"Erro ao buscar no SQLite: {e}")
        return None


def salvar_no_duckdb_robusto(conn_duck, df_dados):
    try:
        colunas_alvo = [
            "id_linha",
            "numero_arquivo",
            "descricao",
            "periodicidade",
            "publicado_em",
            "ano",
            "mes",
            "tamanho_arquivo",
            "arquivo_salvo_em",
            "url_documento",
            "data_extracao",
            "id_pagina",
            "texto_conteudo",
            "status_extracao",
            "data_processamento"
        ]
        
        # Garantir que o DataFrame tenha todas as colunas da lista (preenche com None se faltar)
        for col in colunas_alvo:
            if col not in df_dados.columns:
                df_dados[col] = None

        # Filtrar o DataFrame para ter APENAS essas colunas, na ordem correta
        df_para_inserir = df_dados[colunas_alvo]

        # Montar a Query Dinâmica
        colunas_str = ", ".join(colunas_alvo)
        
        # Verifica se tabela existe para criar ou inserir
        try:
            conn_duck.sql(f"SELECT 1 FROM {TABELA_DUCKDB_DESTINO} LIMIT 1")
            tabela_existe = True
        except:
            tabela_existe = False

        if not tabela_existe:
            # Cria a tabela.
            conn_duck.execute(f"""
                CREATE TABLE {TABELA_DUCKDB_DESTINO} (
                    id INTEGER PRIMARY KEY, 
                    {', '.join([f'{col} VARCHAR' for col in colunas_alvo])}
                )
            """)
            conn_duck.execute(f"INSERT INTO {TABELA_DUCKDB_DESTINO} ({colunas_str}) SELECT {colunas_str} FROM df_para_inserir")
            
        else:
            conn_duck.execute(f"INSERT INTO {TABELA_DUCKDB_DESTINO} ({colunas_str}) SELECT {colunas_str} FROM df_para_inserir")
            
    except Exception as e:
        print(f"\n❌ Erro ao salvar no DuckDB: {e}")
        print(f"   Colunas no DF atual: {list(df_dados.columns)}")
        raise

def processar_e_salvar_texto(id_selecionados):
    
    # Conexões com Bancos de Dados
    conn_sqlite = None
    conn_duck = None
    
    try:
        print(f"\n🔌 Conectando ao SQLite Origem: {CAMINHO_BD_SQLITE_ORIGEM}")
        conn_sqlite = sqlite3.connect(CAMINHO_BD_SQLITE_ORIGEM)
        
        print(f"🦆 Conectando ao DuckDB Final: {CAMINHO_DUCKDB_FINAL}")
        conn_duck = duckdb.connect(CAMINHO_DUCKDB_FINAL)

    except Exception as e:
        print(f"\n❌ Erro nas conexões de banco de dados: {e}")
        return

    arquivos_processados = 0
    sucesso_extracao = 0
    falha_extracao = 0
    nao_encontrado_sqlite = 0

    for id_pasta in id_selecionados:
        pasta_origem = os.path.join(PASTA_RAIZ_DOCUMENTOS, f"id_{id_pasta}")
        pasta_destino = os.path.join(PASTA_RAIZ_DOCUMENTOS, f"id_{id_pasta}extraidos")
        
        if not os.path.isdir(pasta_origem):
            continue

        os.makedirs(pasta_destino, exist_ok=True)
        arquivos_para_extrair = glob.glob(os.path.join(pasta_origem, "*.pdf"))

        if not arquivos_para_extrair:
            print(f"✅ Pasta 'id_{id_pasta}' vazia.")
            continue

        print(f"-> Processando {len(arquivos_para_extrair)} arquivos em 'id_{id_pasta}'")

        for caminho_arquivo in tqdm(arquivos_para_extrair, desc=f"Extraindo id_{id_pasta}"):
            nome_arquivo = os.path.basename(caminho_arquivo)
            arquivos_processados += 1
            
            #  Extrai o Texto
            status, texto_extraido = extrair_texto_pdf(caminho_arquivo)

            if status.startswith("SUCESSO"):
                # Busca Metadados no SQLite Existente
                df_meta = buscar_metadados_sqlite(conn_sqlite, nome_arquivo)
                
                if df_meta is not None:
                    # 3. Adiciona o texto extraído e metadados de processo ao DataFrame
                    df_meta['texto_conteudo'] = texto_extraido
                    df_meta['status_extracao'] = status
                    df_meta['data_processamento'] = pd.Timestamp.now()

                    # Salva no DuckDB
                    try:
                        salvar_no_duckdb_robusto(conn_duck, df_meta)
                        
                        # Move arquivo
                        shutil.move(caminho_arquivo, os.path.join(pasta_destino, nome_arquivo))
                        sucesso_extracao += 1
                    except Exception:
                        falha_extracao += 1
                else:
                    # Arquivo existe na pasta, mas não foi achado no banco SQLite
                    nao_encontrado_sqlite += 1
            else:
                falha_extracao += 1

    # Fecha conexões
    if conn_sqlite: conn_sqlite.close()
    if conn_duck: conn_duck.close()

    print("\n" + "="*40)
    print("📊 RESUMO DA EXTRAÇÃO")
    print("="*40)
    print(f"👉 Processados: {arquivos_processados}")
    print(f"✅ Sucesso (Merge + DuckDB): {sucesso_extracao}")
    print(f"⚠️ Não encontrado no SQLite Origem: {nao_encontrado_sqlite}")
    print(f"❌ Falha Extração: {falha_extracao}")
    print(f"\nDados salvos em: {CAMINHO_DUCKDB_FINAL}")
    print("="*40)

def menu_principal():
    pastas_disponiveis = sorted([
        int(re.search(r'id_(\d+)', name).group(1))
        for name in os.listdir(PASTA_RAIZ_DOCUMENTOS)
        if re.match(r'id_\d+$', name) and os.path.isdir(os.path.join(PASTA_RAIZ_DOCUMENTOS, name))
    ])

    if not pastas_disponiveis:
        print(f"❌ Nenhuma pasta 'id_X' encontrada.")
        return

    while True:
        print(f"\n📂 Pastas disponíveis: {pastas_disponiveis}")
        print("1. IDs específicos | 2. Todos | 3. Sair")
        escolha = input("Opção: ").strip()

        if escolha == '1':
            ids = input("IDs (ex: 8,16): ").split(',')
            try:
                ids_sel = [int(i) for i in ids if i.strip() and int(i) in pastas_disponiveis]
                if ids_sel: processar_e_salvar_texto(ids_sel)
            except: print("Erro nos IDs.")
        elif escolha == '2':
            processar_e_salvar_texto(pastas_disponiveis)
            break
        elif escolha == '3':
            break

if __name__ == "__main__":
    if os.path.isdir(PASTA_RAIZ_DOCUMENTOS):
        menu_principal()
    else:
        print(f"❌ Pasta raiz não encontrada: {PASTA_RAIZ_DOCUMENTOS}")