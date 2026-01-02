import fitz # PyMuPDF
import os
import glob
import shutil
import sqlite3
import re
from tqdm import tqdm

# NOVAS IMPORTAÇÕES PARA OCR
try:
    import pytesseract
    from PIL import Image
    from io import BytesIO
    
    # É NECESSÁRIO AJUSTAR ESTE CAMINHO PARA ONDE O SEU 'tesseract.exe' ESTÁ INSTALADO.
    pytesseract.pytesseract.tesseract_cmd = r'C:/Program Files/Tesseract-OCR/tesseract.exe'
    TESSERACT_DISPONIVEL = True
except ImportError:
    print("⚠️ Aviso: pytesseract e/ou Pillow não encontrados. Apenas a extração direta de texto (sem OCR) funcionará.")
    TESSERACT_DISPONIVEL = False
except Exception as e:
    print(f"⚠️ Aviso: Erro ao configurar o Tesseract. Verifique o caminho. Erro: {e}")
    TESSERACT_DISPONIVEL = False



# --- CONFIGURAÇÃO ---
# 1. Caminho para a pasta raiz dos documentos (onde estão as pastas 'id_X').
PASTA_RAIZ_DOCUMENTOS = "../documentos_convenios" 

# 2. Caminho para o arquivo de banco de dados SQLite de SAÍDA.
SQLITE_DB_FILE = r"C:/Users/gmeri/OneDrive/Área de Trabalho/Pasta Pessoal/Trabalho/gmJulius/bds/convenios.db" 

# 3. Nome da tabela DENTRO do arquivo SQLite de SAÍDA.
SQLITE_TABLE_NAME = "convenios_texto" 

# 4. Nome da coluna que identificará o PDF (deve ser o nome do arquivo, ex: '8_convenio_01.pdf').
COLUNA_IDENTIFICADORA = "numero_arquivo" 

# 5. Coluna onde o texto extraído será salvo (será a mesma para extração direta e OCR).
COLUNA_TEXTO = "texto_extraido"
# --------------------


def extrair_texto_pdf(caminho_pdf):
    """
    Tenta extrair o texto:
    1. Direto (PyMuPDF)
    2. Fallback para OCR (Tesseract), se o primeiro falhar e Tesseract estiver disponível.
    """
    texto_total = []
    
    # --- 1. TENTATIVA DE EXTRAÇÃO DIRETA (PyMuPDF) ---
    try:
        with fitz.open(caminho_pdf) as pdf:
            for pagina in pdf:
                # O parâmetro 'text' extrai texto digitalizado, se houver
                texto_pagina = pagina.get_text("text").strip()
                if texto_pagina:
                    texto_total.append(texto_pagina) 
        
        texto_combinado = "\n".join(texto_total).strip()
        
        if len(texto_combinado) > 50: # Assume que se houver mais de 50 caracteres é um sucesso
            return "SUCESSO_DIRETO", texto_combinado
            
    except Exception as e:
        # Se falhar na extração direta, continua para o OCR
        print(f"\n🚨 Aviso: Falha na extração direta de {os.path.basename(caminho_pdf)}: {e}")

    # --- 2. FALLBACK PARA OCR (Tesseract) ---
    if TESSERACT_DISPONIVEL:
        try:
            texto_ocr = []
            with fitz.open(caminho_pdf) as pdf:
                for num_pagina, pagina in enumerate(pdf):
                    # Renderiza a página como imagem (PNG) em alta resolução (DPI=300)
                    pix = pagina.get_pixmap(matrix=fitz.Matrix(300 / 72, 300 / 72))
                    
                    # Converte o pixmap para um objeto de imagem PIL em memória
                    img_data = pix.tobytes("png")
                    img = Image.open(BytesIO(img_data))
                    
                    # Roda o OCR na imagem (usando idioma português e inglês como padrão, ou apenas 'por')
                    # Configuração para extração do texto
                    config_ocr = '-l por+eng --oem 3 --psm 3' # Otimizado para páginas padronizadas
                    texto = pytesseract.image_to_string(img, config=config_ocr).strip()
                    
                    if texto:
                        texto_ocr.append(f"--- Página {num_pagina + 1} ---\n{texto}")

            texto_combinado_ocr = "\n".join(texto_ocr).strip()
            
            if len(texto_combinado_ocr) > 50:
                return "SUCESSO_OCR", texto_combinado_ocr
                
        except Exception as e:
            print(f"\n🚨 Erro no OCR com Tesseract para {os.path.basename(caminho_pdf)}: {e}")


    # --- 3. FALHA TOTAL ---
    return "FALHA_TOTAL", ""


def criar_tabela_saida(conn, table_name, id_col, texto_col):
    """
    Cria a tabela no banco de dados de saída, se ela não existir.
    """
    try:
        cursor = conn.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                {id_col} TEXT PRIMARY KEY,
                {texto_col} TEXT,
                status_extracao TEXT,
                data_extracao DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
    except sqlite3.Error as e:
        print(f"\n❌ Erro ao criar tabela de saída: {e}")
        raise

def processar_e_salvar_texto(id_selecionados):
    """
    Processa os PDFs das pastas 'id_X' selecionadas, salva o texto no DB de saída 
    e move os PDFs extraídos com sucesso para 'id_Xextraidos'.
    """
    
    # 1. Conexão com o DB de Saída
    conn = None
    try:
        print(f"\nConectando ao banco de dados de saída: {SQLITE_DB_FILE}")
        conn = sqlite3.connect(SQLITE_DB_FILE)
        
        # Garante que a tabela de saída exista
        criar_tabela_saida(conn, SQLITE_TABLE_NAME, COLUNA_IDENTIFICADORA, COLUNA_TEXTO)
        cursor = conn.cursor()

    except sqlite3.Error as e:
        print(f"\n❌ Erro de conexão/criação no SQLite de Saída: {e}")
        return
    
    # 2. Itera sobre os IDs selecionados
    arquivos_processados = 0
    sucesso_extracao = 0
    falha_extracao = 0
    contagem_ocr = 0

    print(f"\nIniciando extração para os IDs: {id_selecionados}...")
    
    for id_pasta in id_selecionados:
        pasta_origem = os.path.join(PASTA_RAIZ_DOCUMENTOS, f"id_{id_pasta}")
        pasta_destino = os.path.join(PASTA_RAIZ_DOCUMENTOS, f"id_{id_pasta}extraidos")
        
        if not os.path.isdir(pasta_origem):
            print(f"⚠️ Pasta de origem 'id_{id_pasta}' não encontrada. Pulando.")
            continue

        os.makedirs(pasta_destino, exist_ok=True)

        arquivos_para_extrair = glob.glob(os.path.join(pasta_origem, "*.pdf"))
        
        if not arquivos_para_extrair:
            print(f"✅ Pasta 'id_{id_pasta}' vazia. Todos os arquivos foram processados/movidos.")
            continue

        print(f"-> Processando {len(arquivos_para_extrair)} arquivos em 'id_{id_pasta}'")

        # 3. Itera sobre os arquivos encontrados
        for caminho_arquivo in tqdm(arquivos_para_extrair, desc=f"Extraindo em id_{id_pasta}"):
            nome_arquivo = os.path.basename(caminho_arquivo)
            arquivos_processados += 1
            
            # --- Tenta Extração Direta, depois OCR ---
            status, texto_extraido = extrair_texto_pdf(caminho_arquivo)

            if status.startswith("SUCESSO"):
                if status == "SUCESSO_OCR":
                    contagem_ocr += 1

                try:
                    # UPSERT: Insere o texto extraído (ou atualiza se já existir)
                    insert_query = f"""
                        INSERT INTO {SQLITE_TABLE_NAME} ({COLUNA_IDENTIFICADORA}, {COLUNA_TEXTO}, status_extracao)
                        VALUES (?, ?, ?)
                        ON CONFLICT({COLUNA_IDENTIFICADORA}) DO UPDATE SET
                            {COLUNA_TEXTO} = excluded.{COLUNA_TEXTO},
                            status_extracao = excluded.status_extracao,
                            data_extracao = CURRENT_TIMESTAMP
                    """
                    cursor.execute(insert_query, (nome_arquivo, texto_extraido, status))
                    conn.commit()
                    
                    # Move o arquivo para a pasta de sucesso (id_Xextraidos)
                    shutil.move(caminho_arquivo, os.path.join(pasta_destino, nome_arquivo))
                    sucesso_extracao += 1
                
                except sqlite3.Error as e:
                    print(f"\n❌ Erro ao salvar/mover {nome_arquivo}: {e}")
                    falha_extracao += 1 
            else:
                # FALHA_TOTAL: Tenta registrar a falha no DB
                try:
                    insert_fail_query = f"""
                        INSERT INTO {SQLITE_TABLE_NAME} ({COLUNA_IDENTIFICADORA}, {COLUNA_TEXTO}, status_extracao)
                        VALUES (?, ?, 'FALHA_TOTAL')
                        ON CONFLICT({COLUNA_IDENTIFICADORA}) DO UPDATE SET
                            {COLUNA_TEXTO} = '', -- Limpa o texto, se houver
                            status_extracao = 'FALHA_TOTAL',
                            data_extracao = CURRENT_TIMESTAMP
                    """
                    cursor.execute(insert_fail_query, (nome_arquivo, ''))
                    conn.commit()
                except sqlite3.Error:
                    pass 
                
                falha_extracao += 1

    # 4. Resumo Final
    if conn:
        conn.close()
    
    print("\n" + "="*40)
    print("📊 RESUMO DA EXTRAÇÃO E GESTÃO DE ARQUIVOS")
    print("="*40)
    print(f"👉 Total de arquivos PDF processados: {arquivos_processados}")
    print(f"✅ Arquivos com texto extraído e movido: {sucesso_extracao}")
    print(f"   (Incluindo {contagem_ocr} extraídos via OCR)")
    print(f"❌ Arquivos com falha na extração (direta e OCR): {falha_extracao}")
    print(f"\nTexto salvo em: {SQLITE_DB_FILE}")
    print("="*40)
    print("✅ Processo concluído.")

def menu_principal():
    """
    Exibe o menu e gerencia a escolha do usuário.
    """
    # Lista dinamicamente as pastas 'id_X' disponíveis
    pastas_disponiveis = sorted([
        int(re.search(r'id_(\d+)', name).group(1))
        for name in os.listdir(PASTA_RAIZ_DOCUMENTOS)
        if re.match(r'id_\d+$', name) and os.path.isdir(os.path.join(PASTA_RAIZ_DOCUMENTOS, name))
    ])

    if not pastas_disponiveis:
        print(f"❌ Nenhuma pasta 'id_X' encontrada em '{PASTA_RAIZ_DOCUMENTOS}'. Verifique o caminho.")
        return

    while True:
        print("\n" + "="*50)
        print("🏛️ MENU DE EXTRAÇÃO DE TEXTO DOS CONVÊNIOS (DIRETO + OCR)")
        print("="*50)
        print(f"Pastas 'id_X' disponíveis para processamento: \n{pastas_disponiveis}")
        print("\nOpções:")
        print("  1. Rodar extração para IDs específicos (ex: 8, 16, 55)")
        print("  2. Rodar extração para TODOS os IDs disponíveis")
        print("  3. Sair")
        print("="*50)

        escolha = input("Digite sua opção (1, 2 ou 3): ").strip()

        if escolha == '1':
            ids_input = input("Digite os IDs que deseja rodar, separados por vírgula (ex: 8,16,55): ").strip()
            try:
                ids_selecionados = []
                for id_str in ids_input.split(','):
                    if id_str:
                        id_int = int(id_str.strip())
                        if id_int in pastas_disponiveis:
                            ids_selecionados.append(id_int)
                        else:
                            print(f"⚠️ Aviso: O ID {id_int} não corresponde a uma pasta 'id_{id_int}' disponível e será ignorado.")
                
                if ids_selecionados:
                    processar_e_salvar_texto(ids_selecionados)
                else:
                    print("❌ Nenhuma ID válida foi selecionada. Tente novamente.")

            except ValueError:
                print("❌ Entrada inválida. Por favor, insira apenas números inteiros separados por vírgula.")
        
        elif escolha == '2':
            processar_e_salvar_texto(pastas_disponiveis)
            break
        
        elif escolha == '3':
            print("Saindo do programa. Até logo!")
            break
        
        else:
            print("❌ Opção inválida. Por favor, escolha 1, 2 ou 3.")

# --- Execução Principal ---
if __name__ == "__main__":
    
    if not os.path.isdir(PASTA_RAIZ_DOCUMENTOS):
        print(f"❌ O caminho da pasta raiz de documentos '{PASTA_RAIZ_DOCUMENTOS}' não existe. Por favor, corrija PASTA_RAIZ_DOCUMENTOS.")
    else:
        menu_principal()