import fitz
from pymongo import MongoClient
import requests
import os
from tqdm import tqdm

client = MongoClient("mongodb://localhost:27017/")
db = client["Julius"]
colecao = db["Sao_Gabriel_da_Palha"]

def baixar_pdf(url, caminho_temp):
    try:
        resp = requests.get(url, timeout=15)
        if resp.status_code == 200 and resp.headers.get("content-type", "").startswith("application/pdf"):
            with open(caminho_temp, "wb") as f:
                f.write(resp.content)
            return True
    except Exception:
        pass
    return False

def extrair_texto_pdf(caminho_pdf):
    try:
        texto_total = []
        with fitz.open(caminho_pdf) as pdf:
            for pagina in pdf:
                texto_total.append(pagina.get_text("text"))
        return "\n".join(texto_total)
    except Exception as e:
        print(f"Erro ao extrair texto de {caminho_pdf}: {e}")
        return ""
    
#  Busca dos documentos
filtro = {"url_documento": {"$exists": True, "$ne": ""},
          "texto_extraido": {"$exists": False}}

cursor = colecao.find(filtro, no_cursor_timeout=True)

for doc in tqdm(cursor, desc="Extraindo texto dos PDFs"):
    url_pdf = doc.get("url_documento")

    # pula se não houver URL
    if not url_pdf:
        continue

    caminho_temp = "temp.pdf"
    texto_extraido = ""

    if baixar_pdf(url_pdf, caminho_temp):
        texto_extraido = extrair_texto_pdf(caminho_temp)
        os.remove(caminho_temp)

    # só atualiza se tiver texto
    if texto_extraido.strip():
        colecao.update_one({"_id": doc["_id"]}, {"$set": {"texto_extraido": texto_extraido}})

cursor.close()
print("✅ Extração concluída com sucesso usando PyMuPDF!")