import os
import json 
from pymongo import MongoClient

# Conexão com o MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["Julius"]
colecao = db["Sao_Gabriel_da_Palha"]

# Caminho da pasta com os JSONs
pasta_json = os.path.join(os.path.dirname(__file__), "jsons")

# Loop pelos arquivos JSON
for arquivo in os.listdir(pasta_json):
    if arquivo.endswith(".json"):
        caminho_arquivo = os.path.join(pasta_json, arquivo)
        with open(caminho_arquivo, "r", encoding="utf-8") as f:
            try:
                dados = json.load(f)

                if isinstance(dados, list):
                    colecao.insert_many(dados)
                else:
                    colecao.insert_one(dados)

                print(f"Inserido: {arquivo}")
            except json.JSONDecodeError:
                print(f"Erro ao ler {arquivo} — formato JSON inválido.")

# Fecha a conexão
client.close()