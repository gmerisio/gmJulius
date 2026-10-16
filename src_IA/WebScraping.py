import requests
import time

base_url = "https://Viana-es.portaltp.com.br/consultas/documentos.aspx?id="

# O 'User-Agent' faz o Python se passar por um navegador normal
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
}

links_validos = []

# Testando um intervalo (ajuste conforme necessário)
for doc_id in range(1, 3501):
    url = f"{base_url}{doc_id}"
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        html = response.text.lower() 
        
        # Procurando por textos exatos que aparecem na interface válida
        if "arraste um cabeçalho" in html or "imprimir relatório" in html:
            print(f"[+] Válido: {url}")
            links_validos.append(url)
        else:
            print(f"[-] Inválido/Vazio: {url}")
            
    except Exception as e:
        print(f"[!] Erro ao conectar no link {url}: {e}")
    
    # Pausa para evitar bloqueios
    time.sleep(1)

print("\n--- Lista Final de Links Válidos ---")
if len(links_validos) > 0:
    for link in links_validos:
        print(link)
else:
    print("Nenhum link válido foi encontrado nesse intervalo.")