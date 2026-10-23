## 1. O que está no GitHub vs O que vem do Google Drive

* **No GitHub:** Códigos-fonte, rotinas de processamento, scripts de embeddings (`Gerar_embedding_*.py`) e dependências (`requirements.txt`).
* **No Google Drive:** O arquivo do banco de dados `TCCa.duckdb` (~2 GB)
* **Onde posicionar o banco baixado:** O arquivo deve ser salvo dentro do diretório `bds/` na raiz do projeto:
  ```text
  TCC/
  ├── bds/
  │   ├── .gitkeep
  │   └── TCCa.duckdb   <-- COLOCAR O ARQUIVO DO GOOGLE DRIVE AQUI
  ├── Gerar_embedding_bertimbau.py
  ├── Gerar_embedding_bge_m3.py
  ├── Gerar_embedding_multilingual_e5_base.py
  ├── requirements.txt
  └── README.md
  ```

---

## 2. Preparação do Ambiente Virtual

### 2.1. Clonar o repositório e acessar a pasta
```bash
git clone <URL_DO_REPOSITORIO>
cd <PASTA_DO_REPOSITORIO>
```

### 2.2. Instalar o PyTorch (com suporte a GPU/CUDA)

Para garantir aceleração por GPU, instale a versão do PyTorch compilada com suporte a CUDA:

* **Se a máquina tiver GPU NVIDIA (CUDA 12.4 - recomendado):**
  ```bash
  pip install --upgrade pip
  pip install torch --index-url https://download.pytorch.org/whl/cu124
  ```
  *(Ou `cu121` se o driver de vídeo for mais antigo: `https://download.pytorch.org/whl/cu121`)*

* **Se a máquina NÃO tiver GPU dedicada (apenas CPU):**
  ```bash
  pip install --upgrade pip
  pip install torch
  ```

### 2.3. Instalar as demais dependências do projeto
```bash
pip install -r requirements.txt
```

### 2.4. Teste rápido de validação do ambiente
Execute o comando abaixo no terminal para validar se o PyTorch detectou a GPU com sucesso:
```bash
python -c "import torch; print('CUDA disponível:', torch.cuda.is_available()); print('Dispositivo:', torch.cuda.get_device_name(0) if torch.cuda.is_available() else 'CPU')"
```
> Se a saída for `CUDA disponível: True`, o ambiente está pronto para usar a aceleração da GPU.

---

## 3. Configuração da Base de Dados

1. Baixe o arquivo **`TCCa.duckdb`** do link compartilhado do Google Drive.
2. Mova o arquivo baixado para `bds/TCCa.duckdb`.

---

## 4. Ordem e Instruções de Execução

> ⚠️ **ATENÇÃO CRÍTICA (CONCORRÊNCIA DO BANCO):**  
> O DuckDB adquire trava exclusiva de escrita no arquivo. **NÃO execute dois scripts ao mesmo tempo.**  
> Execute os 3 scripts **um de cada vez (sequencialmente)**, esperando o primeiro terminar completamente antes de iniciar o próximo.

### Script 1: BERTimbau (BERT em Português)
* **Modelo Hugging Face:** `neuralmind/bert-base-portuguese-cased`
* **Coluna gerada no DuckDB:** `vetor_embedding_bertimbau` (FLOAT[768])
* **Comando:**
  ```bash
  python Gerar_embedding_bertimbau.py
  ```

---

### Script 2: BGE-M3 (Multilingual Dense)
* **Modelo Hugging Face:** `BAAI/bge-m3`
* **Coluna gerada no DuckDB:** `vetor_embedding_bge_m3` (FLOAT[1024])
* **Comando:**
  ```bash
  python Gerar_embedding_bge_m3.py
  ```

---

### Script 3: Multilingual E5 Base
* **Modelo Hugging Face:** `intfloat/multilingual-e5-base`
* **Coluna gerada no DuckDB:** `vetor_embedding_multilingual_e5_base` (FLOAT[768])
* **Comando:**
  ```bash
  python Gerar_embedding_multilingual_e5_base.py
  ```

---

## 5. Como Validar o Resultado Final

Ao concluir a execução dos 3 scripts, execute o comando de verificação para inspecionar se todos os 139.032 chunks possuem os três vetores gerados:

```bash
python -c "import duckdb; conn=duckdb.connect('bds/TCCa.duckdb', read_only=True); print(conn.execute('SELECT COUNT(chunk_id) AS total_chunks, COUNT(vetor_embedding_bertimbau) AS total_bertimbau, COUNT(vetor_embedding_bge_m3) AS total_bge, COUNT(vetor_embedding_multilingual_e5_base) AS total_e5 FROM chunks_contratos').df()); conn.close()"
```

**Resultado esperado:** As 4 colunas devem exibir exatamente o mesmo número total (139.032).

