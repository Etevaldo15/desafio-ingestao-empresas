🚀 Desafio Ingestão no Limite: Pipeline de Dados Otimizado

Este repositório contém a minha solução para o desafio [Ingestão no Limite](https://github.com/mpraes/ingestao_no_limite), uma competição de Engenharia de Dados focada em **eficiência extrema, código performático e FinOps**.

## 🎯 O Desafio
O objetivo era criar um pipeline ELT para ingerir, transformar e carregar **~68,6 milhões de linhas** (aprox. 5GB descompactados) de dados de empresas da Receita Federal em um banco PostgreSQL. 

**A grande restrição:** O pipeline deveria rodar em um container Docker com **no máximo 1 GB de RAM** e **2 CPUs**, processando tudo em menos de 60 minutos.

## 🛠️ Stack e Arquitetura
*   **Linguagem:** Python 3.11
*   **Engine de Processamento:** [DuckDB](https://duckdb.org/) (In-process OLAP)
*   **Destino:** PostgreSQL
*   **Containerização:** Docker

## 🧠 Decisões de Engenharia (Como vencer o limite de 1GB de RAM)

1. **DuckDB com Spill-to-Disk:** Em vez de usar `pandas` (que carrega tudo na memória e estouraria o limite de 1GB causando *OOM Killed*), utilizei o DuckDB. Ele processa os dados em blocos (*out-of-core*) e usa o disco temporário (`/tmp/duckdb_spill`) quando a RAM atinge o limite configurado (`memory_limit='800MB'`).
2. **Tabelas `UNLOGGED` no Postgres:** A tabela final é criada como `UNLOGGED`. Isso desativa o *Write-Ahead Log (WAL)* do PostgreSQL durante a carga, acelerando drasticamente os `INSERTs` e reduzindo o I/O em disco.
3. **Deduplicação via Primary Key (Gate DQ-09):** Para garantir a unicidade do `cnpj_basico` sem gastar RAM implementando um *Bitset* manual, a coluna foi definida como `PRIMARY KEY` no Postgres, rejeitando duplicatas nativamente.
4. **Saneamento de Encoding (Gate DQ-10):** O dataset original está em `ISO-8859-1` (Latin-1) e contém sujeiras. A query SQL do DuckDB aplica `regexp_replace` para remover caracteres inválidos (`U+FFFD`) antes da inserção, garantindo zero erros no gate de qualidade de dados.
5. **Transformações em SQL (Zero-Copy):** Todas as 6 colunas derivadas (faixas de capital, flag de MEI via Regex, grupos de natureza jurídica) são calculadas diretamente na query do DuckDB, evitando a materialização de DataFrames intermediários na memória do Python.

## 🚀 Como rodar localmente
```bash
# Construir a imagem
docker build -t ingestao-empresas .

# Rodar com os limites da competição (1GB RAM, 2 CPUs)
docker run --rm \
  --memory=1g --memory-swap=1g --cpus=2 \
  -v $(pwd)/data:/data:ro \
  -e PG_HOST=host.docker.internal \
  -e PG_PASSWORD=sua_senha \
  ingestao-empresas