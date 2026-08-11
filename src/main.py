import duckdb
import os
import zipfile
import glob
import tempfile
import shutil
import sys
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    logging.info("Iniciando pipeline de ingestão com DuckDB (Modo Competição)...")

    PARTICIPANTE = os.getenv('PARTICIPANTE', 'etevaldo15')
    PG_TABLE = os.getenv('PG_TABLE', f"{PARTICIPANTE}_empresas")
    PG_HOST = os.getenv('PG_HOST', 'postgres_db')
    PG_PORT = os.getenv('PG_PORT', '5432')
    PG_USER = os.getenv('PG_USER', 'homelab_postgres')
    PG_PASSWORD = os.getenv('PG_PASSWORD', 'postgres')
    PG_DB = os.getenv('PG_DB', 'db_empresas')

    con = duckdb.connect(config={
        'memory_limit': '800MB',
        'threads': '2',
        'temp_directory': '/tmp/duckdb_spill'
    })

    try:
        con.execute("INSTALL postgres;")
        con.execute("LOAD postgres;")

        conn_string = f"dbname={PG_DB} user={PG_USER} password={PG_PASSWORD} host={PG_HOST} port={PG_PORT}"
        logging.info(f"Conectando ao Postgres em {PG_HOST}...")
        con.execute(f"ATTACH '{conn_string}' AS pg (TYPE POSTGRES);")

        logging.info(f"Criando tabela UNLOGGED public.{PG_TABLE}...")
        
        con.execute(f"DROP TABLE IF EXISTS pg.public.{PG_TABLE};")
        
        con.execute(f"""
            CREATE UNLOGGED TABLE pg.public.{PG_TABLE} (
                cnpj_basico VARCHAR(8) PRIMARY KEY,
                razao_social VARCHAR NOT NULL,
                natureza_juridica VARCHAR(4) NOT NULL,
                qualificacao_responsavel VARCHAR NOT NULL,
                capital_social DOUBLE PRECISION NOT NULL,
                porte_codigo VARCHAR(2) NOT NULL,
                porte_descricao VARCHAR NOT NULL,
                ente_federativo VARCHAR,
                capital_social_faixa VARCHAR NOT NULL,
                is_mei BOOLEAN NOT NULL,
                natureza_juridica_grupo VARCHAR NOT NULL,
                ente_federativo_presente BOOLEAN NOT NULL,
                data_processamento TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            );
        """)

        data_dir = '/data/'
        zip_files = sorted(glob.glob(os.path.join(data_dir, '*.zip')))
        
        if not zip_files:
            logging.warning(f"Nenhum arquivo .zip encontrado em {data_dir}")
            return

        temp_dir = tempfile.mkdtemp()
        logging.info(f"Pasta temporária criada em: {temp_dir}")

        for zip_path in zip_files:
            logging.info(f"Processando arquivo: {os.path.basename(zip_path)}...")
            
            try:
                with zipfile.ZipFile(zip_path, 'r') as zf:
                    csv_files = [f for f in zf.namelist() if f.endswith('.EMPRECSV')]
                    if not csv_files: continue
                    csv_name = csv_files[0]
                    
                    extracted_path = zf.extract(csv_name, path=temp_dir)
                    
                    # Query com Saneamento de Encoding para garantir DQ-10
                    insert_query = f"""
                    INSERT INTO pg.public.{PG_TABLE}
                    SELECT 
                        LPAD(CAST(column00 AS VARCHAR), 8, '0') AS cnpj_basico,
                        
                        -- DQ-02 e DQ-10: UPPER, TRIM e remoção de caracteres inválidos (U+FFFD)
                        UPPER(TRIM(regexp_replace(column01, '', '', 'g'))) AS razao_social,
                        
                        LPAD(CAST(column02 AS VARCHAR), 4, '0') AS natureza_juridica,
                        CAST(column03 AS VARCHAR) AS qualificacao_responsavel,
                        
                        -- Capital: Tratamento robusto para evitar erros de cast
                        CAST(REPLACE(CAST(column04 AS VARCHAR), ',', '.') AS DOUBLE) AS capital_social,
                        
                        CASE WHEN column05 IS NULL OR TRIM(CAST(column05 AS VARCHAR)) = '' THEN '00' ELSE CAST(column05 AS VARCHAR) END AS porte_codigo,
                        
                        CASE TRIM(CAST(column05 AS VARCHAR))
                            WHEN '01' THEN 'MICRO EMPRESA'
                            WHEN '03' THEN 'EMPRESA DE PEQUENO PORTE'
                            WHEN '05' THEN 'DEMAIS'
                            ELSE 'NÃO INFORMADO'
                        END AS porte_descricao,
                        
                        NULLIF(TRIM(CAST(column06 AS VARCHAR)), '') AS ente_federativo,
                        
                        CASE 
                            WHEN CAST(REPLACE(CAST(column04 AS VARCHAR), ',', '.') AS DOUBLE) = 0 THEN 'SEM CAPITAL'
                            WHEN CAST(REPLACE(CAST(column04 AS VARCHAR), ',', '.') AS DOUBLE) <= 1000 THEN 'ATÉ 1K'
                            WHEN CAST(REPLACE(CAST(column04 AS VARCHAR), ',', '.') AS DOUBLE) <= 10000 THEN '1K A 10K'
                            WHEN CAST(REPLACE(CAST(column04 AS VARCHAR), ',', '.') AS DOUBLE) <= 100000 THEN '10K A 100K'
                            WHEN CAST(REPLACE(CAST(column04 AS VARCHAR), ',', '.') AS DOUBLE) <= 1000000 THEN '100K A 1M'
                            ELSE 'ACIMA DE 1M'
                        END AS capital_social_faixa,
                        
                        regexp_matches(UPPER(TRIM(regexp_replace(column01, '', '', 'g'))), '\\d{{11}}$') AS is_mei,
                        
                        CASE SUBSTR(LPAD(CAST(column02 AS VARCHAR), 4, '0'), 1, 1)
                            WHEN '1' THEN 'ADMINISTRAÇÃO PÚBLICA'
                            WHEN '2' THEN 'ENTIDADES EMPRESARIAIS'
                            WHEN '3' THEN 'ENTIDADES SEM FINS LUCRATIVOS'
                            WHEN '4' THEN 'PESSOAS FÍSICAS'
                            WHEN '5' THEN 'ORGANIZAÇÕES INTERNACIONAIS'
                            ELSE 'OUTROS'
                        END AS natureza_juridica_grupo,
                        
                        CASE WHEN NULLIF(TRIM(CAST(column06 AS VARCHAR)), '') IS NOT NULL THEN true ELSE false END AS ente_federativo_presente,
                        
                        NOW() AS data_processamento

                    FROM read_csv_auto(
                        '{extracted_path}', 
                        header=false, 
                        delim=';', 
                        quote='"', 
                        escape='"',
                        encoding='latin1',
                        all_varchar=true,
                        sample_size=-1
                    )
                    """
                    
                    con.execute(insert_query)
                    logging.info(f" Arquivo {csv_name} carregado!")

            except Exception as e:
                logging.error(f"  Erro ao processar {zip_path}: {e}")
                raise e 
            finally:
                if 'extracted_path' in locals() and os.path.exists(extracted_path):
                    os.remove(extracted_path)

        logging.info(" Pipeline finalizado com sucesso!")

    except Exception as e:
        logging.error(f" Erro fatal no pipeline: {e}")
        sys.exit(1)
    finally:
        if 'temp_dir' in locals():
            shutil.rmtree(temp_dir)
        con.close()

if __name__ == "__main__":
    main()