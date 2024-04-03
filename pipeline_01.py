import os
import gdown
import duckdb
import pandas as pandas
from sqlalchemy import create_engine
from dotenv import load_dotenv

from duckdb import DuckDBPyRelation
from pandas import DataFrame

import sys
from datetime import datetime

load_dotenv()

def conectar_banco():
    """Conecta ao banco de dados DuckDB; cria o banco se não existir."""
    return duckdb.connect(database='duckdb.db', read_only=False)

def inicializar_tabela(con):
    """Cria a tabela se ela não existir."""
    con.execute("""
        CREATE TABLE IF NOT EXISTS historico_arquivos (
            nome_arquivo VARCHAR,
            horario_processamento TIMESTAMP
        )
    """)

def registrar_arquivo(con, nome_arquivo):
    """Registra um novo arquivo no banco de dados com o horário atual."""
    con.execute("""
        INSERT INTO historico_arquivos (nome_arquivo, horario_processamento)
        VALUES (?, ?)
    """, (nome_arquivo, datetime.now()))

def arquivos_processados(con):
    """Retorna um set com os nomes de todos os arquivos já processados."""
    return set(row[0] for row in con.execute("SELECT nome_arquivo FROM historico_arquivos").fetchall())

def baixar_arquivos_gdrive(url_pasta, diretorio_local):
    os.makedirs(diretorio_local, exist_ok=True)
    gdown.download_folder(url_pasta, output=diretorio_local, quiet=False, use_cookies=False)

def listar_arquivos_csv(diretorio):
    arquivos_csv = []
    todos_os_arquivos = os.listdir(diretorio)
    for arquivo in todos_os_arquivos:
        if arquivo.endswith('.csv'):
            caminho_completo = os.path.join(diretorio, arquivo)
            arquivos_csv.append(caminho_completo)

    #print(arquivos_csv)
    return arquivos_csv

def ler_csv(caminho_do_arquivo):
    dataframe_duckdb = duckdb.read_csv(caminho_do_arquivo)
    return dataframe_duckdb

def transformar(df: DuckDBPyRelation) -> DataFrame:
    # execute query adding a new column
    df_transformado = duckdb.sql("""
                                    SELECT 
                                    origin,
                                    dest,
                                    COUNT(*) AS flies_quant
                                    FROM df 
                                    GROUP BY 1, 2
                                    ORDER BY flies_quant DESC""").df()
    size = sys.getsizeof(df_transformado)
    #print(f"O tamanho do DuckDBPyRelation na memória é {size} bytes.")
    quant = duckdb.sql("SELECT COUNT(*) FROM df").df()
    #print(df_transformado)
    print(quant)
    return df_transformado

def salvar_no_postgres(df_duckdb, tabela):
    DATABASE_URL = os.getenv("DATABASE_URL") # ex: 'postgresql://user:password@localhost:5432/database'
    #print(DATABASE_URL)
    engine = create_engine(DATABASE_URL)
    # salvar o duckdb no postgres
    df_duckdb.to_sql(tabela, con=engine, if_exists='append', index=False)


if __name__ == "__main__":
    print("Iniciando pipeline...")
    print(f"A hora atual é {datetime.now().strftime('%H:%M:%S')}")

    url_pasta = 'https://drive.google.com/drive/folders/1h2PgQbQqnSLDQK5KyZIdSb5f6F6i6Y-E'
    diretorio_local = '/workspace/python_duckdb_etl/files'
    #baixar_arquivos_gdrive(url_pasta, diretorio_local)
    lista_de_arquivos = listar_arquivos_csv(diretorio_local)
    con = conectar_banco()
    inicializar_tabela(con)
    processados = arquivos_processados(con)
    
    for caminho_do_arquivo in lista_de_arquivos:
        nome_arquivo = os.path.basename(caminho_do_arquivo)
        if nome_arquivo not in processados:
            arquivo = ler_csv(caminho_do_arquivo)
            duckdbdf = transformar(arquivo)
            salvar_no_postgres(duckdbdf, "flies")
            registrar_arquivo(con, nome_arquivo)
            print(f"Arquivo {nome_arquivo} processado e salvo {datetime.now().strftime('%H:%M:%S')}")
        else:
            print(f"Arquivo {nome_arquivo} já foi processado anteriormente.")
    print(f"Pipeline finalizado {datetime.now().strftime('%H:%M:%S')}")
