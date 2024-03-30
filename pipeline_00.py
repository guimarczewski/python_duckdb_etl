import os
import gdown
import duckdb
import pandas as pandas
from sqlalchemy import create_engine
from dotenv import load_dotenv

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
    return arquivos_csv

if __name__ == "__main__":
    url_pasta = 'url drive folder without ?shared'
    diretorio_local = '/workspace/python_duckdb_etl/files'
    baixar_arquivos_gdrive(url_pasta, diretorio_local)