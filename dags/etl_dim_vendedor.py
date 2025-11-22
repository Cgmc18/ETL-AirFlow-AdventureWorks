from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os

DATA_PATH = '/opt/airflow/dags/data'

def extract_transform_load_vendedor():
    # 1. EXTRACT
    df_salesperson = pd.read_csv(f'{DATA_PATH}/Sales SalesPerson.csv', sep=',')
    df_employee = pd.read_csv(f'{DATA_PATH}/HumanResources Employee.csv', sep=',')
    df_person = pd.read_csv(f'{DATA_PATH}/Person Person.csv', sep=',')

    # 2. TRANSFORM
    # O elo de ligação entre todas essas tabelas é o BusinessEntityID
    
    # Join 1: Vendedor -> Funcionario (Para pegar o Cargo/JobTitle)
    df_merged = pd.merge(
        df_salesperson, 
        df_employee[['BusinessEntityID', 'JobTitle']], 
        on='BusinessEntityID', 
        how='left'
    )
    
    # Join 2: Resultado -> Pessoa (Para pegar o Nome)
    df_merged = pd.merge(
        df_merged, 
        df_person[['BusinessEntityID', 'FirstName', 'LastName', 'MiddleName']], 
        on='BusinessEntityID', 
        how='left'
    )

    # Criar Nome Completo
    # Tratando nulos para não quebrar a concatenação
    df_merged['FirstName'] = df_merged['FirstName'].fillna('')
    df_merged['MiddleName'] = df_merged['MiddleName'].fillna('')
    df_merged['LastName'] = df_merged['LastName'].fillna('')
    
    # Nome = First + Middle (se tiver) + Last
    df_merged['nome_vendedor'] = (
        df_merged['FirstName'] + ' ' + 
        df_merged['MiddleName'] + ' ' + 
        df_merged['LastName']
    ).str.replace('  ', ' ').str.strip() # Remove espaços duplos extras

    # Selecionar colunas finais
    df_final = df_merged[['BusinessEntityID', 'nome_vendedor', 'JobTitle']].copy()
    
    df_final.columns = [
        'id_vendedor_original', # BusinessEntityID
        'nome_vendedor',        
        'cargo'                 # JobTitle
    ]
    
    # Remover duplicatas
    df_final = df_final.drop_duplicates(subset=['id_vendedor_original'])

    # 3. LOAD
    pg_hook = PostgresHook(postgres_conn_id='postgres_dw')
    engine = pg_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        connection.execute("TRUNCATE TABLE public.dim_vendedor RESTART IDENTITY CASCADE;")
        
    df_final.to_sql(
        'dim_vendedor', 
        con=engine, 
        schema='public', 
        if_exists='append', 
        index=False
    )
    
    print(f"Carga Vendedor concluída! {len(df_final)} vendedores inseridos.")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    'etl_dim_vendedor',
    default_args=default_args,
    schedule=None, 
    catchup=False,
    tags=['dw', 'adventureworks']
) as dag:

    task_vendedor = PythonOperator(
        task_id='etl_vendedor_task',
        python_callable=extract_transform_load_vendedor
    )

    task_vendedor