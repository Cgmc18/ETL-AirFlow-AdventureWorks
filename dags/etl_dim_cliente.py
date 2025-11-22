from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd

DATA_PATH = '/opt/airflow/dags/data'

def extract_transform_load_cliente():
    # 1. EXTRACT
    df_customer = pd.read_csv(f'{DATA_PATH}/Sales Customer.csv', sep=',')
    df_person = pd.read_csv(f'{DATA_PATH}/Person Person.csv', sep=',')

    # 2. TRANSFORM
    # O Customer se liga ao Person através do PersonID (no Customer) e BusinessEntityID (no Person)
    df_merged = pd.merge(
        df_customer, 
        df_person, 
        left_on='PersonID', 
        right_on='BusinessEntityID', 
        how='left'
    )

    # Criar Nome Completo
    # Tratamos nulos com string vazia para não quebrar a concatenação
    df_merged['FirstName'] = df_merged['FirstName'].fillna('')
    df_merged['LastName'] = df_merged['LastName'].fillna('')
    df_merged['nome_completo'] = (df_merged['FirstName'] + ' ' + df_merged['LastName']).str.strip()
    
    # Se o nome ficou vazio (ex: clientes corporativos sem PersonID), colocamos "Cliente Corporativo"
    df_merged.loc[df_merged['nome_completo'] == '', 'nome_completo'] = 'Cliente Corporativo / Loja'

    # Definir Tipo de Cliente
    # No AdventureWorks, se tiver PersonID é Pessoa Física (Individual), senão é Loja (Store)
    df_merged['tipo_cliente'] = df_merged['PersonID'].apply(lambda x: 'Individual' if pd.notnull(x) else 'Store')

    # Selecionar colunas finais
    df_final = df_merged[['CustomerID', 'nome_completo', 'tipo_cliente']].copy()
    
    df_final.columns = [
        'id_cliente_original', # CustomerID
        'nome_completo',       
        'tipo_cliente'
    ]

    # Remover possíveis duplicatas de ID original
    df_final = df_final.drop_duplicates(subset=['id_cliente_original'])

    # 3. LOAD
    pg_hook = PostgresHook(postgres_conn_id='postgres_dw')
    engine = pg_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        connection.execute("TRUNCATE TABLE public.dim_cliente RESTART IDENTITY CASCADE;")
        
    df_final.to_sql(
        'dim_cliente', 
        con=engine, 
        schema='public', 
        if_exists='append', 
        index=False
    )
    
    print(f"Carga Cliente concluída! {len(df_final)} registros inseridos.")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    'etl_dim_cliente',
    default_args=default_args,
    schedule=None, 
    catchup=False,
    tags=['dw', 'adventureworks']
) as dag:

    task_etl_cliente = PythonOperator(
        task_id='etl_cliente_task',
        python_callable=extract_transform_load_cliente
    )

    task_etl_cliente