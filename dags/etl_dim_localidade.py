from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd

# Caminho dos arquivos
DATA_PATH = '/opt/airflow/dags/data'

def extract_transform_load_localidade():
    # 1. EXTRACT
    df_address = pd.read_csv(f'{DATA_PATH}/Person Address.csv', sep=',')
    df_state = pd.read_csv(f'{DATA_PATH}/Person StateProvince.csv', sep=',')
    df_country = pd.read_csv(f'{DATA_PATH}/Person CountryRegion.csv', sep=',')

    # 2. TRANSFORM
    # Renomear colunas conflitantes logo no início para evitar confusão no merge
    df_state = df_state.rename(columns={'Name': 'Nome_Estado'})
    df_country = df_country.rename(columns={'Name': 'Nome_Pais'})

    # Join 1: Endereço -> Estado
    # Chave: StateProvinceID
    df_merged = pd.merge(
        df_address, 
        df_state[['StateProvinceID', 'CountryRegionCode', 'Nome_Estado']], # Trazendo só o necessário
        on='StateProvinceID', 
        how='left'
    )
    
    # Join 2: Resultado -> País
    # Chave: CountryRegionCode
    df_final_merge = pd.merge(
        df_merged, 
        df_country[['CountryRegionCode', 'Nome_Pais']], # Trazendo só o necessário
        on='CountryRegionCode', 
        how='left'
    )

    # Selecionar e Renomear para o padrão do DW
    df_final = df_final_merge[['AddressID', 'City', 'Nome_Estado', 'Nome_Pais']].copy()
    
    df_final.columns = [
        'id_endereco_original', # AddressID
        'cidade',               # City
        'estado',               # Nome_Estado (que renomeamos acima)
        'pais'                  # Nome_Pais (que renomeamos acima)
    ]

    # Tratamento de nulos
    df_final['cidade'] = df_final['cidade'].fillna('Desconhecida')
    df_final['estado'] = df_final['estado'].fillna('Não Informado')
    df_final['pais'] = df_final['pais'].fillna('Não Informado')

    # Remover duplicatas de ID original se houver
    df_final = df_final.drop_duplicates(subset=['id_endereco_original'])

    # 3. LOAD
    pg_hook = PostgresHook(postgres_conn_id='postgres_dw')
    engine = pg_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        # Limpa a tabela antes de inserir
        connection.execute("TRUNCATE TABLE public.dim_localidade RESTART IDENTITY CASCADE;")
        
    df_final.to_sql(
        'dim_localidade', 
        con=engine, 
        schema='public', 
        if_exists='append', 
        index=False
    )
    print(f"Carga Localidade concluída! {len(df_final)} registros.")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    'etl_dim_localidade',
    default_args=default_args,
    schedule=None, 
    catchup=False,
    tags=['dw', 'adventureworks']
) as dag:

    task_localidade = PythonOperator(
        task_id='etl_localidade_task',
        python_callable=extract_transform_load_localidade
    )

    task_localidade