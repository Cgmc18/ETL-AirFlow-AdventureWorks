from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os

# Caminho onde os CSVs estão (dentro do container)
# Como mapeamos a pasta dags, o caminho fica /opt/airflow/dags/data
DATA_PATH = '/opt/airflow/dags/data'

def extract_transform_load_produto():
    # 1. EXTRACT (Ler os CSVs)
    df_product = pd.read_csv(f'{DATA_PATH}/Production Product.csv', sep=',') 
    df_subcat = pd.read_csv(f'{DATA_PATH}/Production ProductSubcategory.csv', sep=',')
    df_cat = pd.read_csv(f'{DATA_PATH}/Production ProductCategory.csv', sep=',')

    # 2. TRANSFORM
    # Join: Produto -> Subcategoria
    df_merged = pd.merge(df_product, df_subcat, on='ProductSubcategoryID', how='left')
    
    # Join: Resultado -> Categoria
    df_merged = pd.merge(df_merged, df_cat, on='ProductCategoryID', how='left')

    # Selecionar colunas e renomear para o padrão do DW
    # O DataFrame final deve ter as colunas exatas da tabela dim_produto (exceto o ID serial sk_produto)
    df_final = df_merged[['ProductID', 'Name_x', 'Name_y', 'Name', 'Color']].copy()
    
    df_final.columns = [
        'id_produto_original', # ProductID
        'nome_produto',        # Name_x (do produto)
        'nome_subcategoria',   # Name_y (da subcategoria)
        'nome_categoria',      # Name (da categoria)
        'cor'                  # Color
    ]

    # Tratar nulos
    df_final['cor'] = df_final['cor'].fillna('N/A')
    df_final['nome_subcategoria'] = df_final['nome_subcategoria'].fillna('Sem Subcategoria')
    df_final['nome_categoria'] = df_final['nome_categoria'].fillna('Sem Categoria')

    # 3. LOAD (Carregar no Postgres)
    pg_hook = PostgresHook(postgres_conn_id='postgres_dw')
    
    # Pegar a engine do SQLAlchemy para usar com Pandas
    engine = pg_hook.get_sqlalchemy_engine()
    
    # Inserir dados na tabela dim_produto
    # Aqui vamos limpar antes para garantir não duplicar em testes.
    with engine.connect() as connection:
        connection.execute("TRUNCATE TABLE public.dim_produto RESTART IDENTITY CASCADE;")
        
    df_final.to_sql(
        'dim_produto', 
        con=engine, 
        schema='public', 
        if_exists='append', 
        index=False
    )
    
    print(f"Carga concluída! {len(df_final)} produtos inseridos.")

# Definição da DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    'etl_dim_produto',
    default_args=default_args,
    schedule=None, # Rodar manualmente
    catchup=False,
    tags=['dw', 'adventureworks']
) as dag:

    task_etl_produto = PythonOperator(
        task_id='etl_produto_task',
        python_callable=extract_transform_load_produto
    )

    task_etl_produto