from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os

def generate_load_tempo():
    # 1. GENERATE (Gerar dados via código)
    # Definir o período desejado (AdventureWorks tem dados de ~2011 a 2014)
    start_date = '2010-01-01'
    end_date = '2025-12-31'
    
    # Criar um range de datas
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    # Criar DataFrame base
    df = pd.DataFrame(date_range, columns=['data_completa'])
    
    # 2. TRANSFORM (Derivar colunas)
    df['ano'] = df['data_completa'].dt.year
    df['mes'] = df['data_completa'].dt.month
    df['dia'] = df['data_completa'].dt.day
    df['trimestre'] = df['data_completa'].dt.quarter
    
    # Calcular semestre: Se mês <= 6 é 1, senão 2
    df['semestre'] = df['data_completa'].dt.month.apply(lambda x: 1 if x <= 6 else 2)
    
    # Mapeamento de nomes de meses em PT-BR
    meses_pt = {
        1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril',
        5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto',
        9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'
    }
    df['nome_mes'] = df['mes'].map(meses_pt)
    
    # Criar sk_tempo (Chave Inteira YYYYMMDD)
    # Ex: 2023-05-20 vira o inteiro 20230520
    df['sk_tempo'] = (
        df['ano'] * 10000 + 
        df['mes'] * 100 + 
        df['dia']
    ).astype(int)
    
    # Selecionar colunas finais na ordem da tabela
    df_final = df[['sk_tempo', 'data_completa', 'ano', 'mes', 'nome_mes', 'trimestre', 'semestre']]
    
    # 3. LOAD
    pg_hook = PostgresHook(postgres_conn_id='postgres_dw')
    engine = pg_hook.get_sqlalchemy_engine()
    
    with engine.connect() as connection:
        connection.execute("TRUNCATE TABLE public.dim_tempo RESTART IDENTITY CASCADE;")
        
    df_final.to_sql(
        'dim_tempo', 
        con=engine, 
        schema='public', 
        if_exists='append', 
        index=False
    )

    print(f"Carga Tempo concluída! {len(df_final)} dias gerados.")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    'etl_dim_tempo',
    default_args=default_args,
    schedule=None, 
    catchup=False,
    tags=['dw', 'adventureworks']
) as dag:

    task_tempo = PythonOperator(
        task_id='etl_tempo_task',
        python_callable=generate_load_tempo
    )

    task_tempo