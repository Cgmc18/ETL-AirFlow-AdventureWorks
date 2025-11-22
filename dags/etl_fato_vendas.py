from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd

DATA_PATH = '/opt/airflow/dags/data'

def extract_transform_load_fato():
    # --- 1. EXTRACT (CSVs de Vendas) ---
    print("Lendo CSVs de Vendas...")
    df_header = pd.read_csv(f'{DATA_PATH}/Sales SalesOrderHeader.csv', sep=',')
    df_detail = pd.read_csv(f'{DATA_PATH}/Sales SalesOrderDetail.csv', sep=',')

    # --- 2. EXTRACT (Dimensões do Banco para pegar os SKs) ---
    print("Lendo Dimensões do Banco de Dados...")
    pg_hook = PostgresHook(postgres_conn_id='postgres_dw')
    engine = pg_hook.get_sqlalchemy_engine()
    
    # Lemos apenas as colunas de ID para fazer o "De -> Para"
    df_dim_produto = pd.read_sql("SELECT sk_produto, id_produto_original FROM dim_produto", con=engine)
    df_dim_cliente = pd.read_sql("SELECT sk_cliente, id_cliente_original FROM dim_cliente", con=engine)
    df_dim_vendedor = pd.read_sql("SELECT sk_vendedor, id_vendedor_original FROM dim_vendedor", con=engine)
    df_dim_localidade = pd.read_sql("SELECT sk_localidade, id_endereco_original FROM dim_localidade", con=engine)

    # --- 3. TRANSFORM ---
    print("Iniciando Transformações...")
    
    # A. Unir Cabeçalho e Detalhe (SalesOrderID)
    df_fato = pd.merge(df_header, df_detail, on='SalesOrderID', how='inner')

    # B. Calcular sk_tempo (Baseado na OrderDate)
    # A data vem como string (ex: "2011-05-31 00:00:00"), precisamos transformar em inteiro YYYYMMDD
    df_fato['dt_temp'] = pd.to_datetime(df_fato['OrderDate'])
    df_fato['sk_tempo'] = (
        df_fato['dt_temp'].dt.year * 10000 + 
        df_fato['dt_temp'].dt.month * 100 + 
        df_fato['dt_temp'].dt.day
    ).astype(int)

    # C. Cruzar com Dimensão Produto (Lookup)
    df_fato = pd.merge(
        df_fato, 
        df_dim_produto, 
        left_on='ProductID', 
        right_on='id_produto_original', 
        how='left'
    )

    # D. Cruzar com Dimensão Cliente
    df_fato = pd.merge(
        df_fato, 
        df_dim_cliente, 
        left_on='CustomerID', 
        right_on='id_cliente_original', 
        how='left'
    )

    # E. Cruzar com Dimensão Vendedor
    # Nota: Alguns pedidos são online e não têm vendedor (SalesPersonID pode ser null)
    df_fato = pd.merge(
        df_fato, 
        df_dim_vendedor, 
        left_on='SalesPersonID', 
        right_on='id_vendedor_original', 
        how='left'
    )

    # F. Cruzar com Dimensão Localidade (Usamos ShipToAddressID)
    df_fato = pd.merge(
        df_fato, 
        df_dim_localidade, 
        left_on='ShipToAddressID', 
        right_on='id_endereco_original', 
        how='left'
    )

    # --- 4. CÁLCULO DE MÉTRICAS E LIMPEZA ---
    
    # Calcular Valor Total da Linha (se já não existir no CSV como LineTotal)
    # Fórmula: (Preço * Qtd) * (1 - Desconto)
    # Mas o AdventureWorks já traz UnitPrice e OrderQty. Vamos recalcular para garantir.
    df_fato['valor_total_linha'] = (df_fato['UnitPrice'] * df_fato['OrderQty']) * (1 - df_fato['UnitPriceDiscount'])
    
    # Selecionar colunas finais para a Fato
    df_final = pd.DataFrame()
    df_final['sk_produto'] = df_fato['sk_produto']
    df_final['sk_cliente'] = df_fato['sk_cliente']
    df_final['sk_tempo'] = df_fato['sk_tempo']
    df_final['sk_localidade'] = df_fato['sk_localidade']
    df_final['sk_vendedor'] = df_fato['sk_vendedor']
    
    # Métricas
    df_final['qtd_vendida'] = df_fato['OrderQty']
    df_final['valor_unitario'] = df_fato['UnitPrice']
    df_final['valor_desconto'] = (df_fato['UnitPrice'] * df_fato['OrderQty']) * df_fato['UnitPriceDiscount']
    df_final['valor_total'] = df_fato['valor_total_linha']

    # Tratar Nulos nas Chaves Estrangeiras (Caso não ache a dimensão, remover ou setar nulo)
    # Para integridade referencial, vamos remover linhas que não tenham produto ou cliente (lixo)
    df_final = df_final.dropna(subset=['sk_produto', 'sk_cliente', 'sk_tempo'])
    
    # Vendedor pode ser nulo (venda online), localidade também se der erro de join
    # Mas no Postgres definimos como FK? Se sim, não pode ser null. 
    # Vamos preencher nulos com um valor padrão se precisar, ou deixar passar se a coluna aceitar null.
    # No script SQL original, não colocamos "NOT NULL" nas FKs, então pode passar.

    # --- 5. LOAD ---
    print(f"Carregando {len(df_final)} linhas na Fato Vendas...")
    
    with engine.connect() as connection:
        connection.execute("TRUNCATE TABLE public.fato_vendas RESTART IDENTITY CASCADE;")
        
    # Inserção em lotes (chunksize) para não travar se for muitos dados
    df_final.to_sql(
        'fato_vendas', 
        con=engine, 
        schema='public', 
        if_exists='append', 
        index=False,
        chunksize=5000 
    )
    print("Carga Fato Finalizada com Sucesso!")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    'etl_fato_vendas',
    default_args=default_args,
    schedule=None, 
    catchup=False,
    tags=['dw', 'adventureworks']
) as dag:

    task_fato = PythonOperator(
        task_id='etl_fato_task',
        python_callable=extract_transform_load_fato
    )