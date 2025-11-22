-- 1. Criação da Dimensão Tempo
-- Nota: sk_tempo geralmente não é serial, usa-se o formato YYYYMMDD (ex: 20251121)
CREATE TABLE IF NOT EXISTS public.dim_tempo (
    sk_tempo INT PRIMARY KEY,
    data_completa DATE NOT NULL,
    ano INT NOT NULL,
    mes INT NOT NULL,
    nome_mes VARCHAR(20) NOT NULL,
    trimestre INT NOT NULL,
    semestre INT NOT NULL
);

-- 2. Criação da Dimensão Produto
CREATE TABLE IF NOT EXISTS public.dim_produto (
    sk_produto SERIAL PRIMARY KEY,       -- Chave Surrogada (Auto-incremento)
    id_produto_original INT,             -- Business Key (ID do AdventureWorks)
    nome_produto VARCHAR(255),
    nome_categoria VARCHAR(100),
    nome_subcategoria VARCHAR(100),
    cor VARCHAR(50)
);

-- 3. Criação da Dimensão Cliente
CREATE TABLE IF NOT EXISTS public.dim_cliente (
    sk_cliente SERIAL PRIMARY KEY,
    id_cliente_original INT,
    nome_completo VARCHAR(255),
    tipo_cliente VARCHAR(50)             -- Ex: 'Individual', 'Store'
);

-- 4. Criação da Dimensão Localidade
CREATE TABLE IF NOT EXISTS public.dim_localidade (
    sk_localidade SERIAL PRIMARY KEY,
    id_endereco_original INT,
    cidade VARCHAR(100),
    estado VARCHAR(100),
    pais VARCHAR(100)
);

-- 5. Criação da Dimensão Vendedor
CREATE TABLE IF NOT EXISTS public.dim_vendedor (
    sk_vendedor SERIAL PRIMARY KEY,
    id_vendedor_original INT,
    nome_vendedor VARCHAR(255),
    cargo VARCHAR(100)
);

-- 6. Criação da Tabela Fato Vendas
-- Esta tabela conecta todas as dimensões e guarda as métricas
CREATE TABLE IF NOT EXISTS public.fato_vendas (
    id_venda SERIAL PRIMARY KEY,
    
    -- Chaves Estrangeiras para as Dimensões
    sk_produto INT REFERENCES public.dim_produto(sk_produto),
    sk_cliente INT REFERENCES public.dim_cliente(sk_cliente),
    sk_tempo INT REFERENCES public.dim_tempo(sk_tempo),
    sk_localidade INT REFERENCES public.dim_localidade(sk_localidade),
    sk_vendedor INT REFERENCES public.dim_vendedor(sk_vendedor),
    
    -- Métricas (KPIs)
    qtd_vendida INT,
    valor_unitario NUMERIC(18, 2),       -- Numeric é melhor para dinheiro que Float
    valor_desconto NUMERIC(18, 2),
    valor_total NUMERIC(18, 2)
);

-- Índices para melhorar a performance das consultas dos KPIs
CREATE INDEX idx_fato_tempo ON public.fato_vendas(sk_tempo);
CREATE INDEX idx_fato_produto ON public.fato_vendas(sk_produto);
CREATE INDEX idx_fato_vendedor ON public.fato_vendas(sk_vendedor);
CREATE INDEX idx_fato_localidade ON public.fato_vendas(sk_localidade);
CREATE INDEX idx_fato_cliente ON public.fato_vendas(sk_cliente);