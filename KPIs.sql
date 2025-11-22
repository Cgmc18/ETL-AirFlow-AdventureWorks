-- Indicadores Financeiros Gerais

-- KPI 1: Faturamento Bruto Total (Valor total das mercadorias antes dos descontos)
SELECT 
    SUM(qtd_vendida * valor_unitario) AS faturamento_bruto
FROM public.fato_vendas;

-- KPI 2: Faturamento Líquido Total (O dinheiro que realmente entrou no caixa)
SELECT 
    SUM(valor_total) AS faturamento_liquido
FROM public.fato_vendas;

-- KPI 3: Total de Descontos Concedidos (Quanto a empresa "deixou de ganhar" para fechar vendas)
SELECT 
    SUM(valor_desconto) AS total_descontos
FROM public.fato_vendas;

-- KPI 4: Quantidade Total de Produtos Vendidos (Volume de saída de estoque)
SELECT 
    SUM(qtd_vendida) AS total_itens_vendidos
FROM public.fato_vendas;


-- Análise de Produtos e Categorias

--KPI 5: Top 5 Produtos Mais Vendidos (Por Faturamento)
SELECT 
    p.nome_produto,
    SUM(f.valor_total) AS total_vendas
FROM public.fato_vendas f
JOIN public.dim_produto p ON f.sk_produto = p.sk_produto
GROUP BY p.nome_produto
ORDER BY total_vendas DESC
LIMIT 5;

-- KPI 6: Vendas por Categoria
SELECT 
    p.nome_categoria,
    SUM(f.valor_total) AS total_vendas,
    COUNT(*) AS quantidade_vendas
FROM public.fato_vendas f
JOIN public.dim_produto p ON f.sk_produto = p.sk_produto
GROUP BY p.nome_categoria
ORDER BY total_vendas DESC;


-- Tempo, Geografia e Pessoas

-- KPI 7: Faturamento por País
SELECT 
    l.pais,
    SUM(f.valor_total) AS total_vendas
FROM public.fato_vendas f
JOIN public.dim_localidade l ON f.sk_localidade = l.sk_localidade
GROUP BY l.pais
ORDER BY total_vendas DESC;

-- KPI 8: Sazonalidade Mensal (Evolução Temporal)
SELECT 
    t.ano,
    t.mes,
    t.nome_mes,
    SUM(f.valor_total) AS total_vendas
FROM public.fato_vendas f
JOIN public.dim_tempo t ON f.sk_tempo = t.sk_tempo
GROUP BY t.ano, t.mes, t.nome_mes
ORDER BY t.ano, t.mes;

-- KPI 9: Ranking de Melhores Vendedores
SELECT 
    v.nome_vendedor,
    SUM(f.valor_total) AS total_gerado
FROM public.fato_vendas f
JOIN public.dim_vendedor v ON f.sk_vendedor = v.sk_vendedor
WHERE v.nome_vendedor IS NOT NULL  -- Remove vendas online (sem vendedor)
GROUP BY v.nome_vendedor
ORDER BY total_gerado DESC
LIMIT 10;

-- KPI 10: Ticket Médio por Item
SELECT 
    AVG(valor_total) AS ticket_medio_item
FROM public.fato_vendas;