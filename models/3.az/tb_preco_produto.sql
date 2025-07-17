{{ config(
    tags = ['az', 'produto'],
    enabled = true
)}}

--visão atual dos produtos, não trabalho aqui com histórico do preço
with cte_produto as (
    select distinct
           a.cd_produto_bling
          ,a.cd_produto
          ,a.cd_codigo_barras
          ,a.nm_produto
          ,a.nm_produto_completo
          ,a.ds_variacao
          ,a.qt_estoque_atual
          ,a.vl_custo_compra
          ,a.vl_custo_total
          ,a.vl_preco_venda
          ,a.vl_preco_venda_por
          ,a.ds_subcategoria
          ,a.ds_categoria
          ,a.ds_classificacao_produto
          ,a.ds_origem_produto
          ,a.ds_tipo_produto
          ,a.fg_produto_composicao
          ,a.dt_ultima_ingestao
      from {{ ref('tb_produto') }}  as a   
    --  where a.ds_categoria not in ('Suprimentos', 'Inativos')
)

, cte_meta_margem as (
    select ds_classificacao_produto
          ,ds_origem_produto
          ,pr_desconto_padrao 
          ,pr_meta_margem 
      from {{ ref('stg_meta_margem_preco') }}
)

, cte_forma_pagamento as (
   select cd_forma_pagamento
         ,nm_forma_pagamento
         ,ds_condicao_pagamento
         ,qt_dias_pagamento
         ,vl_taxa_aliquota
         ,vl_taxa_fixa
     from {{ ref('stg_forma_pagamento') }}
    where nm_forma_pagamento in ('[Nuvem] Cartão de Crédito', '[Nuvem] PIX')
)

, cte_compra_produto as (
    select cd_produto_bling
          ,cd_produto
          ,vl_item
          ,dt_compra
      from {{ ref('tb_agg_compra_produto') }}
     where nr_seq = 1
)

, cte_produto_base_kit as (
    select distinct cd_produto_bling_componente
      from {{ ref('tb_composicao_produto') }}
)

, cte_joins as (
    select distinct
           a.cd_produto_bling
          ,a.cd_produto
          ,a.cd_codigo_barras
          ,a.nm_produto
          ,a.ds_variacao
          ,a.qt_estoque_atual
          ,coalesce(a.vl_custo_total, 0)      as vl_custo_cadastro
          ,coalesce(c.vl_item, 0)             as vl_custo_ultima_compra
          ,coalesce(a.vl_preco_venda, 0)      as vl_preco_venda
          ,coalesce(a.vl_preco_venda_por, 0)  as vl_preco_venda_por
          ,a.ds_subcategoria
          ,a.ds_categoria
          ,a.ds_classificacao_produto
          ,a.ds_origem_produto
          ,a.ds_tipo_produto
          ,a.fg_produto_composicao
          ,if(d.cd_produto_bling_componente is null, false, true)                                                            as fg_produto_base_composicao
          ,coalesce(b.pr_desconto_padrao, 0)                                                                                 as pr_desconto_padrao 
          ,coalesce(b.pr_meta_margem, 0)                                                                                     as pr_meta_margem 
          ,(select (vl_taxa_aliquota / 100) from cte_forma_pagamento where nm_forma_pagamento = '[Nuvem] Cartão de Crédito') as tx_aliquota_cartao
          ,(select vl_taxa_fixa     from cte_forma_pagamento where nm_forma_pagamento = '[Nuvem] Cartão de Crédito')         as tx_fixa_cartao
          ,(select (vl_taxa_aliquota / 100) from cte_forma_pagamento where nm_forma_pagamento = '[Nuvem] PIX')               as tx_aliquota_pix
          --regra manual de desconto do pix
          ,0.03                                                                                                              as vl_desconto_fixo_pix
          --valor aproximado de materiais de envio por pedido
          ,2.50                                                                                                              as vl_materiais_envio
          ,c.dt_compra                                                                                                       as dt_ultima_compra
          ,a.dt_ultima_ingestao
          ,datetime(current_timestamp(), "America/Sao_Paulo") as dt_ultima_atualizacao
     from cte_produto        as a
left join cte_meta_margem    as b
       on a.ds_classificacao_produto = b.ds_classificacao_produto
      and a.ds_origem_produto = b.ds_origem_produto
left join cte_compra_produto as c
       on a.cd_produto_bling = c.cd_produto_bling
left join cte_produto_base_kit as d
       on a.cd_produto_bling = d.cd_produto_bling_componente
)

  select *
    from cte_joins
order by nm_produto
        ,ds_variacao