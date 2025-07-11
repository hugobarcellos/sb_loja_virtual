

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
          ,a.ds_subcategoria
          ,a.ds_categoria
          ,a.ds_classificacao_produto
          ,a.ds_origem_produto
          ,a.ds_tipo_produto
          ,a.fg_alteracao_preco
          ,a.ds_tipo_alteracao_preco
          ,a.dt_alteracao_preco
          ,a.vl_alteracao_preco
      from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_produto`  as a   
     where a.ds_categoria not in ('Suprimentos', 'Inativos')
)

, cte_meta_margem as (
    select ds_classificacao_produto
          ,ds_origem_produto
          ,pr_desconto_padrao 
          ,pr_meta_margem 
      from `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_meta_margem_preco`
)

, cte_forma_pagamento as (
   select cd_forma_pagamento
         ,nm_forma_pagamento
         ,ds_condicao_pagamento
         ,qt_dias_pagamento
         ,vl_taxa_aliquota
         ,vl_taxa_fixa
     from `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_forma_pagamento`
    where nm_forma_pagamento in ('[Nuvem] Cartão de Crédito', '[Nuvem] PIX')
)

, cte_joins as (
    select distinct
           a.cd_produto_bling
          ,a.cd_produto
          ,a.cd_codigo_barras
          ,a.nm_produto
          ,a.ds_variacao
          ,a.qt_estoque_atual
          ,coalesce(a.vl_custo_compra, 0) as vl_custo_compra
          ,coalesce(a.vl_custo_total, 0)  as vl_custo_total
          ,coalesce(a.vl_preco_venda, 0)  as vl_preco_venda
          ,a.ds_subcategoria
          ,a.ds_categoria
          ,a.ds_classificacao_produto
          ,a.ds_origem_produto
          ,a.ds_tipo_produto
          ,a.fg_alteracao_preco
          ,a.ds_tipo_alteracao_preco
          ,a.dt_alteracao_preco
          ,coalesce(a.vl_alteracao_preco, 0)                                                                                 as vl_alteracao_preco
          ,coalesce(b.pr_desconto_padrao, 0)                                                                                 as pr_desconto_padrao 
          ,coalesce(b.pr_meta_margem, 0)                                                                                     as pr_meta_margem 
          ,(select (vl_taxa_aliquota / 100) from cte_forma_pagamento where nm_forma_pagamento = '[Nuvem] Cartão de Crédito') as tx_aliquota_cartao
          ,(select vl_taxa_fixa     from cte_forma_pagamento where nm_forma_pagamento = '[Nuvem] Cartão de Crédito')         as tx_fixa_cartao
          ,(select (vl_taxa_aliquota / 100) from cte_forma_pagamento where nm_forma_pagamento = '[Nuvem] PIX')               as tx_aliquota_pix
          --regra manual de desconto do pix
          ,0.03                                                                                                              as vl_desconto_fixo_pix
          --valor aproximado de materiais de envio por pedido
          ,2.50                                                                                                              as vl_materiais_envio
     from cte_produto     as a
left join cte_meta_margem as b
       on a.ds_classificacao_produto = b.ds_classificacao_produto
      and a.ds_origem_produto = b.ds_origem_produto
)

  select *
    from cte_joins
order by nm_produto
        ,ds_variacao