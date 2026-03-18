{{ config(
    tags = ['az', 'pedido', 'snaps'],
    enabled = true
)}}

with cte_pedido as (
   select cd_codigo_interno
         ,cd_pedido
         ,cd_pedido_loja
         ,dt_pedido
         ,cd_status_pedido
         ,ds_status_pedido
         ,vl_total_pedido
         ,vl_total_item
         ,cd_contato
         ,nm_contato
         ,nr_doc_contato
         ,cd_loja
     from {{ ref('stg_pedido') }}
)

, cte_loja as (
    select cd_loja
          ,nm_loja
          ,ds_tipo_loja
      from {{ ref('stg_canal_venda') }}
)

, cte_item_pedido as (
   select cd_codigo_interno
         ,dt_pedido
         ,cd_produto_bling
         ,cd_produto
         ,nm_produto_completo
         ,qt_item
         ,vl_item
         ,ds_tipo_desconto
         ,vl_desconto
         ,cd_forma_pagamento
     from {{ ref('stg_item_pedido') }}
)

, cte_forma_pagamento as (
    select cd_forma_pagamento
          ,nm_forma_pagamento
          ,qt_dias_pagamento
          ,vl_taxa_aliquota
          ,vl_taxa_fixa
      from {{ ref('stg_forma_pagamento') }}
)

, cte_pedido_base as (
   select distinct
          a.cd_codigo_interno                                                                      as cd_codigo_interno
         ,a.cd_pedido                                                                              as cd_pedido
         ,a.cd_pedido_loja                                                                         as cd_pedido_loja
         ,a.dt_pedido                                                                              as dt_pedido
         ,a.cd_status_pedido                                                                       as cd_status_pedido
         ,a.ds_status_pedido                                                                       as ds_status_pedido
         ,b.cd_produto_bling                                                                       as cd_produto_bling
         ,b.cd_produto                                                                             as cd_produto
         ,b.nm_produto_completo                                                                    as nm_produto_completo
         ,b.qt_item                                                                                as qt_item
         ,b.vl_item                                                                                as vl_item
         ,round((b.qt_item * b.vl_item), 2)                                                        as vl_total_item
         ,b.ds_tipo_desconto                                                                       as ds_tipo_desconto
         ,b.vl_desconto                                                                            as vl_desconto
         ,a.vl_total_pedido                                                                        as vl_total_pedido
         ,a.vl_total_item                                                                          as vl_total_item_pedido
         ,b.cd_forma_pagamento                                                                     as cd_forma_pagamento
         ,d.nm_forma_pagamento                                                                     as nm_forma_pagamento
         ,d.qt_dias_pagamento                                                                      as qt_dias_pagamento
         ,date_add(cast(a.dt_pedido as date), interval cast(qt_dias_pagamento as int64) day)       as dt_recebimento_pedido
         ,d.vl_taxa_aliquota                                                                       as vl_taxa_aliquota
         ,d.vl_taxa_fixa                                                                           as vl_taxa_fixa
         ,a.cd_contato                                                                             as cd_contato
         ,a.nm_contato                                                                             as nm_contato
         ,a.nr_doc_contato                                                                         as nr_doc_contato
         ,c.nm_loja                                                                                as nm_loja
         ,c.ds_tipo_loja                                                                           as ds_tipo_loja
         ,count(distinct b.cd_produto_bling) over (partition by a.cd_codigo_interno)               as qt_linhas_pedido
     from cte_pedido          as a
left join cte_item_pedido     as b
       on a.cd_codigo_interno = b.cd_codigo_interno
left join cte_loja            as c
       on a.cd_loja = c.cd_loja
left join cte_forma_pagamento as d
       on b.cd_forma_pagamento = d.cd_forma_pagamento
)

, cte_frete as (
    select cd_codigo_interno 
          ,dt_envio
          ,vl_frete
          ,fg_frete_gratis
    from {{ ref('tb_frete_envio') }}
)

, cte_rateio_frete as (
   select distinct
          a.cd_codigo_interno
         ,a.cd_pedido
         ,a.cd_pedido_loja
         ,a.dt_pedido
         ,a.cd_status_pedido
         ,a.ds_status_pedido
         ,a.cd_produto_bling
         ,a.cd_produto
         ,a.nm_produto_completo
         ,a.qt_item
         ,a.vl_item
         ,a.vl_total_item
         ,a.vl_total_pedido
         ,round((a.vl_desconto / a.qt_linhas_pedido), 2)   as vl_desconto_rateio
         ,case when dt_pedido >= '2026-02-26'
                then round((b.vl_frete / a.qt_linhas_pedido) , 2)
               else round(((a.vl_total_pedido + a.vl_desconto) - a.vl_total_item_pedido) / a.qt_linhas_pedido, 2) end as vl_frete_rateio
         ,a.nm_forma_pagamento
         ,a.qt_dias_pagamento
         ,a.dt_recebimento_pedido
         ,a.vl_taxa_aliquota
         ,a.vl_taxa_fixa
         ,a.cd_contato
         ,a.nm_contato
         ,a.nr_doc_contato
         ,a.nm_loja
         ,a.ds_tipo_loja
         ,b.dt_envio
         ,b.fg_frete_gratis
         ,a.qt_linhas_pedido
     from cte_pedido_base as a
left join cte_frete       as b
       on a.cd_codigo_interno = b.cd_codigo_interno
)

, cte_pedido_total as (
   select distinct
          a.cd_codigo_interno
         ,a.cd_pedido
         ,a.cd_pedido_loja
         ,a.dt_pedido
         ,a.cd_status_pedido
         ,a.ds_status_pedido
         ,a.cd_produto_bling
         ,a.cd_produto
         ,a.nm_produto_completo
         ,a.qt_item
         ,a.vl_item
         ,a.vl_total_item
         ,a.vl_desconto_rateio
         ,a.vl_frete_rateio
         ,a.vl_total_pedido as vl_total_pedido1
         ,case when a.fg_frete_gratis is true 
                    then round((a.vl_total_item - a.vl_desconto_rateio), 2)
                else round((a.vl_total_item + a.vl_frete_rateio) - a.vl_desconto_rateio, 2) end as vl_total_pedido
         ,a.nm_forma_pagamento
         ,a.qt_dias_pagamento
         ,a.dt_recebimento_pedido
         ,a.vl_taxa_aliquota
         ,a.vl_taxa_fixa
         ,a.cd_contato
         ,a.nm_contato
         ,a.nr_doc_contato
         ,a.nm_loja
         ,a.ds_tipo_loja
         ,a.dt_envio
         ,a.fg_frete_gratis
         ,a.qt_linhas_pedido
     from cte_rateio_frete as a
)

, cte_produto as (
    select cd_produto_bling
          ,cd_produto
          ,nm_produto
          ,ds_subcategoria
          ,ds_categoria
          ,ds_classificacao_produto
          ,ds_origem_produto
          ,vl_custo_cadastro
     from {{ ref('tb_produto') }}
)

, cte_custo as (
    select dt_prim_dia_mes
          ,cd_produto_bling
          ,cd_produto
          ,nm_produto
          ,vl_custo_final
     from {{ ref('tb_hist_preco_custo_simples') }}
)

, cte_final as (
    select distinct
          a.cd_codigo_interno
         ,a.cd_pedido
         ,a.cd_pedido_loja
         ,date_trunc(cast(dt_pedido as date), month) as dt_prim_dia_mes
         ,a.dt_pedido
         ,a.ds_status_pedido
         ,a.cd_produto_bling
         ,a.cd_produto
         ,b.nm_produto
         ,a.nm_produto_completo
         ,b.ds_subcategoria
         ,b.ds_categoria
         ,b.ds_classificacao_produto
         ,b.ds_origem_produto
         ,a.qt_item
         ,a.vl_item
         ,a.vl_total_item
         ,a.vl_desconto_rateio
         ,a.vl_frete_rateio
         ,a.vl_total_pedido1
         ,a.vl_total_pedido
         ,round(coalesce(nullif(c.vl_custo_final,0), vl_custo_cadastro) ,2)                                   as vl_custo_item
         ,round((coalesce(nullif(c.vl_custo_final,0), vl_custo_cadastro) * a.qt_item) ,2)                     as vl_custo_pedido
         ,round((a.vl_total_pedido * (a.vl_taxa_aliquota / 100) ) + (a.vl_taxa_fixa / a.qt_linhas_pedido) ,2) as vl_taxa_pedido_rateio
         ,a.nm_forma_pagamento
         ,a.dt_recebimento_pedido
         ,a.cd_contato
         ,a.nm_contato
         ,a.nr_doc_contato
         ,a.nm_loja
         ,a.ds_tipo_loja
         ,a.dt_envio
         ,a.fg_frete_gratis
         ,a.qt_linhas_pedido
     from cte_pedido_total      as a
left join cte_produto           as b
       on a.cd_produto_bling = b.cd_produto_bling
left join cte_custo             as c
       on a.cd_produto_bling = c.cd_produto_bling
      and date_trunc(cast(a.dt_pedido as date), month) = cast(c.dt_prim_dia_mes as date)
)
select *
    from cte_final
