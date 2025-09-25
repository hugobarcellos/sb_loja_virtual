
  
    

    create or replace table `igneous-sandbox-381622`.`dbt_dw_az`.`tb_pedido`
      
    
    

    OPTIONS(
      description=""""""
    )
    as (
      

with cte_pedido as (
   select cd_codigo_interno
         ,cd_pedido
         ,dt_pedido
         ,cd_status_pedido
         ,ds_status_pedido
         ,vl_total_pedido
         ,vl_total_item
         ,cd_contato
         ,nm_contato
         ,nr_doc_contato
         ,cd_loja
         ,ds_loja
     from `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_pedido`
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
         ,ds_forma_pagamento
     from `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_item_pedido`
)

, cte_pedido_base as (
   select distinct
          a.cd_codigo_interno                                                        as cd_codigo_interno
         ,a.cd_pedido                                                                as cd_pedido
         ,a.dt_pedido                                                                as dt_pedido
         ,a.cd_status_pedido                                                         as cd_status_pedido
         ,a.ds_status_pedido                                                         as ds_status_pedido
         ,b.cd_produto_bling                                                         as cd_produto_bling
         ,b.cd_produto                                                               as cd_produto
         ,b.nm_produto_completo                                                      as nm_produto_completo
         ,b.qt_item                                                                  as qt_item
         ,b.vl_item                                                                  as vl_item
         ,round((b.qt_item * b.vl_item), 2)                                          as vl_total_item
         ,b.ds_tipo_desconto                                                         as ds_tipo_desconto
         ,b.vl_desconto                                                              as vl_desconto
         ,a.vl_total_pedido                                                          as vl_total_pedido
         ,a.vl_total_item                                                            as vl_total_item_pedido
         ,b.ds_forma_pagamento                                                       as ds_forma_pagamento
         ,a.cd_contato                                                               as cd_contato
         ,a.nm_contato                                                               as nm_contato
         ,a.nr_doc_contato                                                           as nr_doc_contato
         ,a.ds_loja                                                                  as ds_loja
         ,count(distinct b.cd_produto_bling) over (partition by a.cd_codigo_interno) as qt_linhas_pedido
     from cte_pedido        as a
left join cte_item_pedido   as b
       on a.cd_codigo_interno = b.cd_codigo_interno
)

, cte_rateio_frete as (
   select distinct
          a.cd_codigo_interno
         ,a.cd_pedido
         ,a.dt_pedido
         ,a.cd_status_pedido
         ,a.ds_status_pedido
         ,a.cd_produto_bling
         ,a.cd_produto
         ,a.nm_produto_completo
         ,a.qt_item
         ,a.vl_item
         ,a.vl_total_item
         ,round((a.vl_desconto / a.qt_linhas_pedido), 2)                                                as vl_desconto
         ,round(((a.vl_total_pedido + a.vl_desconto) - a.vl_total_item_pedido) / a.qt_linhas_pedido, 2) as vl_frete_rateio
         ,a.ds_forma_pagamento
         ,a.cd_contato
         ,a.nm_contato
         ,a.nr_doc_contato
         ,a.ds_loja
         ,a.qt_linhas_pedido
     from cte_pedido_base as a
)

, cte_pedido_total as (
   select distinct
          a.cd_codigo_interno
         ,a.cd_pedido
         ,a.dt_pedido
         ,a.cd_status_pedido
         ,a.ds_status_pedido
         ,a.cd_produto_bling
         ,a.cd_produto
         ,a.nm_produto_completo
         ,a.qt_item
         ,a.vl_item
         ,a.vl_total_item
         ,a.vl_desconto as vl_desconto_rateio
         ,a.vl_frete_rateio
         ,round((a.vl_total_item + a.vl_frete_rateio) - a.vl_desconto, 2) as vl_total_pedido
         ,a.ds_forma_pagamento
         ,a.cd_contato
         ,a.nm_contato
         ,a.nr_doc_contato
         ,a.ds_loja
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
     from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_produto`
)

, cte_custo as (
    select dt_prim_dia_mes
          ,cd_produto_bling
          ,cd_produto
          ,nm_produto
          ,vl_custo_final
     from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_hist_preco_custo_simples`
)

, cte_final as (
    select distinct
          a.cd_codigo_interno
         ,a.cd_pedido
         ,date_trunc(cast(dt_pedido as date), month) as dt_prim_dia_mes
         ,a.dt_pedido
         ,a.cd_status_pedido
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
         ,a.vl_total_pedido
         ,coalesce(nullif(c.vl_custo_final,0), vl_custo_cadastro)               as vl_custo_item
         ,(coalesce(nullif(c.vl_custo_final,0), vl_custo_cadastro) * a.qt_item) as vl_custo_pedido
         ,a.ds_forma_pagamento
         ,a.cd_contato
         ,a.nm_contato
         ,a.nr_doc_contato
         ,a.ds_loja
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
    );
  