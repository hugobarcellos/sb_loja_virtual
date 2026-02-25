
  
    

    create or replace table `igneous-sandbox-381622`.`dbt_dw_az`.`tb_pedido_agg_dia`
      
    
    

    OPTIONS(
      description=""""""
    )
    as (
      


with cte_pedido as (
   select cd_codigo_interno
         ,cd_pedido
         ,dt_prim_dia_mes
         ,cast(dt_pedido as date) dt_pedido
         ,cd_status_pedido
         ,ds_status_pedido
         ,cd_produto_bling
         ,cd_produto
         ,nm_produto
         ,nm_produto_completo
         ,ds_subcategoria
         ,ds_categoria
         ,ds_classificacao_produto
         ,ds_origem_produto
         ,qt_item
         ,vl_item
         ,vl_total_item
         ,vl_desconto_rateio
         ,vl_frete_rateio
         ,vl_total_pedido
         ,vl_custo_item
         ,vl_custo_pedido
         ,ds_forma_pagamento
         ,cd_contato
         ,nm_contato
         ,nr_doc_contato
         ,nm_loja
         ,ds_tipo_loja
         ,qt_linhas_pedido
    from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_pedido`
)

, cte_pedido_agg as (
  select dt_pedido
        ,count(distinct cd_pedido) qt_pedido
        ,sum(qt_item)              qt_item
        ,sum(vl_total_item)        vl_total_item
        ,sum(vl_desconto_rateio)   vl_desconto_rateio
        ,sum(vl_frete_rateio)      vl_frete_rateio
        ,sum(vl_total_pedido)      vl_total_pedido
        ,sum(vl_custo_pedido)      vl_custo_pedido
        ,min(dt_pedido) over ()    dt_pedido_min
        ,max(dt_pedido) over ()    dt_pedido_max
    from cte_pedido
   where cd_status_pedido not in ('CANCELADO')
group by dt_pedido
)

, cte_tempo as (
  select dt_data
        ,nr_ano
        ,nr_mes
        ,nr_dia
        ,nr_semana
        ,dt_prim_dia_mes
        ,ds_dia_semana
        ,ds_dia_semana_abreviado
        ,ds_mes
        ,ds_mes_abreviado
        ,ds_trimestre
        ,ds_semestre
        ,fg_dia_util
        ,fg_feriado
        ,ds_feriado
    from `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_tempo`
)

, cte_join as (
    select a.dt_data
          ,a.nr_ano
          ,a.nr_mes
          ,a.nr_dia
          ,a.nr_semana
          ,a.dt_prim_dia_mes
          ,a.ds_dia_semana
          ,a.ds_dia_semana_abreviado
          ,a.ds_mes
          ,a.ds_mes_abreviado
          ,a.ds_trimestre
          ,a.ds_semestre
          ,a.fg_dia_util
          ,a.fg_feriado
          ,a.ds_feriado
          ,coalesce(b.qt_pedido, 0)          as qt_pedido
          ,coalesce(b.qt_item, 0)            as qt_item
          ,coalesce(b.vl_total_item, 0)      as vl_total_item
          ,coalesce(b.vl_desconto_rateio, 0) as vl_desconto_rateio
          ,coalesce(b.vl_frete_rateio, 0)    as vl_frete_rateio
          ,coalesce(b.vl_total_pedido, 0)    as vl_total_pedido
          ,coalesce(b.vl_custo_pedido, 0)    as vl_custo_pedido
     from cte_tempo      as a
left join cte_pedido_agg as b
       on cast(a.dt_data as date) = cast(b.dt_pedido as date)
)

  select *
    from cte_join
   where dt_data >= '2023-11-01'
     and dt_data <= current_date
order by dt_data desc
    );
  