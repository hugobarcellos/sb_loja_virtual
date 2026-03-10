
  
    

    create or replace table `igneous-sandbox-381622`.`dbt_dw_az`.`tb_resultado_final`
      
    
    

    OPTIONS(
      description=""""""
    )
    as (
      

with cte_pedido_base as (
  select dt_prim_dia_mes               as dt_prim_dia_mes
        ,count(distinct cd_pedido)     as qt_pedido
        ,count(distinct cd_contato)    as qt_cliente
        ,sum(qt_item)                  as qt_item
        ,sum(vl_total_pedido)          as vl_faturamento_bruto
        ,sum(vl_frete_rateio)          as vl_frete
        ,(sum(vl_total_pedido) * 0.05) as vl_taxa_aproximada
        ,sum(vl_custo_pedido)          as vl_custo_mercadoria_vendida
    from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_pedido`
   where ds_status_pedido not in ('CANCELADO')
group by dt_prim_dia_mes
)

, cte_pedido as (
  select dt_prim_dia_mes                                                                                       as dt_prim_dia_mes
        ,cast(qt_pedido as float64)                                                                            as qt_pedido 
        ,cast(qt_cliente as float64)                                                                           as qt_cliente
        ,cast(qt_item as float64)                                                                              as qt_item
        ,cast(vl_faturamento_bruto as float64)                                                                 as vl_faturamento_bruto
        ,cast(vl_frete as float64)                                                                             as vl_frete
        ,cast(vl_taxa_aproximada as float64)                                                                   as vl_taxa_aproximada
        ,cast((vl_faturamento_bruto - (vl_frete + vl_taxa_aproximada)) as float64)                             as vl_faturamento_liquido
        ,cast(vl_custo_mercadoria_vendida as float64)                                                          as vl_custo_mercadoria_vendida
        ,cast(vl_faturamento_bruto - (vl_frete + vl_taxa_aproximada - vl_custo_mercadoria_vendida) as float64) as vl_lucro_bruto
    from cte_pedido_base
)

, cte_contas as (
  select date_trunc(cast(dt_vencimento as date), month) as dt_prim_dia_mes
        ,cast(sum(vl_valor) as float64)                 as vl_contas
    from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_contas_pagar`
group by 1
)

, cte_base as (
    select a.*
          ,b.vl_contas
          ,(a.vl_faturamento_liquido - b.vl_contas) as vl_lucro_liquido
      from cte_pedido   as a
 left join cte_contas   as b
        on a.dt_prim_dia_mes = b.dt_prim_dia_mes
)

, cte_porcentagens as (
    select dt_prim_dia_mes                                                    as dt_prim_dia_mes
          ,qt_pedido                                                          as qt_pedido
          ,qt_cliente                                                         as qt_cliente
          ,safe_divide(qt_cliente, qt_pedido)                                 as qt_pedido_cliente
          ,qt_item                                                            as qt_item
          ,safe_divide(qt_item, qt_pedido)                                    as qt_item_pedido
          ,vl_faturamento_bruto                                               as vl_faturamento_bruto
          ,vl_frete                                                           as vl_frete
          ,vl_taxa_aproximada                                                 as vl_taxa_aproximada
          ,(vl_frete + vl_taxa_aproximada)                                    as vl_impostos_totais
          ,safe_divide((vl_frete + vl_taxa_aproximada), vl_faturamento_bruto) as pr_impostos
          ,vl_faturamento_liquido                                             as vl_faturamento_liquido
          ,vl_custo_mercadoria_vendida                                        as vl_custo_mercadoria_vendida
          ,safe_divide(vl_custo_mercadoria_vendida, vl_faturamento_liquido)   as pr_cmv
          ,vl_lucro_bruto                                                     as vl_lucro_bruto
          ,safe_divide(vl_lucro_bruto, vl_faturamento_liquido)                as pr_lucro_bruto
          ,vl_contas                                                          as vl_contas
          ,safe_divide(vl_contas, vl_lucro_bruto)                             as pr_contas_lucro_bruto
          ,vl_lucro_liquido                                                   as vl_lucro_liquido
          ,safe_divide(vl_lucro_liquido, vl_lucro_bruto)                      as pr_lucro_liquido
          ,safe_divide(vl_faturamento_liquido, qt_pedido)                     as vl_ticket_medio
          ,safe_divide(vl_faturamento_liquido, qt_item)                       as vl_preco_medio
          ,safe_divide(vl_custo_mercadoria_vendida, qt_item)                  as vl_custo_medio
      from cte_base 
)

, cte_pivot as (
 select dt_prim_dia_mes
       ,nm_indicador
       ,vl_indicador
   from cte_porcentagens
unpivot ( vl_indicador for nm_indicador in (
          qt_pedido
         ,qt_cliente
         ,qt_pedido_cliente
         ,qt_item
         ,qt_item_pedido
         ,vl_faturamento_bruto
         ,vl_frete
         ,vl_taxa_aproximada
         ,vl_impostos_totais
         ,pr_impostos
         ,vl_faturamento_liquido
         ,vl_custo_mercadoria_vendida
         ,pr_cmv
         ,vl_lucro_bruto
         ,pr_lucro_bruto
         ,vl_contas
         ,pr_contas_lucro_bruto
         ,vl_lucro_liquido
         ,pr_lucro_liquido
         ,vl_ticket_medio
         ,vl_preco_medio
         ,vl_custo_medio
     )
  )
)

select *
  from cte_pivot
    );
  