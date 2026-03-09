
  
    

    create or replace table `igneous-sandbox-381622`.`dbt_dw_az`.`tb_pedido_agg_mes`
      
    
    

    OPTIONS(
      description=""""""
    )
    as (
      


with cte_base as (
  select *
    from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_pedido_agg_dia`
)

, cte_meta as (
  select *
    from `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_meta_faturamento`
)

   select nr_ano                                                                                                  as nr_ano
         ,if(length(cast(nr_mes as string)) = 1, 
             concat('0', cast(nr_mes as string), ' - ', ds_mes_abreviado), 
             concat(cast(nr_mes as string), ' - ', ds_mes_abreviado))                                             as ds_mes_completo
         ,qt_dias_mes                                                                                             as qt_dias_mes
         ,coalesce(a.vl_objetivo_total,0)                                                                           as vl_objetivo_total
         ,count(distinct cd_contato)                                                                              as cd_contato
         ,count(distinct cd_pedido)                                                                               as cd_pedido
         ,sum(qt_item)                                                                                            as qt_item
         ,sum(vl_total_pedido)                                                                                    as vl_faturamento_bruto
         ,sum(vl_frete_rateio)                                                                                    as vl_frete
         ,sum(vl_total_pedido) * 0.05                                                                             as vl_taxa_aproximada
         ,sum(vl_total_pedido) -(sum(vl_frete_rateio) + (sum(vl_total_pedido) * 0.05))                            as vl_faturamento_liquido
         ,sum(vl_custo_pedido)                                                                                    as vl_custo_merc_vendida
         ,(sum(vl_total_pedido) -(sum(vl_frete_rateio) + (sum(vl_total_pedido) * 0.05))) - sum(vl_custo_pedido)   as vl_lucro_bruto
     from cte_base as a
left join cte_meta as b
       on a.dt_prim_dia_mes = b.dt_prim_dia_mes
   where a.dt_prim_dia_mes < date_trunc(current_date, month)
group by nr_ano
        ,nr_mes
        ,ds_mes_abreviado
        ,qt_dias_mes
        ,a.vl_objetivo_total
order by nr_ano desc
        ,nr_mes desc
    );
  