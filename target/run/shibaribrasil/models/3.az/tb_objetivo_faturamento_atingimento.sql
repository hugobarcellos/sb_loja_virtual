
  
    

    create or replace table `igneous-sandbox-381622`.`dbt_dw_az`.`tb_objetivo_faturamento_atingimento`
      
    
    

    OPTIONS(
      description=""""""
    )
    as (
      

with cte_meta as (
  select  dt_prim_dia_mes
         ,dt_data
         ,nr_dia
         ,vl_meta_dia_acumulado
         ,vl_meta_dia
         ,vl_objetivo_total
     from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_objetivo_faturamento`
)

, cte_pedido as (
  select date_trunc(cast(dt_pedido as date), month) as dt_prim_dia_mes
        ,dt_pedido
        ,sum(vl_total_pedido)          vl_total_pedido
        ,sum(vl_frete_rateio)          vl_frete
        ,(sum(vl_total_pedido) * 0.05) vl_taxa
        ,sum(vl_custo_pedido)          vl_custo_total_pedido
    from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_pedido`
   where date_trunc(cast(dt_pedido as date), month) >= '2025-08-01'
     and ds_status_pedido not in ('CANCELADO')
group by dt_pedido
)

   select a.dt_prim_dia_mes
         ,a.dt_data
         ,b.vl_total_pedido
         ,b.vl_frete
         ,b.vl_taxa
         ,b.vl_custo_total_pedido
         ,a.vl_meta_dia_acumulado
         ,a.vl_meta_dia
         ,a.vl_objetivo_total
     from cte_meta   as a
left join cte_pedido as b
       on cast(a.dt_data as date) = cast(b.dt_pedido as date)
    );
  