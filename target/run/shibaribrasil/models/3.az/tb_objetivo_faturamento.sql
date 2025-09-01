
  
    

    create or replace table `igneous-sandbox-381622`.`dbt_dw_az`.`tb_objetivo_faturamento`
      
    
    

    OPTIONS(
      description=""""""
    )
    as (
      

with cte_meta as (
  select dt_prim_dia_mes
        ,vl_objetivo_total
        ,vl_objetivo_shibari
        ,vl_objetivo_curadoria
    from `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_meta_faturamento`
)

, cte_tempo as (
  select distinct
         dt_prim_dia_mes
        ,dt_data
        ,nr_dia
    from `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_tempo`
)

, cte_meta_dia as (
   select a.dt_prim_dia_mes
         ,count(distinct b.dt_data) qt_dias_mes
         ,a.vl_objetivo_total
         ,round(safe_divide(a.vl_objetivo_total, count(distinct b.dt_data)), 2) vl_meta_dia
     from cte_meta  as a
left join cte_tempo as b
       on a.dt_prim_dia_mes = b.dt_prim_dia_mes
 group by a.dt_prim_dia_mes
         ,a.vl_objetivo_total
)

, cte_base as (
   select a.dt_prim_dia_mes
         ,b.dt_data
         ,b.nr_dia
         ,round((b.nr_dia * c.vl_meta_dia), 2) vl_meta_dia_acumulado
         ,c.vl_meta_dia
         ,a.vl_objetivo_total
     from cte_meta     as a
left join cte_tempo    as b
       on a.dt_prim_dia_mes = b.dt_prim_dia_mes
left join cte_meta_dia as c
       on a.dt_prim_dia_mes = c.dt_prim_dia_mes
)

select *
  from cte_base
order by 1,2
    );
  