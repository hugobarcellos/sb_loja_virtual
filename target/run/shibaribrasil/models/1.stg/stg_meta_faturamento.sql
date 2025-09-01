

  create or replace view `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_meta_faturamento`
  OPTIONS(
      description=""""""
    )
  as 

with cte_meta as (
   select cast(trim(dt_prim_dia_mes) as date) as dt_prim_dia_mes
         ,vl_objetivo_total                   as vl_objetivo_total 
         ,vl_objetivo_shibari                 as vl_objetivo_shibari 
         ,vl_objetivo_curadoria               as vl_objetivo_curadoria
     from `igneous-sandbox-381622`.`datalake_drive`.`drive_meta_faturamento`
)

select *
  from cte_meta;

