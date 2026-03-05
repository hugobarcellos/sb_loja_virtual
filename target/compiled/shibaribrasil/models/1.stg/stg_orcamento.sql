

with cte_orcamento as (
   select dt_prim_dia_mes                     as dt_prim_dia_mes
         ,ds_categoria_despesa                as ds_categoria_despesa
         ,vl_orcado                           as vl_orcado 
     from `igneous-sandbox-381622`.`datalake_drive`.`drive_orcamento`
)

select *
  from cte_orcamento