

  create or replace view `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_tempo`
  OPTIONS(
      description=""""""
    )
  as 

with cte_drive as (
   select cast(dt_data as date)         as dt_data 	
         ,cast(nr_ano as int64)         as nr_ano	
         ,cast(nr_mes as int64)         as nr_mes	
         ,cast(nr_dia as int64)         as nr_dia	
         ,cast(nr_semana as int64)      as nr_semana	
         ,cast(dt_prim_dia_mes as date) as dt_prim_dia_mes	
         ,cast(dt_ult_dia_mes as date)  as dt_ult_dia_mes	
         ,ds_dia_semana                 as ds_dia_semana	
         ,ds_dia_semana_abrev           as ds_dia_semana_abreviado	
         ,ds_mes                        as ds_mes	
         ,ds_mes_abrev                  as ds_mes_abreviado	
         ,ds_trimestre                  as ds_trimestre	
         ,ds_semestre                   as ds_semestre	
         ,ds_ano_mes                    as ds_ano_mes	
         ,cast(fg_dia_util as int64)    as fg_dia_util	
         ,cast(fg_feriado as int64)     as fg_feriado	
         ,ds_feriado                    as ds_feriado 
     from `igneous-sandbox-381622`.`datalake_drive`.`drive_tempo` 
)

select *
  from cte_drive;

