-- back compat for old kwarg name
  
  
        
            
            
        
    

    

    merge into `igneous-sandbox-381622`.`dbt_dw_az`.`tb_atingimento_faturamento_dinamico` as DBT_INTERNAL_DEST
        using (

with cte_pedido_base as (
  select *
    from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_pedido_agg_dia`
--    where dt_prim_dia_mes = '2026-03-01'
)

, cte_pedido as (
    select dt_data                                                                                                 as dt_data
          ,dt_prim_dia_mes                                                                                         as dt_prim_dia_mes
          ,qt_dias_mes                                                                                             as qt_dias_mes
          ,extract(day from dt_data)                                                                               as qt_dias_passados
          ,(qt_dias_mes - extract(day from dt_data)) + 1                                                           as qt_dias_restantes
          ,if(dt_data < current_date, true, false)                                                                 as fg_dia_passado                                                
          ,vl_objetivo_total                                                                                       as vl_objetivo_total
          ,round(vl_objetivo_total / qt_dias_mes ,2)                                                               as vl_objetivo_dia
          ,round((vl_objetivo_total / qt_dias_mes) *  extract(day from dt_data) ,2)                                as vl_objetivo_dia_acumulado
          ,count(distinct cd_pedido)                                                                               as cd_pedido
          ,sum(qt_item)                                                                                            as qt_item
          ,sum(vl_total_pedido)                                                                                    as vl_faturamento_bruto
          ,sum(vl_frete_rateio)                                                                                    as vl_frete
          ,sum(vl_total_pedido) * 0.05                                                                             as vl_taxa_aproximada
          ,sum(vl_total_pedido) -(sum(vl_frete_rateio) + (sum(vl_total_pedido) * 0.05))                            as vl_faturamento_liquido
          ,sum(vl_custo_pedido)                                                                                    as vl_custo_merc_vendida
          ,(sum(vl_total_pedido) -(sum(vl_frete_rateio) + (sum(vl_total_pedido) * 0.05))) - sum(vl_custo_pedido)   as vl_lucro_bruto
     from cte_pedido_base as a
 group by dt_data
         ,dt_prim_dia_mes
         ,qt_dias_mes
         ,vl_objetivo_total
)

, base as (
  select dt_data
        ,vl_objetivo_total
        ,vl_objetivo_dia
        ,vl_faturamento_bruto
        ,date_trunc(dt_data, month) as mes_ref
        ,last_day(dt_data, month) as dt_fim_mes
   from cte_pedido
  
    -- em incremental, só recalcula o mês atual (inclui passado do mês, hoje e futuro do mês)
    where dt_data >= date_trunc(current_date(), month)
      and dt_data <= last_day(current_date(), month)
  
)

-- traz valor já calculado anteriormente (para "congelar" o passado)
, existing as (
  
  select
    dt_data,
    meta_diaria_ajustada
  from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_atingimento_faturamento_dinamico`
  
)

-- déficit acumulado ATÉ ONTEM (por mês)
, acum as (
   select mes_ref
         ,sum(case when dt_data < current_date() then vl_objetivo_dia else 0 end) as objetivo_ate_ontem
         ,sum(case when dt_data < current_date() then vl_faturamento_bruto else 0 end) as faturamento_ate_ontem
         ,max(dt_fim_mes) as dt_fim_mes
    from base
group by 1
)

, params as (
  select mes_ref
        ,greatest(0, objetivo_ate_ontem - faturamento_ate_ontem) as deficit
        ,(date_diff(dt_fim_mes, current_date(), day) + 1) as dias_restantes
   from acum
)

-- meta ajustada de ontem (somente se ontem for do mesmo mês)
, ontem as (
  
  select meta_diaria_ajustada as meta_ajustada_ontem
  from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_atingimento_faturamento_dinamico`
  where dt_data = date_sub(current_date(), interval 1 day)
    and date_trunc(dt_data, month) = date_trunc(current_date(), month)
  
),

calc as (
  select
    b.*,
    p.deficit,
    nullif(p.dias_restantes, 0) as dias_restantes,
    e.meta_diaria_ajustada as meta_ajustada_existente,
    y.meta_diaria_ajustada as meta_ajustada_ontem
  from base b
  join params p
    on p.mes_ref = b.mes_ref

  left join existing e
    on e.dt_data = b.dt_data

  
    -- pega a meta ajustada de ontem (só existe depois da 1ª carga)
    left join `igneous-sandbox-381622`.`dbt_dw_az`.`tb_atingimento_faturamento_dinamico` y
      on y.dt_data = date_sub(current_date(), interval 1 day)
     and date_trunc(y.dt_data, month) = date_trunc(current_date(), month)
  
)

   select dt_data
         ,vl_objetivo_total
         ,vl_objetivo_dia
         ,vl_faturamento_bruto
         ,deficit
         ,dias_restantes
         ,case
             -- passado: mantém o que já foi calculado (incremental); se não existir (full-refresh), usa objetivo do dia
             when dt_data < current_date() then coalesce(meta_ajustada_existente, vl_objetivo_dia)
             -- hoje e futuro:
             else
               case
                 when deficit > 0 then vl_objetivo_dia + (deficit / dias_restantes)
                 else coalesce(meta_ajustada_ontem, vl_objetivo_dia)
               end
             end as meta_diaria_ajustada
    from calc
order by dt_data
        ) as DBT_INTERNAL_SOURCE
        on (
                DBT_INTERNAL_SOURCE.dt_data = DBT_INTERNAL_DEST.dt_data
            )

    
    when matched then update set
        `dt_data` = DBT_INTERNAL_SOURCE.`dt_data`,`vl_objetivo_total` = DBT_INTERNAL_SOURCE.`vl_objetivo_total`,`vl_objetivo_dia` = DBT_INTERNAL_SOURCE.`vl_objetivo_dia`,`vl_faturamento_bruto` = DBT_INTERNAL_SOURCE.`vl_faturamento_bruto`,`deficit` = DBT_INTERNAL_SOURCE.`deficit`,`dias_restantes` = DBT_INTERNAL_SOURCE.`dias_restantes`,`meta_diaria_ajustada` = DBT_INTERNAL_SOURCE.`meta_diaria_ajustada`
    

    when not matched then insert
        (`dt_data`, `vl_objetivo_total`, `vl_objetivo_dia`, `vl_faturamento_bruto`, `deficit`, `dias_restantes`, `meta_diaria_ajustada`)
    values
        (`dt_data`, `vl_objetivo_total`, `vl_objetivo_dia`, `vl_faturamento_bruto`, `deficit`, `dias_restantes`, `meta_diaria_ajustada`)


    