

with cte_snap as (
  select *
    from `igneous-sandbox-381622`.`snapshots`.`snapshot_status_pedido`
)

, cte_pedido as (
  select cd_codigo_interno
        ,cd_pedido_loja
        ,vl_total_item
        ,vl_total_pedido
    from `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_pedido`

)

, cte_item_pedido as (
  select cd_codigo_interno as cd_codigo_interno
        ,qt_linhas_pedido  as qt_linhas_pedido
        ,max(vl_desconto)  as vl_desconto
    from `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_item_pedido`
group by cd_codigo_interno
        ,qt_linhas_pedido 
)

, cte_base as (
    select a.cd_codigo_interno
          ,a.cd_pedido
          ,b.cd_pedido_loja
          ,a.dt_pedido
          ,a.ds_status_pedido
          ,date(dbt_updated_at) dt_status
          ,row_number() over(partition by a.cd_codigo_interno order by dbt_updated_at ) seq
          ,b.vl_total_item
          ,b.vl_total_pedido
          ,c.vl_desconto
          ,c.qt_linhas_pedido
      from cte_snap          as a
 left join cte_pedido        as b
        on a.cd_codigo_interno = b.cd_codigo_interno
 left join cte_item_pedido   as c
        on a.cd_codigo_interno = c.cd_codigo_interno
)


    select cd_codigo_interno                                           as cd_codigo_interno
          ,cd_pedido                                                   as cd_pedido
          ,cd_pedido_loja                                              as cd_pedido_loja
          ,dt_pedido                                                   as dt_pedido
          ,ds_status_pedido                                            as ds_status_pedido
          ,case when ds_status_pedido = 'ATENDIDO'
                    then dt_status else null end                       as dt_envio
          ,date_trunc(cast(case when ds_status_pedido = 'ATENDIDO'
                    then dt_status else null end as date), month)      as dt_prim_dia_mes_envio
          ,vl_total_item                                               as vl_total_item
          ,vl_total_pedido                                             as vl_total_pedido
          ,vl_desconto                                                 as vl_desconto
          ,round(((vl_total_pedido + vl_desconto) - vl_total_item) ,2) as vl_frete
          ,if(vl_total_item >= 400, true, false)                       as fg_frete_gratis
    from cte_base
   where seq = 1
    --  and ds_status_pedido = 'ATENDIDO'
     and dt_pedido >= '2026-02-26'
order by cd_pedido_loja desc