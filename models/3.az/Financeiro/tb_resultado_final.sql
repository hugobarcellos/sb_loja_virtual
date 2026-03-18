{{ config(
    tags = ['az', 'financeiro', 'pedido', 'snaps'],
    enabled = true
)}}

with cte_pedido as (
  select dt_prim_dia_mes                          as dt_prim_dia_mes
        ,sum(vl_total_pedido)                     as vl_faturamento_bruto
        ,sum(vl_custo_pedido)                     as vl_custo_pedido
    from {{ ref('tb_pedido') }}
   where dt_pedido >= '2026-02-26'
     and ds_status_pedido not in ('CANCELADO')
group by dt_prim_dia_mes
)

, cte_recebido as (
  select date_trunc(dt_recebimento_pedido, month) as dt_recebimento_pedido
        ,sum(vl_total_pedido)                     as vl_faturamento_bruto
        ,sum(vl_taxa_pedido_rateio)               as vl_taxa_pedido
        ,sum(vl_total_pedido)
          - sum(vl_taxa_pedido_rateio)            as vl_recebido
    from {{ ref('tb_pedido') }}
   where date_trunc(dt_recebimento_pedido, month) >= '2026-03-01'
     and ds_status_pedido not in ('CANCELADO')
group by date_trunc(dt_recebimento_pedido, month)
)

, cte_frete as (
  select date_trunc(dt_envio, month)              as dt_envio
        ,count(distinct cd_codigo_interno)        as qt_envio
        ,sum(vl_frete_rateio)                     as vl_frete
    from {{ ref('tb_pedido') }}
   where date_trunc(dt_envio, month) >= '2026-03-01'
     and ds_status_pedido not in ('CANCELADO')
group by date_trunc(dt_envio, month)
)

, cte_tempo as (
  select distinct
         dt_prim_dia_mes
    from {{ ref('stg_tempo') }}
   where dt_data >= '2026-03-01'
)
, cte_contas as (
  select date_trunc(cast(dt_vencimento as date), month) as dt_prim_dia_mes
        ,cast(sum(vl_valor) as float64)                 as vl_contas
    from {{ ref('tb_contas_pagar') }}
   where date_trunc(cast(dt_vencimento as date), month) >= '2026-03-01'
     and ds_categoria not in ('f. Despesas da Venda')
group by 1
)

, cte_join as (
   select a.dt_prim_dia_mes                                     as dt_ref
         ,coalesce(round(sum(p.vl_faturamento_bruto), 2), 0)    as vl_faturamento_bruto
         ,coalesce(round(sum(p.vl_custo_pedido), 2), 0)         as vl_custo_pedido
         ,coalesce(round(sum(r.vl_taxa_pedido), 2), 0)          as vl_taxa_pedido
         ,coalesce(round(sum(r.vl_recebido), 2), 0)             as vl_recebido
         ,coalesce(round(sum(f.vl_frete), 2), 0)                as vl_frete_faturado
         ,coalesce(sum(f.qt_envio), 0)                          as qt_envio
         ,coalesce(round(sum(c.vl_contas), 2), 0)               as vl_contas_total
     from cte_tempo       as a
left join cte_pedido      as p
       on a.dt_prim_dia_mes = p.dt_prim_dia_mes
left join cte_recebido    as r
       on a.dt_prim_dia_mes = r.dt_recebimento_pedido
left join cte_frete       as f
       on a.dt_prim_dia_mes = f.dt_envio
left join cte_contas      as c    
       on a.dt_prim_dia_mes = c.dt_prim_dia_mes
 group by a.dt_prim_dia_mes
)

, cte_resultado as (
  select dt_ref                                                          as dt_ref
        ,vl_faturamento_bruto                                            as vl_faturamento_bruto
        ,vl_taxa_pedido                                                  as vl_taxa_pedido
        ,vl_recebido                                                     as vl_recebido
        ,vl_frete_faturado                                               as vl_frete_faturado
        ,round((vl_recebido - vl_frete_faturado), 2)                     as vl_faturamento_liquido
        ,vl_contas_total                                                 as vl_contas_total
        ,round(((vl_recebido - vl_frete_faturado) - vl_contas_total), 2) as vl_lucro_luquido
        ,vl_custo_pedido                                                 as vl_custo_pedido
        ,qt_envio                                                        as qt_envio
    from cte_join
)

select *
  from cte_resultado