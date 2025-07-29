{{ config(
    tags = ['az', 'pedido'],
    enabled = true
)}}

with cte_produto as (
   select distinct
          cd_produto_bling
         ,cd_produto
         ,cd_codigo_barras
         ,nm_produto
         ,nm_produto_completo
         ,ds_variacao
         ,ds_tipo_produto
         ,cd_produto_bling_pai
         ,fg_produto_composicao
         ,ds_tipo_estoque
         ,vl_custo_total
         ,vl_preco_venda
         ,ds_subcategoria
         ,ds_categoria
         ,ds_classificacao_produto
         ,ds_origem_produto
     from {{ ref('tb_produto') }}
    where ds_categoria not in ('Suprimentos', 'Produtos Digitais')
)

, cte_composicao as (
    select cd_produto_bling
          ,cd_produto
          ,cd_codigo_barras
          ,nm_produto
          ,nm_produto_completo
          ,ds_variacao
          ,ds_tipo_produto
          ,cd_produto_bling_pai
          ,cd_produto_bling_componente
          ,cd_produto_componente
          ,cd_codigo_barras_componente
          ,nm_produto_componente
          ,nm_produto_completo_componente
          ,ds_variacao_componente
          ,qt_componente
      from {{ ref('tb_composicao_produto') }}
)

, cte_produto_composicao as (
   select a.cd_produto_bling
         ,a.cd_produto
         ,a.cd_codigo_barras
         ,a.nm_produto
         ,a.nm_produto_completo
         ,a.ds_variacao
         ,a.ds_tipo_produto
         ,a.cd_produto_bling_pai
         ,a.fg_produto_composicao
         ,b.cd_produto_bling_componente
         ,b.cd_produto_componente
         ,b.cd_codigo_barras_componente
         ,b.nm_produto_componente
         ,b.nm_produto_completo_componente
         ,b.ds_variacao_componente
         ,b.qt_componente
         ,a.ds_tipo_estoque
         ,a.vl_custo_total
         ,a.vl_preco_venda
         ,a.ds_subcategoria
         ,a.ds_categoria
         ,a.ds_classificacao_produto
         ,a.ds_origem_produto
     from cte_produto    as a 
left join cte_composicao as b
       on a.cd_produto_bling = b.cd_produto_bling
)

, cte_venda_produto as (
    select cd_produto_bling
          ,cd_produto
          ,cd_codigo_barras
          ,nm_produto
          ,ds_variacao
          ,coalesce(qt_pedidos_mes_atual, 0)     as qt_pedidos_mes_atual
          ,coalesce(qt_pecas_mes_atual, 0)       as qt_pecas_mes_atual
          ,coalesce(vl_total_mes_atual, 0)       as vl_total_mes_atual
          ,coalesce(qt_pedidos_mes_anterior, 0)  as qt_pedidos_mes_anterior
          ,coalesce(qt_pecas_mes_anterior, 0)    as qt_pecas_mes_anterior
          ,coalesce(vl_total_mes_anterior, 0)    as vl_total_mes_anterior
          ,coalesce(qt_pedidos_tres_meses, 0)    as qt_pedidos_tres_meses
          ,coalesce(qt_pecas_tres_meses, 0)      as qt_pecas_tres_meses
          ,coalesce(vl_total_tres_meses, 0)      as vl_total_tres_meses
          ,coalesce(qt_pedidos_seis_meses, 0)    as qt_pedidos_seis_meses
          ,coalesce(qt_pecas_seis_meses, 0)      as qt_pecas_seis_meses
          ,coalesce(vl_total_seis_meses, 0)      as vl_total_seis_meses
          ,coalesce(qt_pedidos_sessenta_dias, 0) as qt_pedidos_sessenta_dias
          ,coalesce(qt_pecas_sessenta_dias, 0)   as qt_pecas_sessenta_dias
          ,coalesce(vl_total_sessenta_dias, 0)   as vl_total_sessenta_dias
      from {{ ref('tb_agg_venda_produto_final') }}
)

, cte_base as (
   select a.cd_produto_bling
         ,a.cd_produto
         ,a.cd_codigo_barras
         ,a.nm_produto
         ,a.nm_produto_completo
         ,a.ds_variacao
         ,a.ds_tipo_produto
         ,a.cd_produto_bling_pai
         ,a.fg_produto_composicao
         ,coalesce(a.cd_produto_bling_componente, a.cd_produto_bling)              as cd_produto_bling_componente
         ,coalesce(a.cd_produto_componente, a.cd_produto)                          as cd_produto_componente
         ,coalesce(a.cd_codigo_barras_componente, a.cd_codigo_barras)              as cd_codigo_barras_componente
         ,coalesce(a.nm_produto_componente, a.nm_produto)                          as nm_produto_componente
         ,coalesce(a.nm_produto_completo_componente, a.nm_produto_completo)        as nm_produto_completo_componente
         ,coalesce(a.ds_variacao_componente, a.ds_variacao)                        as ds_variacao_componente
         ,coalesce(a.qt_componente, 1)                                             as qt_componente
         ,coalesce((b.qt_pecas_mes_atual * coalesce(a.qt_componente, 1)), 0)       as qt_pecas_mes_atual 
         ,coalesce((b.qt_pecas_mes_anterior * coalesce(a.qt_componente, 1)), 0)    as qt_pecas_mes_anterior 
         ,coalesce((b.qt_pecas_tres_meses * coalesce(a.qt_componente, 1)), 0)      as qt_pecas_tres_meses 
         ,coalesce((b.qt_pecas_seis_meses * coalesce(a.qt_componente, 1)), 0)      as qt_pecas_seis_meses 
         ,coalesce((b.qt_pecas_sessenta_dias * coalesce(a.qt_componente, 1)), 0)   as qt_pecas_sessenta_dias 
     from cte_produto_composicao    as a 
left join cte_venda_produto         as b
       on a.cd_produto_bling = b.cd_produto_bling
)

, cte_produto_base as (
   select cd_produto_bling_componente      as cd_produto_bling
         ,cd_produto_componente            as cd_produto
         ,cd_codigo_barras_componente      as cd_codigo_barras
         ,nm_produto_componente            as nm_produto
         ,nm_produto_completo_componente   as nm_produto_completo
         ,ds_variacao_componente           as ds_variacao
         ,sum(qt_pecas_mes_atual)          as qt_pecas_mes_atual 
         ,sum(qt_pecas_mes_anterior)       as qt_pecas_mes_anterior 
         ,sum(qt_pecas_tres_meses)         as qt_pecas_tres_meses 
         ,sum(qt_pecas_seis_meses)         as qt_pecas_seis_meses 
         ,sum(qt_pecas_sessenta_dias)      as qt_pecas_sessenta_dias 
     from cte_base
 group by cd_produto_bling_componente
         ,cd_produto_componente
         ,cd_codigo_barras_componente
         ,nm_produto_componente
         ,nm_produto_completo_componente
         ,ds_variacao_componente
)

, cte_base_final as (
   select a.cd_produto_bling                 as cd_produto_bling
         ,a.cd_produto                       as cd_produto
         ,a.cd_codigo_barras                 as cd_codigo_barras
         ,a.nm_produto                       as nm_produto
         ,a.nm_produto_completo              as nm_produto_completo
         ,a.ds_variacao                      as ds_variacao
         ,b.ds_subcategoria                  as ds_subcategoria
         ,b.ds_categoria                     as ds_categoria
         ,b.ds_classificacao_produto         as ds_classificacao_produto
         ,b.ds_origem_produto                as ds_origem_produto
         ,a.qt_pecas_mes_atual               as qt_pecas_mes_atual 
         ,a.qt_pecas_mes_anterior            as qt_pecas_mes_anterior 
         ,a.qt_pecas_tres_meses              as qt_pecas_tres_meses 
         ,a.qt_pecas_seis_meses              as qt_pecas_seis_meses 
         ,a.qt_pecas_sessenta_dias           as qt_pecas_sessenta_dias 
     from cte_produto_base as a
left join cte_produto_composicao as b
       on a.cd_produto_bling = b.cd_produto_bling
    where b.fg_produto_composicao is false 
      and b.ds_tipo_produto not in ('PAI')
)

, cte_ordenada AS (
  select *
         ,sum(qt_pecas_sessenta_dias) over () as qt_total_geral
         ,sum(qt_pecas_sessenta_dias) over (order by qt_pecas_sessenta_dias desc rows between unbounded preceding and current row) as qt_acumulada
    from cte_base_final
)

, cte_classificada AS (
  select *
         ,round(qt_acumulada / qt_total_geral, 4) as vl_percentual_acumulado
         ,round(qt_pecas_sessenta_dias / qt_total_geral, 4) as vl_percentual_participacao
         ,case
           when qt_acumulada / qt_total_geral <= 0.80 then 'A'
           when qt_acumulada / qt_total_geral <= 0.95 then 'B'
           else 'C'
         end as ds_classificacao_abc
    from cte_ordenada
)

  select *
    from cte_classificada
order by nm_produto