{{ config(
    tags = ['az', 'produto'],
    enabled = true
)}}

with cte_produto as (
    select distinct
           a.cd_produto_bling
          ,a.cd_produto
          ,a.cd_codigo_barras
          ,a.nm_produto
          ,a.nm_produto_completo
          ,a.ds_variacao
          ,a.ds_tipo_produto
          ,a.cd_produto_bling_pai
          ,a.fg_produto_composicao
          ,a.ds_tipo_estoque
          ,a.qt_estoque_minimo
          ,a.qt_estoque_atual
          ,a.vl_custo_total
          ,a.vl_preco_venda
          ,a.ds_subcategoria
          ,a.ds_categoria
          ,a.ds_classificacao_produto
          ,a.ds_origem_produto
          ,a.dt_ultima_ingestao
      from {{ ref('tb_produto') }}  as a   
)

, cte_venda_produto as (
   select a.cd_produto_bling                 as cd_produto_bling
         ,a.cd_produto                       as cd_produto
         ,a.cd_codigo_barras                 as cd_codigo_barras
         ,a.nm_produto                       as nm_produto
         ,a.nm_produto_completo              as nm_produto_completo
         ,a.ds_variacao                      as ds_variacao
         ,a.ds_subcategoria                  as ds_subcategoria
         ,a.ds_categoria                     as ds_categoria
         ,a.ds_classificacao_produto         as ds_classificacao_produto
         ,a.ds_origem_produto                as ds_origem_produto
         ,a.qt_pecas_mes_atual               as qt_pecas_mes_atual 
         ,a.qt_pecas_mes_anterior            as qt_pecas_mes_anterior 
         ,a.qt_pecas_tres_meses              as qt_pecas_tres_meses 
         ,a.qt_pecas_seis_meses              as qt_pecas_seis_meses 
         ,a.qt_pecas_sessenta_dias           as qt_pecas_sessenta_dias
         ,a.ds_classificacao_abc             as ds_classificacao_abc
         ,a.vl_percentual_participacao       as vl_percentual_participacao
     from {{ ref('tb_venda_produto_base') }} as a
)

, cte_tempo as (
    select dt_data
          ,dt_prim_dia_mes
      from {{ ref('stg_tempo') }}
     where dt_data >= date_trunc(date_sub(current_date(), interval 6 month), month)
       and dt_data <= current_date
)

, cte_tempo_mes_atual as (
    select count(distinct dt_data) qt_dias_mes_atual
      from cte_tempo
     where dt_data >= date_trunc(date_sub(current_date(), interval 0 month), month)
       and dt_data <= current_date
)

, cte_tempo_mes_anterior as (
    select count(distinct dt_data) qt_dias_mes_anterior
      from cte_tempo
     where dt_data >= date_trunc(date_sub(current_date(), interval 1 month), month)
       and dt_data <  date_trunc(date_sub(current_date(), interval 0 month), month)
)

, cte_tempo_tres_meses as (
    select count(distinct dt_data) qt_dias_tres_meses
      from cte_tempo
     where dt_data >= date_trunc(date_sub(current_date(), interval 3 month), month)
       and dt_data <  date_trunc(date_sub(current_date(), interval 0 month), month)
)

, cte_base as (
   select a.cd_produto_bling                 as cd_produto_bling
         ,a.cd_produto                       as cd_produto
         ,a.cd_codigo_barras                 as cd_codigo_barras
         ,a.nm_produto                       as nm_produto
         ,a.nm_produto_completo              as nm_produto_completo
         ,a.ds_variacao                      as ds_variacao
         ,a.ds_subcategoria                  as ds_subcategoria
         ,a.ds_categoria                     as ds_categoria
         ,a.ds_classificacao_produto         as ds_classificacao_produto
         ,a.ds_origem_produto                as ds_origem_produto
         ,a.ds_classificacao_abc             as ds_classificacao_abc
         ,a.vl_percentual_participacao       as vl_percentual_participacao
         ,b.qt_estoque_minimo                as qt_estoque_minimo
         ,b.qt_estoque_atual                 as qt_estoque_atual
         ,b.vl_custo_total                   as vl_custo_total
         ,b.vl_preco_venda                   as vl_preco_venda
         ,a.qt_pecas_mes_atual               as qt_pecas_mes_atual 
         ,a.qt_pecas_mes_anterior            as qt_pecas_mes_anterior 
         ,a.qt_pecas_tres_meses              as qt_pecas_tres_meses 
         ,a.qt_pecas_sessenta_dias           as qt_pecas_sessenta_dias
         ,c.qt_dias_mes_atual                as qt_dias_mes_atual
         ,d.qt_dias_mes_anterior             as qt_dias_mes_anterior
         ,e.qt_dias_tres_meses               as qt_dias_tres_meses
     from cte_venda_produto  as a
        ,cte_tempo_mes_atual      as c
        ,cte_tempo_mes_anterior   as d
        ,cte_tempo_tres_meses     as e
left join cte_produto        as b 
       on a.cd_produto_bling = b.cd_produto_bling
)

, cte_compra_produto as (
    select cd_compra
          ,cd_produto_bling
          ,qt_item
     from {{ ref('tb_agg_compra_produto') }}
    where ds_status_compra in ('EM ABERTO')
)

, cte_joins as (
   select a.cd_produto_bling                 as cd_produto_bling
         ,a.cd_produto                       as cd_produto
         ,a.cd_codigo_barras                 as cd_codigo_barras
         ,a.nm_produto                       as nm_produto
         ,a.nm_produto_completo              as nm_produto_completo
         ,a.ds_variacao                      as ds_variacao
         ,a.ds_subcategoria                  as ds_subcategoria
         ,a.ds_categoria                     as ds_categoria
         ,a.ds_classificacao_produto         as ds_classificacao_produto
         ,a.ds_origem_produto                as ds_origem_produto
         ,a.ds_classificacao_abc             as ds_classificacao_abc
         ,a.vl_percentual_participacao       as vl_percentual_participacao
         ,a.qt_estoque_minimo                as qt_estoque_minimo
         ,a.qt_estoque_atual                 as qt_estoque_atual
         ,a.vl_custo_total                   as vl_custo_total
         ,a.vl_preco_venda                   as vl_preco_venda
         ,sum(b.qt_item)                     as qt_item_compra_pendente
         ,a.qt_pecas_mes_atual               as qt_pecas_mes_atual 
         ,a.qt_pecas_mes_anterior            as qt_pecas_mes_anterior 
         ,a.qt_pecas_tres_meses              as qt_pecas_tres_meses 
         ,a.qt_pecas_sessenta_dias           as qt_pecas_sessenta_dias
         ,a.qt_dias_mes_atual                as qt_dias_mes_atual
         ,a.qt_dias_mes_anterior             as qt_dias_mes_anterior
         ,a.qt_dias_tres_meses               as qt_dias_tres_meses
     from cte_base               as a
left join cte_compra_produto     as b
       on a.cd_produto_bling = b.cd_produto_bling
 group by a.cd_produto_bling
         ,a.cd_produto
         ,a.cd_codigo_barras
         ,a.nm_produto
         ,a.nm_produto_completo
         ,a.ds_variacao
         ,a.ds_subcategoria
         ,a.ds_categoria
         ,a.ds_classificacao_produto
         ,a.ds_origem_produto
         ,a.ds_classificacao_abc
         ,a.vl_percentual_participacao
         ,a.qt_estoque_minimo
         ,a.qt_estoque_atual
         ,a.vl_custo_total
         ,a.vl_preco_venda
         ,a.qt_pecas_mes_atual 
         ,a.qt_pecas_mes_anterior 
         ,a.qt_pecas_tres_meses 
         ,a.qt_pecas_sessenta_dias
         ,a.qt_dias_mes_atual
         ,a.qt_dias_mes_anterior
         ,a.qt_dias_tres_meses
)
  select *
    from cte_joins
order by nm_produto
        ,ds_variacao