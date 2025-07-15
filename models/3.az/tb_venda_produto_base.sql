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
     from `dbt_dw_az.tb_produto`
    where ds_categoria not in ('Suprimentos')
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
      from `dbt_dw_az.tb_composicao_produto`
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
          ,qt_pedidos_mes_atual
          ,qt_pecas_mes_atual
          ,vl_total_mes_atual
          ,qt_pedidos_mes_anterior
          ,qt_pecas_mes_anterior
          ,vl_total_mes_anterior
          ,qt_pedidos_tres_meses
          ,qt_pecas_tres_meses
          ,vl_total_tres_meses
          ,qt_pedidos_seis_meses
          ,qt_pecas_seis_meses
          ,vl_total_seis_meses
      from `dbt_dw_az.tb_agg_venda_produto_final`
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
         ,coalesce(a.cd_produto_bling_componente, a.cd_produto_bling)       as cd_produto_bling_componente
         ,coalesce(a.cd_produto_componente, a.cd_produto)                   as cd_produto_componente
         ,coalesce(a.cd_codigo_barras_componente, a.cd_codigo_barras)       as cd_codigo_barras_componente
         ,coalesce(a.nm_produto_componente, a.nm_produto)                   as nm_produto_componente
         ,coalesce(a.nm_produto_completo_componente, a.nm_produto_completo) as nm_produto_completo_componente
         ,coalesce(a.ds_variacao_componente, a.ds_variacao)                 as ds_variacao_componente
         ,coalesce(a.qt_componente, 1)                                      as qt_componente
         ,a.ds_tipo_estoque
         ,a.vl_custo_total
         ,a.vl_preco_venda
         ,a.ds_subcategoria
         ,a.ds_categoria
         ,a.ds_classificacao_produto
         ,a.ds_origem_produto
         ,b.qt_pedidos_mes_atual
         ,b.qt_pecas_mes_atual
         ,b.vl_total_mes_atual
         ,b.qt_pedidos_mes_anterior
         ,b.qt_pecas_mes_anterior
         ,b.vl_total_mes_anterior
         ,b.qt_pedidos_tres_meses
         ,b.qt_pecas_tres_meses
         ,b.vl_total_tres_meses
         ,b.qt_pedidos_seis_meses
         ,b.qt_pecas_seis_meses
         ,b.vl_total_seis_meses
     from cte_produto_composicao    as a 
left join cte_venda_produto         as b
       on a.cd_produto_bling = b.cd_produto_bling
)

  select *
    from cte_base
order by nm_produto