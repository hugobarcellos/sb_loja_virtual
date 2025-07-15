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
    where ds_categoria not in ('Suprimentos')
)

, cte_pedido as (
    select distinct
          cd_codigo_interno
         ,cd_pedido
         ,dt_pedido
         ,date_trunc(cast(dt_pedido as date), month) as dt_prim_dia_mes
         ,cd_status_pedido
         ,ds_status_pedido
         ,cd_produto_bling
         ,cd_produto
         ,nm_produto_completo
         ,qt_item
         ,vl_item
         ,vl_total_item
    from {{ ref('tb_pedido') }}
   where ds_status_pedido <> 'CANCELADO'
)

, cte_produto_pedido_base as (
   select a.cd_produto_bling
         ,a.cd_produto
         ,a.cd_codigo_barras
         ,a.nm_produto
         ,a.nm_produto_completo
         ,a.ds_variacao
         ,a.ds_tipo_produto
         ,a.cd_produto_bling_pai
         ,a.fg_produto_composicao
         ,a.ds_tipo_estoque
         ,a.ds_subcategoria
         ,a.ds_categoria
         ,a.ds_classificacao_produto
         ,a.ds_origem_produto
         ,b.dt_pedido
         ,b.dt_prim_dia_mes
         ,count(distinct b.cd_codigo_interno) as qt_pedidos
         ,sum(b.qt_item)                      as qt_item
         ,sum(b.vl_total_item)                as vl_total_item
     from cte_produto as a
left join cte_pedido  as b 
       on a.cd_produto_bling = b.cd_produto_bling
 group by a.cd_produto_bling
         ,a.cd_produto
         ,a.cd_codigo_barras
         ,a.nm_produto
         ,a.nm_produto_completo
         ,a.ds_variacao
         ,a.ds_tipo_produto
         ,a.cd_produto_bling_pai
         ,a.fg_produto_composicao
         ,a.ds_tipo_estoque
         ,a.vl_custo_total
         ,a.vl_preco_venda
         ,a.ds_subcategoria
         ,a.ds_categoria
         ,a.ds_classificacao_produto
         ,a.ds_origem_produto
         ,b.dt_pedido
         ,b.dt_prim_dia_mes
)

select *
  from cte_produto_pedido_base