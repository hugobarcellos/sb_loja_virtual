

with cte_venda_produto as (
   select cd_produto_bling
         ,cd_produto
         ,cd_codigo_barras
         ,nm_produto
         ,nm_produto_completo
         ,ds_variacao
         ,ds_tipo_produto
         ,cd_produto_bling_pai
         ,fg_produto_composicao
         ,ds_tipo_estoque
         ,ds_subcategoria
         ,ds_categoria
         ,ds_classificacao_produto
         ,ds_origem_produto
         ,dt_pedido
         ,dt_prim_dia_mes
         ,qt_pedidos
         ,qt_item
         ,vl_total_item
     from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_venda_produto_final`
)

, cte_pedido_mes_atual as (
   select cd_produto_bling
         ,cd_produto
         ,nm_produto_completo
         ,sum(qt_pedidos)     as qt_pedidos
         ,sum(qt_item)        as qt_item
         ,sum(vl_total_item)  as vl_total_item
    from cte_venda_produto
   where dt_prim_dia_mes = date_trunc(cast(current_date as date), month)
group by cd_produto_bling
        ,cd_produto
        ,nm_produto_completo
)

, cte_pedido_mes_anterior as (
   select cd_produto_bling
         ,cd_produto
         ,nm_produto_completo
         ,sum(qt_pedidos)     as qt_pedidos
         ,sum(qt_item)        as qt_item
         ,sum(vl_total_item)  as vl_total_item
    from cte_venda_produto
   where dt_prim_dia_mes = date_trunc(date_sub(current_date(), interval 1 month), month)
group by cd_produto_bling
        ,cd_produto
        ,nm_produto_completo
)

, cte_pedido_tres_meses as (
   select cd_produto_bling
         ,cd_produto
         ,nm_produto_completo
         ,sum(qt_pedidos)     as qt_pedidos
         ,sum(qt_item)        as qt_item
         ,sum(vl_total_item)  as vl_total_item
    from cte_venda_produto
   where dt_prim_dia_mes >= date_trunc(date_sub(current_date(), interval 3 month), month)
     and dt_prim_dia_mes <= date_trunc(date_sub(current_date(), interval 1 month), month)
group by cd_produto_bling
        ,cd_produto
        ,nm_produto_completo
)

, cte_pedido_seis_meses as (
   select cd_produto_bling
         ,cd_produto
         ,nm_produto_completo
         ,sum(qt_pedidos)     as qt_pedidos
         ,sum(qt_item)        as qt_item
         ,sum(vl_total_item)  as vl_total_item
    from cte_venda_produto
   where dt_prim_dia_mes >= date_trunc(date_sub(current_date(), interval 6 month), month)
     and dt_prim_dia_mes <= date_trunc(date_sub(current_date(), interval 1 month), month)
group by cd_produto_bling
        ,cd_produto
        ,nm_produto_completo
)

, cte_pedido_60_dias as (
   select cd_produto_bling
         ,cd_produto
         ,nm_produto_completo
         ,sum(qt_pedidos)     as qt_pedidos
         ,sum(qt_item)        as qt_item
         ,sum(vl_total_item)  as vl_total_item
    from cte_venda_produto
   where cast(dt_pedido as date) >= date_trunc(date_sub(current_date(), interval 59 day), day)
     and cast(dt_pedido as date) <= current_date
group by cd_produto_bling
        ,cd_produto
        ,nm_produto_completo
)

, cte_final as (
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
          ,a.ds_subcategoria
          ,a.ds_categoria
          ,a.ds_classificacao_produto
          ,a.ds_origem_produto
          ,coalesce(b.qt_pedidos,0)    as qt_pedidos_mes_atual
          ,coalesce(b.qt_item,0)       as qt_pecas_mes_atual
          ,coalesce(b.vl_total_item,0) as vl_total_mes_atual
          ,coalesce(c.qt_pedidos,0)    as qt_pedidos_mes_anterior
          ,coalesce(c.qt_item,0)       as qt_pecas_mes_anterior
          ,coalesce(c.vl_total_item,0) as vl_total_mes_anterior
          ,coalesce(d.qt_pedidos,0)    as qt_pedidos_tres_meses
          ,coalesce(d.qt_item,0)       as qt_pecas_tres_meses
          ,coalesce(d.vl_total_item,0) as vl_total_tres_meses
          ,coalesce(e.qt_pedidos,0)    as qt_pedidos_seis_meses
          ,coalesce(e.qt_item,0)       as qt_pecas_seis_meses
          ,coalesce(e.vl_total_item,0) as vl_total_seis_meses
          ,coalesce(f.qt_pedidos,0)    as qt_pedidos_sessenta_dias
          ,coalesce(f.qt_item,0)       as qt_pecas_sessenta_dias
          ,coalesce(f.vl_total_item,0) as vl_total_sessenta_dias
     from cte_venda_produto               as a
left join cte_pedido_mes_atual            as b
       on a.cd_produto_bling = b.cd_produto_bling
left join cte_pedido_mes_anterior         as c
       on a.cd_produto_bling = c.cd_produto_bling
left join cte_pedido_tres_meses           as d
       on a.cd_produto_bling = d.cd_produto_bling
left join cte_pedido_seis_meses           as e
       on a.cd_produto_bling = e.cd_produto_bling
left join cte_pedido_60_dias              as f
       on a.cd_produto_bling = f.cd_produto_bling
)

   select *
     from cte_final
 order by nm_produto_completo