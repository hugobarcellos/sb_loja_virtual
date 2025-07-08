

with cte_produto as (
   select *
     from `igneous-sandbox-381622`.`dbt_dw_dw`.`dm_produto`
)

, cte_categoria as (
   select cd_categoria
         ,nm_categoria
         ,cd_categoria_pai
         ,ds_tipo_categoria
     from `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_categoria_produto`
)

-- join com a dimensao de categoria de produto
, cte_tratamentos as (
    select distinct
           a.cd_produto_bling
          ,a.cd_produto
          ,a.cd_codigo_barras
          ,a.nm_produto
          ,a.nm_produto_completo
          ,ds_variacao
          ,a.ds_tipo_produto
          ,a.cd_produto_bling_pai
          ,a.fg_produto_composicao
          ,a.ds_tipo_estoque
          ,a.qt_estoque_minimo
          ,a.qt_estoque_atual
          ,a.vl_custo_compra
          ,a.vl_custo_total
          ,a.vl_preco_venda
          ,b.nm_categoria                           as ds_subcategoria
          ,coalesce(c.nm_categoria, b.nm_categoria) as ds_categoria
          ,a.ds_situacao
          ,a.dm_altura
          ,a.dm_largura
          ,a.dm_profundidade
          ,a.dm_peso_bruto
          ,a.dm_peso_liquido
      from cte_produto   as a
 left join cte_categoria    as b
        on a.cd_categoria = b.cd_categoria
 left join cte_categoria    as c
        on b.cd_categoria_pai = c.cd_categoria     
)

select *
    from cte_tratamentos
  order by nm_produto_completo