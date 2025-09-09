
  
    

    create or replace table `igneous-sandbox-381622`.`dbt_dw_az`.`tb_composicao_produto`
      
    
    

    OPTIONS(
      description=""""""
    )
    as (
      

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
          ,a.vl_custo_compra
          ,a.vl_custo_total
          ,a.vl_preco_venda
          ,a.ds_subcategoria
          ,a.ds_categoria
     from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_produto` as a
)

, cte_produto_composicao as (
    select *
      from cte_produto
     where fg_produto_composicao is true
)

, cte_item_composicao_base as (
   select a.cd_produto_bling
         ,a.cd_produto
         ,a.cd_produto_bling_componente
         ,a.qt_componente
    from `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_composicao_produto` as a   
)

, cte_item_composicao as (
    select a.cd_produto_bling
          ,a.cd_produto
          ,a.cd_codigo_barras
          ,a.nm_produto
          ,a.nm_produto_completo
          ,a.ds_variacao
          ,a.ds_tipo_produto
          ,a.cd_produto_bling_pai
          ,b.cd_produto_bling_componente
          ,c.cd_produto                   as cd_produto_componente
          ,c.cd_codigo_barras             as cd_codigo_barras_componente
          ,c.nm_produto                   as nm_produto_componente
          ,c.nm_produto_completo          as nm_produto_completo_componente
          ,c.ds_variacao                  as ds_variacao_componente
          ,b.qt_componente
      from cte_produto_composicao   as a
 left join cte_item_composicao_base as b
        on a.cd_produto_bling = b.cd_produto_bling
 left join cte_produto              as c
        on b.cd_produto_bling_componente = c.cd_produto_bling
)

select *
    from cte_item_composicao
  order by nm_produto_completo
    );
  