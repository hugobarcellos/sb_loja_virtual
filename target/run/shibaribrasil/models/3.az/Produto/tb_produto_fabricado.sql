
  
    

    create or replace table `igneous-sandbox-381622`.`dbt_dw_az`.`tb_produto_fabricado`
      
    
    

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
          ,a.vl_custo_cadastro
          ,a.vl_preco_venda
          ,a.vl_preco_venda_por
          ,a.ds_subcategoria
          ,a.ds_categoria
     from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_produto` as a
)

, cte_compra_produto as (
    select cd_produto_bling
          ,cd_produto
          ,vl_item
          ,dt_compra
      from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_agg_compra_produto`
     where nr_seq = 1
)

, cte_produto_fabricado as (
    select cd_produto
          ,nm_produto_completo
          ,cd_produto_composicao 
          ,nm_produto_completo_composicao 
          ,qt_produto_composicao 
     from `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_produto_fabricado`
)

, cte_base as (
    select distinct 
           b.cd_produto_bling
          ,a.cd_produto
          ,b.nm_produto
          ,b.nm_produto_completo
          ,b.ds_variacao
          ,c.cd_produto_bling                          as cd_produto_bling_composicao
          ,a.cd_produto_composicao                     as cd_produto_composicao
          ,c.nm_produto                                as nm_produto_composicao
          ,c.nm_produto_completo                       as nm_produto_completo_composicao
          ,c.ds_variacao                               as ds_variacao_composicao
          ,a.qt_produto_composicao                     as qt_produto_composicao
          ,d.vl_item                                   as vl_custo_compra
          ,a.qt_produto_composicao * d.vl_item         as vl_custo_compra_total
      from cte_produto_fabricado as a 
 left join cte_produto           as b 
        on a.cd_produto = b.cd_produto
 left join cte_produto           as c 
        on a.cd_produto_composicao = c.cd_produto
 left join cte_compra_produto    as d 
        on c.cd_produto_bling = d.cd_produto_bling
)

select *
    from cte_base
  order by nm_produto_completo
    );
  