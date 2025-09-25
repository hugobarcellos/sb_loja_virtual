
  
    

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
          ,a.cd_produto_bling_pai
          ,a.fg_produto_composicao
          ,a.ds_tipo_estoque
          ,a.vl_custo_cadastro
          ,a.vl_preco_venda
          ,a.ds_subcategoria
          ,a.ds_categoria
     from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_produto` as a
    where ds_tipo_produto not in ('PAI')
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

, cte_compra_produto as (
    select cd_produto_bling
          ,cd_produto
          ,vl_item
          ,dt_compra
      from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_agg_compra_produto`
     where nr_seq = 1
)

, cte_produto_fabricado as (
    select cd_produto_bling
          ,cd_produto
          ,nm_produto
          ,nm_produto_completo
          ,ds_variacao
          ,sum(vl_custo_compra_total) vl_custo_compra_total
      from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_produto_fabricado`
  group by cd_produto_bling
          ,cd_produto
          ,nm_produto
          ,nm_produto_completo
          ,ds_variacao
)

, cte_item_composicao as (
    select a.cd_produto_bling
          ,a.cd_produto
          ,a.cd_codigo_barras
          ,a.nm_produto
          ,a.nm_produto_completo
          ,a.ds_variacao
          ,a.cd_produto_bling_pai
          ,b.cd_produto_bling_componente
          ,c.cd_produto                                                                                            as cd_produto_componente
          ,c.cd_codigo_barras                                                                                      as cd_codigo_barras_componente
          ,c.nm_produto                                                                                            as nm_produto_componente
          ,c.nm_produto_completo                                                                                   as nm_produto_completo_componente
          ,c.ds_variacao                                                                                           as ds_variacao_componente
          ,b.qt_componente                                                                                         as qt_componente
          ,coalesce(nullif(e.vl_custo_compra_total,0), nullif(d.vl_item,0), c.vl_custo_cadastro)                   as vl_custo_componente
          ,b.qt_componente * coalesce(nullif(e.vl_custo_compra_total,0), nullif(d.vl_item,0), c.vl_custo_cadastro) as vl_custo_componente_total
      from cte_produto_composicao   as a
 left join cte_item_composicao_base as b
        on a.cd_produto_bling = b.cd_produto_bling
 left join cte_produto              as c
        on b.cd_produto_bling_componente = c.cd_produto_bling
 left join cte_compra_produto       as d 
        on b.cd_produto_bling_componente = d.cd_produto_bling
 left join cte_produto_fabricado    as e 
        on b.cd_produto_bling_componente = e.cd_produto_bling
)

    select *
      from cte_item_composicao
  order by nm_produto_completo
    );
  