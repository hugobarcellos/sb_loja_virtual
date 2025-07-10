
  
    

    create or replace table `igneous-sandbox-381622`.`dbt_dw_az`.`tb_preco_produto`
      
    
    

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
          ,a.vl_custo_compra
          ,a.vl_custo_total
          ,a.vl_preco_venda
          ,a.ds_subcategoria
          ,a.ds_categoria
          ,a.ds_classificacao_produto
          ,a.ds_origem_produto
          ,a.fg_alteracao_preco
          ,a.ds_tipo_alteracao_preco
          ,a.dt_alteracao_preco
          ,a.vl_alteracao_preco
      from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_produto`  as a   
)

, cte_meta_margem as (
    select ds_classificacao_produto
          ,ds_origem_produto
          ,pr_desconto_padrao 
          ,pr_meta_margem 
      from `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_meta_margem_preco`
)

, cte_join_meta as (
    select distinct
           a.cd_produto_bling
          ,a.cd_produto
          ,a.cd_codigo_barras
          ,a.nm_produto
          ,a.nm_produto_completo
          ,a.ds_variacao
          ,a.vl_custo_compra
          ,a.vl_custo_total
          ,a.vl_preco_venda
          ,a.ds_subcategoria
          ,a.ds_categoria
          ,a.ds_classificacao_produto
          ,a.ds_origem_produto
          ,a.fg_alteracao_preco
          ,a.ds_tipo_alteracao_preco
          ,a.dt_alteracao_preco
          ,a.vl_alteracao_preco
          ,b.pr_desconto_padrao 
          ,b.pr_meta_margem 
     from cte_produto     as a
left join cte_meta_margem as b
       on a.ds_classificacao_produto = b.ds_classificacao_produto
      and a.ds_origem_produto = b.ds_origem_produto
)

  select *
    from cte_join_meta
order by nm_produto_completo
    );
  