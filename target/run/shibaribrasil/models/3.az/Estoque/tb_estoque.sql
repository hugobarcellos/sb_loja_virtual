
  
    

    create or replace table `igneous-sandbox-381622`.`dbt_dw_az`.`tb_estoque`
      
    
    

    OPTIONS(
      description=""""""
    )
    as (
      

with cte_estoque as (
   select a.cd_produto_bling
         ,a.cd_produto
         ,a.cd_codigo_barras
         ,a.nm_produto_completo
         ,a.qt_estoque_minimo
         ,a.qt_estoque_atual
         ,a.dt_ultima_ingestao
     from `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_estoque`  as a 
)

, cte_produto as (
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
          ,a.fg_produto_fabricado
          ,a.ds_tipo_estoque
          ,a.ds_subcategoria
          ,a.ds_categoria
          ,a.ds_classificacao_produto
          ,a.ds_origem_produto
          ,a.qt_lead_time
          ,a.qt_cobertura_desejada
          ,a.nm_fornecedor
          ,a.lk_ultima_compra
          ,a.dt_ultima_ingestao
      from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_produto`  as a  
     where a.ds_tipo_produto not in ('PAI') 
       and a.ds_categoria not in ('Livros e Presentes')
)

, cte_preco as (
    select distinct
           a.cd_produto_bling
          ,a.cd_produto
          ,a.vl_custo_cadastro
          ,a.vl_custo_ultima_compra
          ,a.vl_preco_venda
          ,a.vl_preco_venda_por
          ,a.fg_produto_base_composicao
     from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_preco_produto`  as a   
)

, cte_produto_estoque as (
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
          ,c.fg_produto_base_composicao
          ,a.fg_produto_fabricado
          ,a.ds_subcategoria
          ,a.ds_categoria
          ,a.ds_classificacao_produto
          ,a.ds_origem_produto
          ,a.nm_fornecedor
          ,a.lk_ultima_compra
          ,a.ds_tipo_estoque
          ,a.qt_lead_time
          ,a.qt_cobertura_desejada
          ,b.qt_estoque_minimo
          ,b.qt_estoque_atual
          ,c.vl_custo_cadastro
          ,c.vl_custo_ultima_compra
          ,c.vl_preco_venda
          ,c.vl_preco_venda_por
      from cte_produto  as a  
 left join cte_estoque  as b 
        on a.cd_produto_bling = b.cd_produto_bling
 left join cte_preco    as c 
        on a.cd_produto_bling = c.cd_produto_bling
)

  select *
    from cte_produto_estoque
    );
  