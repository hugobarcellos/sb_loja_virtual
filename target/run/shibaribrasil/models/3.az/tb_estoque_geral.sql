
  
    

    create or replace table `igneous-sandbox-381622`.`dbt_dw_az`.`tb_estoque_geral`
      
    
    

    OPTIONS(
      description=""""""
    )
    as (
      

--visão atual dos produtos, não trabalho aqui com histórico do preço
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
          ,datetime(current_timestamp(), "America/Sao_Paulo") as dt_ultima_atualizacao
      from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_produto`  as a   
)

  select *
    from cte_produto
order by nm_produto
        ,ds_variacao
    );
  