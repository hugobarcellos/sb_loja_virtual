
  
    

    create or replace table `igneous-sandbox-381622`.`dbt_dw_az`.`tb_preco_analitico`
      
    
    

    OPTIONS(
      description=""""""
    )
    as (
      

with cte_preco as (
    select distinct
           a.cd_produto_bling
          ,a.cd_produto
          ,a.cd_codigo_barras
          ,a.nm_produto
          ,a.ds_variacao
          ,a.vl_custo_cadastro
          ,a.vl_custo_ultima_compra
          ,a.vl_preco_venda
          ,a.vl_preco_venda_por
          ,a.vl_custo_final_anterior
          ,a.vl_preco_final_anterior
          ,a.ds_subcategoria
          ,a.ds_categoria
          ,a.ds_classificacao_produto
          ,a.ds_origem_produto
          ,a.ds_tipo_produto
          ,a.fg_produto_composicao
          ,a.fg_produto_base_composicao
          ,a.fg_produto_fabricado
          ,a.fg_alteracao
          ,a.pr_desconto_padrao 
          ,a.pr_meta_margem 
          ,a.tx_aliquota_cartao
          ,a.tx_fixa_cartao
          ,a.tx_aliquota_pix
          ,a.vl_desconto_fixo_pix
          ,a.vl_materiais_envio
          ,a.dt_ini_vigencia
          ,a.dt_ultima_compra
          ,a.dt_ultima_ingestao
          ,a.dt_ultima_atualizacao
      from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_preco_produto`    as a
)

, cte_estoque as (
    select a.cd_produto_bling
          ,a.qt_estoque_atual
          ,a.ds_classificacao_risco
      from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_estoque_analitico` as a  
)

   select distinct
           a.cd_produto_bling
          ,a.cd_produto
          ,a.cd_codigo_barras
          ,a.nm_produto
          ,a.ds_variacao
          ,a.vl_custo_cadastro
          ,a.vl_custo_ultima_compra
          ,a.vl_preco_venda
          ,a.vl_preco_venda_por
          ,a.vl_custo_final_anterior
          ,a.vl_preco_final_anterior
          ,b.qt_estoque_atual
          ,b.ds_classificacao_risco
          ,a.ds_subcategoria
          ,a.ds_categoria
          ,a.ds_classificacao_produto
          ,a.ds_origem_produto
          ,a.ds_tipo_produto
          ,a.fg_produto_composicao
          ,a.fg_produto_base_composicao
          ,a.fg_produto_fabricado
          ,a.fg_alteracao
          ,a.pr_desconto_padrao 
          ,a.pr_meta_margem 
          ,a.tx_aliquota_cartao
          ,a.tx_fixa_cartao
          ,a.tx_aliquota_pix
          ,a.vl_desconto_fixo_pix
          ,a.vl_materiais_envio
          ,a.dt_ini_vigencia
          ,a.dt_ultima_compra
          ,a.dt_ultima_ingestao
          ,a.dt_ultima_atualizacao
      from cte_preco   as a
 left join cte_estoque as b
        on a.cd_produto_bling = b.cd_produto_bling
    );
  