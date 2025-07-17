

with cte_compra as (
    select cd_codigo_interno
          ,cd_compra
          ,dt_compra
          ,dt_prevista_compra
          ,cd_fornecedor
          ,ds_status_compra
          ,cd_categoria_financeira
          ,ds_observacoes
          ,cd_produto_bling
          ,cd_produto
          ,cd_codigo_barras
          ,nm_produto
          ,nm_produto_completo
          ,ds_variacao
          ,ds_unidade
          ,qt_item
          ,vl_item
          ,vl_total_item
          ,ds_tipo_produto
          ,ds_subcategoria
          ,ds_categoria
          ,ds_classificacao_produto
          ,ds_origem_produto
          ,fg_produto_composicao
          ,lk_produto_compra
     from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_compra`
)

, cte_compra_produto as (
    select distinct 
           cd_produto_bling
          ,cd_produto
          ,cd_codigo_barras
          ,nm_produto
          ,nm_produto_completo
          ,ds_variacao
          ,qt_item
          ,vl_item
          ,ds_subcategoria
          ,ds_categoria
          ,ds_classificacao_produto
          ,ds_origem_produto
          ,cd_compra
          ,dt_compra
          ,ds_status_compra
          ,fg_produto_composicao
          ,lk_produto_compra
          ,row_number() over (partition by cd_produto_bling order by dt_compra desc) as nr_seq
      from cte_compra
)

  select *
    from cte_compra_produto