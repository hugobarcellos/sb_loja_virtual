{% snapshot snapshot_preco_custo_produto %}

{{
  config(
    target_schema='snapshots',
    unique_key='cd_produto_bling',
    strategy='check',
    check_cols=['vl_preco_venda', 'vl_custo_cadastro', 'vl_custo_ultima_compra', 'vl_preco_venda_por'], 
    invalidate_hard_deletes=True
  )
}}

    select cd_produto_bling
          ,cd_produto
          ,cd_codigo_barras
          ,nm_produto
          ,ds_variacao
          ,vl_custo_cadastro
          ,vl_custo_ultima_compra
          ,vl_preco_venda
          ,vl_preco_venda_por
          ,ds_subcategoria
          ,ds_categoria
          ,ds_classificacao_produto
          ,ds_origem_produto
          ,fg_produto_base_composicao
          ,fg_produto_composicao
          ,dt_ultima_compra 
     from {{ ref('tb_preco_produto_base') }}

{% endsnapshot %}