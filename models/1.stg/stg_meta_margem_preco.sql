{{ config(
    tags = ['stg'],
    enabled = true
)}}

with cte_meta as (
   select trim(ds_classificacao_produto)      as ds_classificacao_produto
         ,trim(ds_origem_produto)             as ds_origem_produto
         ,pr_desconto_padrao                  as pr_desconto_padrao 
         ,pr_meta_margem                      as pr_meta_margem 
     from {{ source('drive', 'drive_meta_margem_preco') }}
)

select ds_classificacao_produto
      ,ds_origem_produto
      ,pr_desconto_padrao 
      ,pr_meta_margem 
  from cte_meta