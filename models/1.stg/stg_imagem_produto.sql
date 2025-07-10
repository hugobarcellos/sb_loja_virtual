{{ config(
    tags = ['stg', 'produto'],
    enabled = true
)}}

with cte_imagem as (
    select trim(a.id)                                         as cd_produto_bling
          ,nullif(trim(replace(a.codigo, '.', '')), '')       as cd_produto
          ,a.midia__imagens__internas                         as js_imagens
      from {{ source('erathos', 'produtos_detalhes') }}  as a
)

, cte_imagem_expandido as (
  select cd_produto_bling
        ,cd_produto
        ,json_element
    from cte_imagem,
  unnest(json_extract_array(js_imagens)) as json_element
)

, cte_imagem_expandido_base as (
    select a.cd_produto_bling
          ,a.cd_produto
          ,cast(json_value(a.json_element, '$.link') as string)  as lk_imagem_produto
      from cte_imagem_expandido as a
)

  select cd_produto_bling
        ,cd_produto
        ,lk_imagem_produto
        ,row_number() over(partition by cd_produto_bling) as seq_imagem
   from cte_imagem_expandido_base
