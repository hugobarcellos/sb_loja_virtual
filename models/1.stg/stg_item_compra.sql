{{ config(
    tags = ['stg', 'compra'],
    enabled = true
)}}

with cte_item_base as (
  select nullif(trim(id), '')                        as cd_codigo_interno
        ,nullif(trim(numero), '')                    as cd_compra
        ,nullif(trim(data), '')                      as dt_compra
        ,nullif(trim(categoria__id), '')             as cd_categoria_financeira
        ,nullif(trim(observacoes), '')               as ds_observacoes
        ,itens
   from {{ source('erathos', 'pedidos_compras_detalhes') }}
)

, cte_itens_json as (
  select cd_codigo_interno
        ,cd_compra
        ,dt_compra
        ,cd_categoria_financeira
        ,ds_observacoes
        ,json_item
    from cte_item_base,
  unnest(json_extract_array(itens)) as json_item
)

, cte_item as (
   select i.cd_codigo_interno
         ,i.cd_compra
         ,i.dt_compra
         ,i.cd_categoria_financeira
         ,i.ds_observacoes
         ,trim(json_value(i.json_item, '$.produto.id'))                 as cd_produto_bling
         ,replace(trim(json_value(i.json_item, '$.codigo')), '.', '')   as cd_produto
         ,json_value(i.json_item, '$.descricao')                        as nm_produto_completo
         ,json_value(i.json_item, '$.unidade')                          as ds_unidade
         ,cast(json_value(i.json_item, '$.quantidade') as int64)        as qt_item
         ,cast(json_value(i.json_item, '$.valor') as float64)           as vl_item
     from cte_itens_json i
)

   select distinct
          a.cd_codigo_interno
         ,a.cd_compra
         ,a.dt_compra
         ,a.cd_categoria_financeira
         ,a.ds_observacoes
         ,a.cd_produto_bling
         ,a.cd_produto
         ,a.nm_produto_completo
         ,a.ds_unidade
         ,a.qt_item
         ,a.vl_item
     from cte_item               as a