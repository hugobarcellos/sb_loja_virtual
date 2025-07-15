

  create or replace view `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_item_pedido`
  OPTIONS(
      description=""""""
    )
  as 

with cte_item_base as (
  select nullif(trim(id), '')                                 as cd_codigo_interno
        ,nullif(trim(data), '')                               as dt_pedido
        ,nullif(trim(desconto__unidade), '')                  as ds_tipo_desconto
        ,cast(nullif(trim(desconto__valor), '') as float64)   as vl_desconto
        ,itens
        ,parcelas
   from `igneous-sandbox-381622`.`datalake_bling`.`pedidos_vendas_detalhes`
)

, cte_itens_json as (
  select cd_codigo_interno
        ,dt_pedido
        ,ds_tipo_desconto
        ,vl_desconto
        ,json_item
    from cte_item_base,
  unnest(json_extract_array(itens)) as json_item
)

, cte_parcelas_json as (
  select cd_codigo_interno
        ,dt_pedido
        ,ds_tipo_desconto
        ,vl_desconto
        ,json_parcela
    from cte_item_base,
  unnest(json_extract_array(parcelas)) as json_parcela
)

, cte_item as (
   select i.cd_codigo_interno
         ,i.dt_pedido
         ,i.ds_tipo_desconto
         ,i.vl_desconto
         ,trim(json_value(i.json_item, '$.produto.id'))                 as cd_produto_bling
         ,replace(trim(json_value(i.json_item, '$.codigo')), '.', '')   as cd_produto
         ,json_value(i.json_item, '$.descricao')                        as nm_produto_completo
         ,cast(json_value(i.json_item, '$.quantidade') as int64)        as qt_item
         ,cast(json_value(i.json_item, '$.valor') as float64)           as vl_item
         ,nullif(json_value(p.json_parcela, '$.observacoes'), '')       as ds_forma_pagamento
     from cte_itens_json i
left join cte_parcelas_json p
       on i.cd_codigo_interno = p.cd_codigo_interno
      and i.dt_pedido = p.dt_pedido
)

   select distinct
          a.cd_codigo_interno
         ,a.dt_pedido
         ,a.cd_produto_bling                                                         as cd_produto_bling
         ,a.cd_produto                                                               as cd_produto
         ,a.nm_produto_completo                                                      as nm_produto_completo
         ,a.qt_item                                                                  as qt_item
         ,a.vl_item                                                                  as vl_item
         ,a.ds_tipo_desconto                                                         as ds_tipo_desconto
         ,a.vl_desconto                                                              as vl_desconto
         ,trim(replace(a.ds_forma_pagamento, 'Método de pagamento:', ''))            as ds_forma_pagamento
     from cte_item               as a;

