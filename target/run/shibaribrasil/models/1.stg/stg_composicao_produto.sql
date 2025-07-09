

  create or replace view `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_composicao_produto`
  OPTIONS(
      description=""""""
    )
  as 

with cte_composicao as (
    select trim(a.id)                                         as cd_produto_bling
          ,nullif(trim(replace(a.codigo, '.', '')), '')       as cd_produto
          ,a.estrutura__componentes                           as js_componentes
      from `igneous-sandbox-381622`.`datalake_bling`.`produtos_detalhes`  as a
)

, cte_componentes_expandido as (
  select cd_produto_bling
        ,cd_produto
        ,json_element
    from cte_composicao,
  unnest(json_extract_array(js_componentes)) as json_element
)

, cte_componentes_expandido_base as (
    select a.cd_produto_bling
          ,a.cd_produto
          ,cast(json_value(a.json_element, '$.produto.id') as string)      as cd_produto_bling_componente
          ,cast(json_value(a.json_element, '$.quantidade') as int64)       as qt_componente
      from cte_componentes_expandido as a
)

select *
  from cte_componentes_expandido_base;

