

with cte_campo as (
    select trim(a.id)                                         as cd_produto_bling
          ,nullif(trim(replace(a.codigo, '.', '')), '')       as cd_produto
          ,a.campos_customizados                              as js_campos
      from `igneous-sandbox-381622`.`datalake_bling`.`produtos_detalhes`  as a
)

, cte_campo_expandido as (
  select cd_produto_bling
        ,cd_produto
        ,json_element
    from cte_campo,
  unnest(json_extract_array(js_campos)) as json_element
)

, cte_campo_expandido_base as (
    select a.cd_produto_bling
          ,a.cd_produto
          ,cast(json_value(a.json_element, '$.idCampoCustomizado') as string)  as cd_campo_customizado
          ,cast(json_value(a.json_element, '$.idVinculo') as string)           as cd_vinculo
          ,cast(json_value(a.json_element, '$.valor') as string)               as cd_valor
          ,cast(json_value(a.json_element, '$.item') as string)                as ds_valor_campo
      from cte_campo_expandido as a
)

, cte_campo_base as (
  select cd_produto_bling
        ,cd_produto
        -- Pivoteia os campos
        ,max(if(cd_campo_customizado = '3435243', ds_valor_campo, null)) as ds_classificacao_produto
        ,max(if(cd_campo_customizado = '3435246', ds_valor_campo, null)) as fg_ajuste_preco
        ,max(if(cd_campo_customizado = '3435249', cd_valor, null))       as dt_ajuste_preco
    from cte_campo_expandido_base
group by cd_produto_bling, cd_produto
)

  select cd_produto_bling
        ,cd_produto
        ,ds_classificacao_produto
        ,fg_ajuste_preco
        ,replace(dt_ajuste_preco, '/', '-') dt_ajuste_preco
   from cte_campo_base