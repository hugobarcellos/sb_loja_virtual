

with cte_compra as (
  select cd_codigo_interno
        ,cd_compra
        ,dt_compra
        ,dt_prevista_compra
        ,cd_fornecedor
        ,vl_total_item
        ,vl_total_compra
        ,ds_status_compra
     from `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_compra`
    where ds_status_compra not in ('CANCELADO')
)

, cte_item_compra as (
   select cd_codigo_interno
         ,cd_compra
         ,dt_compra
         ,cd_categoria_financeira
         ,ds_observacoes
         ,cd_produto_bling
         ,cd_produto
         ,nm_produto_completo
         ,ds_unidade
         ,qt_item
         ,vl_item
     from `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_item_compra`
)

, cte_compra_base as (
   select distinct
          a.cd_codigo_interno
         ,a.cd_compra
         ,a.dt_compra
         ,a.dt_prevista_compra
         ,a.cd_fornecedor
         ,a.ds_status_compra
         ,b.cd_categoria_financeira
         ,b.ds_observacoes
         ,b.cd_produto_bling
         ,b.ds_unidade
         ,b.qt_item
         ,b.vl_item
         ,b.qt_item * b.vl_item as vl_total_item
         ,a.vl_total_compra
     from cte_compra        as a
left join cte_item_compra   as b
       on a.cd_codigo_interno = b.cd_codigo_interno
)

, cte_produto as (
    select cd_produto_bling
          ,cd_produto
          ,cd_codigo_barras
          ,nm_produto
          ,nm_produto_completo
          ,ds_variacao
          ,ds_tipo_produto
          ,ds_subcategoria
          ,ds_categoria
          ,ds_classificacao_produto
          ,ds_origem_produto
      from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_produto`
)

, cte_base as (
    select a.cd_codigo_interno
          ,a.cd_compra
          ,a.dt_compra
          ,a.dt_prevista_compra
          ,a.cd_fornecedor
          ,a.ds_status_compra
          ,a.cd_categoria_financeira
          ,a.ds_observacoes
          ,a.cd_produto_bling
          ,b.cd_produto
          ,b.cd_codigo_barras
          ,b.nm_produto
          ,b.nm_produto_completo
          ,b.ds_variacao
          ,a.ds_unidade
          ,a.qt_item
          ,a.vl_item
          ,a.vl_total_item
          ,b.ds_tipo_produto
          ,b.ds_subcategoria
          ,b.ds_categoria
          ,b.ds_classificacao_produto
          ,b.ds_origem_produto
     from cte_compra_base        as a
left join cte_produto            as b
       on a.cd_produto_bling = b.cd_produto_bling
)

, cte_validacao_link as (
  select *
        ,regexp_contains(ds_observacoes, r'\b\d{3,}\s*:\s*https?://') as is_padrao_valido
    from cte_compra_base
)

, cte_link_produto_base as (
  select cd_codigo_interno
        ,split(item, ': ') as partes
    from cte_validacao_link,
  unnest(
        case 
          when is_padrao_valido then split(ds_observacoes, ',') 
          else array<string>[null] 
          end
  ) as item
)

, cte_link_produto as (
    select distinct 
           cd_codigo_interno
          ,replace(trim(partes[offset(0)]), '.', '') as cd_produto
          ,trim(partes[offset(1)])                   as lk_produto_compra
      from cte_link_produto_base
)

, cte_base_link as (
    select a.cd_codigo_interno
          ,a.cd_compra
          ,a.dt_compra
          ,a.dt_prevista_compra
          ,a.cd_fornecedor
          ,a.ds_status_compra
          ,a.cd_categoria_financeira
          ,a.ds_observacoes
          ,a.cd_produto_bling
          ,a.cd_produto
          ,a.cd_codigo_barras
          ,a.nm_produto
          ,a.nm_produto_completo
          ,a.ds_variacao
          ,a.ds_unidade
          ,a.qt_item
          ,a.vl_item
          ,a.vl_total_item
          ,a.ds_tipo_produto
          ,a.ds_subcategoria
          ,a.ds_categoria
          ,a.ds_classificacao_produto
          ,a.ds_origem_produto
          ,b.lk_produto_compra
     from cte_base         as a
left join cte_link_produto as b
       on a.cd_codigo_interno = b.cd_codigo_interno
      and a.cd_produto = b.cd_produto
)

  select *
    from cte_base_link