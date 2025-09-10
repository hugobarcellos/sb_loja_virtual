

with cte_compra as (
  select cd_codigo_interno
        ,cd_compra
        ,dt_compra
        ,cd_fornecedor
    from `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_compra`
   where ds_status_compra not in ('CANCELADO')
)

, cte_item_compra as (
  select cd_codigo_interno
        ,cd_compra
        ,dt_compra
        ,ds_observacoes
        ,cd_produto_bling
    from `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_item_compra`
)

, cte_fornecedor as (
    select distinct 
          cd_contato
         ,nm_contato
     from `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_contatos`
)

, cte_compra_base as (
   select distinct
          a.cd_codigo_interno
         ,a.cd_compra
         ,a.dt_compra
         ,a.cd_fornecedor
         ,c.nm_contato as nm_fornecedor
         ,b.ds_observacoes
         ,b.cd_produto_bling
     from cte_compra        as a
left join cte_item_compra   as b
       on a.cd_codigo_interno = b.cd_codigo_interno
      and a.cd_compra         = b.cd_compra
left join cte_fornecedor    as c
       on a.cd_fornecedor = c.cd_contato
)

, cte_validacao_link as (
  select *
        ,regexp_contains(ds_observacoes, r'\b\d{3,}\s*:\s*https?://') as is_padrao_valido
    from cte_compra_base
)

, cte_link_produto_base as (
  select cd_codigo_interno
        ,cd_compra
        ,dt_compra
        ,cd_fornecedor
        ,nm_fornecedor
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
        ,cd_compra
        ,dt_compra
        ,cd_fornecedor
        ,nm_fornecedor
        ,replace(trim(partes[safe_offset(0)]), '.', '') as cd_produto
        ,trim(partes[safe_offset(1)])                   as lk_produto_compra
    from cte_link_produto_base
   where partes[safe_offset(0)] is not null
     and partes[safe_offset(1)] is not null
)

, final as (
  select cd_codigo_interno
        ,cd_compra
        ,dt_compra
        ,cd_fornecedor
        ,nm_fornecedor
        ,cd_produto
        ,lk_produto_compra
        ,row_number() over (
           partition by cd_produto
               order by dt_compra desc, cd_compra desc
         ) as seq
    from cte_link_produto
)

select *
  from final
 order by cd_produto
         ,seq