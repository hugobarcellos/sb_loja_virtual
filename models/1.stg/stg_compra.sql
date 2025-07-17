{{ config(
    tags = ['stg', 'compra'],
    enabled = true
)}}

with cte_compra as (
  select nullif(trim(id), '')                                   as cd_codigo_interno
        ,nullif(trim(numero), '')                               as cd_compra
        ,nullif(trim(data), '')                                 as dt_compra
        ,nullif(nullif(trim(data_prevista), ''), '0000-00-00')  as dt_prevista_compra
        ,nullif(trim(fornecedor__id), '')                       as cd_fornecedor
        ,nullif(trim(total_produtos), '')                       as vl_total_item
        ,nullif(trim(total), '')                                as vl_total_compra
        ,nullif(trim(situacao__valor), '')                      as cd_situacao_valor
    from {{ source('erathos', 'pedidos_compras') }}
)

, cte_tratamentos_compra as (
  select cd_codigo_interno                                as cd_codigo_interno
        ,cd_compra                                        as cd_compra
        ,cast(dt_compra as date)                          as dt_compra
        ,cast(dt_prevista_compra as date)                 as dt_prevista_compra
        ,cd_fornecedor                                    as cd_fornecedor
        ,vl_total_item                                    as vl_total_item
        ,vl_total_compra                                  as vl_total_compra
        ,case
          when cd_situacao_valor = '2' then 'CANCELADO'
          when cd_situacao_valor = '1' then 'ATENDIDO' 
          when cd_situacao_valor = '0' then 'EM ABERTO'
          else null                                      end ds_status_compra
    from cte_compra 
)

select *
  from cte_tratamentos_compra