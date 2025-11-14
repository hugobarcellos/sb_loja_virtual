

  create or replace view `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_pedido`
  OPTIONS(
      description=""""""
    )
  as 

with cte_pedido as (
  select nullif(trim(id), '')                        as cd_codigo_interno
        ,nullif(trim(numero), '')                    as cd_pedido
        ,nullif(trim(data), '')                      as dt_pedido
        ,nullif(trim(situacao__id), '')              as cd_status
        ,nullif(trim(total_produtos), '')            as vl_total_item
        ,nullif(trim(total), '')                     as vl_total_pedido
        ,nullif(trim(contato__id), '')               as cd_contato
        ,nullif(trim(contato__nome), '')             as nm_contato
        ,nullif(trim(contato__numero_documento), '') as nr_doc_contato
        ,nullif(trim(loja__id), '')                  as cd_loja
    from `igneous-sandbox-381622`.`datalake_bling`.`pedidos_vendas`
)

, cte_tratamentos_pedido as (
  select cd_codigo_interno                      as cd_codigo_interno
        ,cd_pedido                              as cd_pedido
        ,dt_pedido                              as dt_pedido
        ,cd_status                              as cd_status_pedido
        ,case
          when cd_status = '6'  then 'EM ABERTO'
          when cd_status = '12' then 'CANCELADO'
          when cd_status = '9'  then 'ATENDIDO'
          when cd_status = '15' then 'AGUARDANDO PGTO'
          else null                            end ds_status_pedido
        ,cast(vl_total_item as float64)         as vl_total_item
        ,cast(vl_total_pedido as float64)       as vl_total_pedido
        ,cd_contato                             as cd_contato
        ,nm_contato                             as nm_contato
        ,nr_doc_contato                         as nr_doc_contato
        ,cd_loja                                as cd_loja
    from cte_pedido 
)

select *
  from cte_tratamentos_pedido;

