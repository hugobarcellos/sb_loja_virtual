

  create or replace view `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_estoque`
  OPTIONS(
      description=""""""
    )
  as 

with cte_estoque as (
   select trim(a.id)                                                               as cd_produto_bling
         ,nullif(trim(replace(a.codigo, '.', '')), '')                             as cd_produto
         ,nullif(trim(a.gtin), '')                                                 as cd_codigo_barras
         ,nullif(trim(a.nome), '')                                                 as nm_produto_completo
         ,coalesce(cast(nullif(a.estoque__minimo,'') as float64), 0)               as qt_estoque_minimo
         ,coalesce(cast(nullif(a.estoque__saldo_virtual_total,'') as float64), 0)  as qt_estoque_atual
         ,datetime(timestamp(a._erathos_synced_at), "America/Sao_Paulo")           as dt_ultima_ingestao
     from `igneous-sandbox-381622`.`datalake_bling`.`produtos_detalhes`  as a
left join `igneous-sandbox-381622`.`datalake_bling`.`produtos`           as b 
       on trim(a.id) = trim(b.id) 
)

select distinct *
  from cte_estoque;

