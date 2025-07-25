

  create or replace view `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_produto`
  OPTIONS(
      description=""""""
    )
  as 

with cte_produto as (
   select trim(a.id)                                                               as cd_produto_bling
         ,nullif(trim(replace(a.codigo, '.', '')), '')                             as cd_produto
         ,nullif(trim(a.gtin), '')                                                 as cd_codigo_barras
         ,nullif(trim(a.nome), '')                                                 as nm_produto_completo
         ,coalesce(cast(nullif(a.dimensoes__altura,'') as float64), 0)             as dm_altura
         ,coalesce(cast(nullif(a.dimensoes__largura,'') as float64), 0)            as dm_largura
         ,coalesce(cast(nullif(a.dimensoes__profundidade,'') as float64), 0)       as dm_profundidade
         ,coalesce(cast(nullif(a.peso_bruto,'') as float64), 0)                    as dm_peso_bruto
         ,coalesce(cast(nullif(a.peso_liquido,'') as float64), 0)                  as dm_peso_liquido
         ,coalesce(cast(nullif(a.estoque__minimo,'') as float64), 0)               as qt_estoque_minimo
         ,coalesce(cast(nullif(a.estoque__saldo_virtual_total,'') as float64), 0)  as qt_estoque_atual
         ,nullif(a.estrutura__tipo_estoque,'')                                     as ds_tipo_estoque
         ,cast(nullif(a.fornecedor__preco_compra,'') as float64)                   as vl_custo_compra
         ,cast(nullif(a.fornecedor__preco_custo,'') as float64)                    as vl_custo_total
         ,cast(nullif(a.preco,'') as float64)                                      as vl_preco_venda
         ,nullif(a.situacao,'')                                                    as ds_situacao
         ,nullif(a.tipo,'')                                                        as ds_tipo
         ,nullif(a.formato,'')                                                     as ds_formato
         ,nullif(a.categoria__id,'')                                               as cd_categoria
         ,nullif(a.descricao_curta,'')                                             as ds_descricao_produto
         ,nullif(trim(left(b.id_produto_pai,11)), '')                              as cd_produto_bling_pai
         ,datetime(timestamp(a._erathos_synced_at), "America/Sao_Paulo")           as dt_ultima_ingestao
     from `igneous-sandbox-381622`.`datalake_bling`.`produtos_detalhes`  as a
left join `igneous-sandbox-381622`.`datalake_bling`.`produtos`           as b 
       on trim(a.id) = trim(b.id) 
)

select *
  from cte_produto;

