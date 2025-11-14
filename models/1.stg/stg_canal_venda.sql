{{ config(
    tags = ['stg'],
    enabled = true
)}}

with cte_canal as (
   select nullif(trim(id), '')                       as cd_loja
         ,nullif(trim(descricao), '')                as nm_loja
         ,nullif(trim(tipo), '')                     as ds_tipo_loja
     from {{ source('erathos', 'canais_venda') }}
)

select *
  from cte_canal