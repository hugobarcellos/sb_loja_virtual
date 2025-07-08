{{ config(
    tags = ['stg'],
    enabled = true
)}}

with cte_categoria as (
   select nullif(trim(id), '')                                                                     as cd_categoria
         ,nullif(trim(descricao), '')                                                              as nm_categoria
         ,nullif(nullif(trim(categoria_pai__id), ''),'0')                                          as cd_categoria_pai
         ,if(nullif(nullif(trim(categoria_pai__id), ''),'0') is null, 'CATEGORIA', 'SUBCATEGORIA') as ds_tipo_categoria
     from {{ source('erathos', 'categorias_produtos') }}
)

select *
  from cte_categoria