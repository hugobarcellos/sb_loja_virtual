{{ config(
    tags = ['stg'],
    enabled = true
)}}

SELECT
  id
FROM {{ source('erathos', 'pedidos_vendas') }}