{{ config(
    tags = ['stg'],
    enabled = true
)}}

SELECT
  id,
  cliente,
  data,
  total,
  status
FROM {{ source('erathos', 'pedidos_vendas') }}