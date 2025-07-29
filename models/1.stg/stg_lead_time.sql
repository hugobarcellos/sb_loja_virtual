{{ config(
    tags = ['stg', 'produto'],
    enabled = true
)}}

select distinct 
       ds_origem_produto
      ,case 
        when ds_origem_produto in ('Revenda Nacional', 'Produção Própria', 'Uso e Consumo')
          then 15
        when ds_origem_produto in ('Revenda Importada')
          then 25
        else null end qt_lead_time
      ,case 
        when ds_origem_produto in ('Revenda Nacional', 'Produção Própria', 'Uso e Consumo')
          then 45
        when ds_origem_produto in ('Revenda Importada')
          then 60
        else null end qt_cobertura_desejada
  from {{ ref('stg_campo_customizado_produto') }}