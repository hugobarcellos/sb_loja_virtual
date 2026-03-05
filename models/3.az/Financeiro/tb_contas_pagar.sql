{{ config(
    tags = ['az', 'financeiro'],
    enabled = true
)}}

with cte_header as (
  select id
        ,contato__id
        ,forma_pagamento__id
        ,situacao
        ,valor
        ,vencimento
    from {{ source('erathos', 'contas_pagar') }}
)

, cte_item as (
  select id
        ,data_emissao
        ,competencia
        ,historico
        ,categoria__id
        ,forma_pagamento__id
        ,ocorrencia__tipo
        ,portador__id
        ,saldo
        ,situacao
        ,valor
        ,vencimento
        ,date(_erathos_inserted_at) dt_atualizacao_erathos
    from {{ source('erathos', 'contas_pagar_detalhes') }}
)

, cte_categoria_financeira as (
  select coalesce(pai.id, filho.id)                                as id_categoria
        ,coalesce(pai.descricao, filho.descricao)                  as categoria
        ,coalesce(case when pai.id is null then null
                       else filho.id end,   filho.id)              as id_subcategoria
        ,coalesce(case when pai.id is null then null
                       else filho.descricao end, filho.descricao)  as subcategoria
     from {{ source('erathos', 'categorias_financeiras') }} filho
left join {{ source('erathos', 'categorias_financeiras') }} pai
       on filho.id_categoria_pai = pai.id
)

, cte_forma_pagamento as (
   select distinct
          cd_forma_pagamento
         ,nm_forma_pagamento
         ,ds_condicao_pagamento
         ,qt_dias_pagamento
         ,vl_taxa_aliquota
         ,vl_taxa_fixa
    from {{ ref('stg_forma_pagamento') }}
)

, cte_contato as (
  select distinct
         cd_contato 
        ,nm_contato 
    from {{ ref('stg_contatos') }}
)

, cte_base_pagar as (
  select a.id                               as id
        ,b.data_emissao                     as dt_emissao
        ,b.competencia                      as dt_competencia
        ,b.historico                        as ds_descricao
        ,c.subcategoria                     as ds_subcategoria
        ,c.categoria                        as ds_categoria
        ,f.nm_contato                       as nm_fornecedor
        ,e.nm_forma_pagamento               as nm_forma_pagamento
        ,case
          when b.portador__id = '14890939403' then 'Shibari Brasil - Caixa'
          else null end ds_conta
        ,case
          when b.situacao = '2' then 'Pago'
          when b.situacao = '1' and cast(b.vencimento as date) = current_date then 'Vence Hoje'
          when b.situacao = '1' and cast(b.vencimento as date) > current_date then 'Em Aberto'
          when b.situacao = '1' and cast(b.vencimento as date) < current_date then 'Atrasado'
          else null end ds_situacao
        ,cast(b.valor as float64)           as vl_valor
        ,b.vencimento                       as dt_vencimento
        ,b.dt_atualizacao_erathos           as dt_atualizacao_erathos
     from cte_header               as a   
left join cte_item                 as b 
       on a.id = b.id
left join cte_categoria_financeira as c
       on b.categoria__id = c.id_subcategoria
-- left join cte_categoria_financeira as d
--        on b.categoria__id = c.id_categoria_pai
left join cte_forma_pagamento      as e   
       on b.forma_pagamento__id = e.cd_forma_pagamento
left join cte_contato              as f  
       on a.contato__id = f.cd_contato
)

select *
  from cte_base_pagar