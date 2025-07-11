

  create or replace view `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_formas_pagamento`
  OPTIONS(
      description=""""""
    )
  as 

with cte_forma_pagamento as (
   select nullif(trim(id), '')                                                                     as cd_forma_pagamento
         ,nullif(trim(descricao), '')                                                              as nm_forma_pagamento
         ,nullif(trim(condicao), '')                                                               as ds_condicao_pagamento
         ,cast(nullif(taxas__aliquota, 0) as float64)                                              as vl_taxa_aliquota
         ,cast(nullif(taxas__valor, 0) as float64)                                                 as vl_taxa_fixa
         ,cast(nullif(taxas__prazo, 0) as float64)                                                 as qt_dias_pagamento
     from `igneous-sandbox-381622`.`datalake_bling`.`formas_pagamentos_detalhes`
)

select *
  from cte_forma_pagamento;

