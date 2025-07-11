

with cte_forma_pagamento as (
   select nullif(trim(id), '')                                                          as cd_forma_pagamento
         ,nullif(trim(descricao), '')                                                   as nm_forma_pagamento
         ,nullif(trim(condicao), '')                                                    as ds_condicao_pagamento
         ,cast(taxas__prazo as float64)                                                 as qt_dias_pagamento
         ,cast(taxas__aliquota as float64)                                              as vl_taxa_aliquota
         ,cast(taxas__valor as float64)                                                 as vl_taxa_fixa
     from `igneous-sandbox-381622`.`datalake_bling`.`formas_pagamentos_detalhes`
)

select *
  from cte_forma_pagamento