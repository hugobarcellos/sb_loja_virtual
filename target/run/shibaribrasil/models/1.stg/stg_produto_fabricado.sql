

  create or replace view `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_produto_fabricado`
  OPTIONS(
      description=""""""
    )
  as 

with cte_base as (
   select replace(trim(cd_produto), '.', '')             as cd_produto
         ,trim(nm_produto_completo)                      as nm_produto_completo
         ,replace(trim(cd_produto_composicao), '.', '')  as cd_produto_composicao
         ,trim(nm_produto_completo_composicao)           as nm_produto_completo_composicao
         ,qt_produto_composicao                          as qt_produto_composicao 
     from `igneous-sandbox-381622`.`datalake_drive`.`drive_produto_fabricacao_propria`
    where trim(cd_produto) is not null
)

select cd_produto
      ,nm_produto_completo
      ,cd_produto_composicao 
      ,nm_produto_completo_composicao 
      ,qt_produto_composicao 
  from cte_base;

