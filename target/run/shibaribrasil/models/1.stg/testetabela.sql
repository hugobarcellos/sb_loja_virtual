

  create or replace view `igneous-sandbox-381622`.`dbt_dw_stg`.`testetabela`
  OPTIONS()
  as 

SELECT
  id
FROM `igneous-sandbox-381622`.`datalake_bling`.`pedidos_vendas`;

