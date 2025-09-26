

with cte_fabricado as (
    select distinct 
           cd_produto_bling
          ,cd_produto
          ,nm_produto
          ,nm_produto_completo
          ,ds_variacao
          ,cd_produto_bling_composicao
          ,cd_produto_composicao
          ,nm_produto_composicao
          ,nm_produto_completo_composicao
          ,ds_variacao_composicao
          ,qt_produto_composicao
          ,vl_custo_compra
          ,vl_custo_compra_total
      from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_produto_fabricado`
)

, cte_venda as (
   select cd_produto_bling
         ,cd_produto
         ,qt_pecas_mes_atual 
         ,qt_pecas_mes_anterior 
         ,qt_pecas_tres_meses 
     from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_venda_produto_base`
)

, cte_base as (
    select distinct 
           a.cd_produto_bling
          ,a.cd_produto
          ,a.nm_produto
          ,a.nm_produto_completo
          ,a.ds_variacao
          ,a.cd_produto_bling_composicao
          ,a.cd_produto_composicao
          ,a.nm_produto_composicao
          ,a.nm_produto_completo_composicao
          ,a.ds_variacao_composicao
          ,a.qt_produto_composicao
          ,a.vl_custo_compra
          ,a.vl_custo_compra_total
          ,(a.qt_produto_composicao * b.qt_pecas_mes_atual)      as qt_mes_atual_materia_prima 
          ,(a.qt_produto_composicao * b.qt_pecas_mes_anterior)   as qt_mes_anterior_materia_prima
          ,(a.qt_produto_composicao * b.qt_pecas_tres_meses)     as qt_tres_meses_materia_prima
          ,(a.vl_custo_compra_total * b.qt_pecas_mes_atual)      as vl_custo_mes_atual_materia_prima 
          ,(a.vl_custo_compra_total * b.qt_pecas_mes_anterior)   as vl_custo_mes_anterior_materia_prima
          ,(a.vl_custo_compra_total * b.qt_pecas_tres_meses)     as vl_custo_tres_meses_materia_prima
          ,b.qt_pecas_mes_atual
          ,b.qt_pecas_mes_anterior
          ,b.qt_pecas_tres_meses
      from cte_fabricado as a
 left join cte_venda     as b
        on a.cd_produto_bling = b.cd_produto_bling
)

select *
  from cte_base