

with cte_estoque as (
    select distinct
           cd_produto_bling
          ,cd_produto
          ,nm_produto
          ,nm_produto_completo
          ,ds_variacao
          ,ds_subcategoria
          ,ds_categoria
          ,ds_classificacao_produto
          ,ds_origem_produto
          ,nm_fornecedor
          ,lk_ultima_compra
          ,qt_estoque_minimo
          ,qt_estoque_atual
          ,vl_custo_cadastro
          ,vl_custo_ultima_compra
    from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_estoque`
   where ds_categoria = '[Interno] Suprimentos'
)

, cte_materia_prima as (
     select distinct 
           cd_produto_bling_composicao
          ,cd_produto_composicao
          ,nm_produto_completo_composicao
          ,ds_variacao_composicao
          ,sum(qt_mes_atual_materia_prima)     as qt_mes_atual_materia_prima 
          ,sum(qt_mes_anterior_materia_prima)  as qt_mes_anterior_materia_prima
          ,sum(qt_tres_meses_materia_prima)    as qt_tres_meses_materia_prima
      from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_venda_produto_fabricado`
  group by cd_produto_bling_composicao
          ,cd_produto_composicao
          ,nm_produto_completo_composicao
          ,ds_variacao_composicao
)

, cte_compra_produto as (
    select cd_compra
          ,cd_produto_bling
          ,qt_item
          ,vl_item
          ,lk_produto_compra
          ,ds_status_compra
          ,nr_seq
     from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_agg_compra_produto`
)

, cte_base as (
    select distinct
           a.cd_produto_bling
          ,a.cd_produto
          ,a.nm_produto
          ,a.nm_produto_completo
          ,a.ds_variacao
          ,a.ds_subcategoria
          ,a.ds_categoria
          ,a.ds_classificacao_produto
          ,a.ds_origem_produto
          ,if(b.cd_produto_bling_composicao is not null, true, false) as fg_materia_prima
          ,a.nm_fornecedor
          ,a.lk_ultima_compra
          ,a.qt_estoque_minimo
          ,a.qt_estoque_atual
          ,a.vl_custo_cadastro
          ,a.vl_custo_ultima_compra
          ,b.qt_mes_atual_materia_prima
          ,b.qt_mes_anterior_materia_prima
          ,b.qt_tres_meses_materia_prima
          ,sum(c.qt_item)                                     as qt_item_compra_pendente
          ,max(c.vl_item)                                     as vl_item_compra_pendente
      from cte_estoque        as a
 left join cte_materia_prima  as b
        on a.cd_produto_bling = b.cd_produto_bling_composicao
 left join cte_compra_produto as c
        on a.cd_produto_bling = c.cd_produto_bling
       and c.ds_status_compra in ('EM ABERTO')
  group by a.cd_produto_bling
          ,a.cd_produto
          ,a.nm_produto
          ,a.nm_produto_completo
          ,a.ds_variacao
          ,a.ds_subcategoria
          ,a.ds_categoria
          ,a.ds_classificacao_produto
          ,a.ds_origem_produto
          ,b.cd_produto_bling_composicao
          ,a.nm_fornecedor
          ,a.lk_ultima_compra
          ,a.qt_estoque_minimo
          ,a.qt_estoque_atual
          ,a.vl_custo_cadastro
          ,a.vl_custo_ultima_compra
          ,b.qt_mes_atual_materia_prima
          ,b.qt_mes_anterior_materia_prima
          ,b.qt_tres_meses_materia_prima
)

select *
  from cte_base