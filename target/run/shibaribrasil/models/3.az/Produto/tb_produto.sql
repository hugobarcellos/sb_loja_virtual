
  
    

    create or replace table `igneous-sandbox-381622`.`dbt_dw_az`.`tb_produto`
      
    
    

    OPTIONS(
      description=""""""
    )
    as (
      

with cte_produto as (
   select *
     from `igneous-sandbox-381622`.`dbt_dw_dw`.`dm_produto`
)

, cte_categoria as (
   select cd_categoria
         ,nm_categoria
         ,cd_categoria_pai
         ,ds_tipo_categoria
     from `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_categoria_produto`
)

-- join com a dimensao de categoria de produto
, cte_tratamentos as (
    select distinct
           a.cd_produto_bling
          ,a.cd_produto
          ,a.cd_codigo_barras
          ,a.nm_produto
          ,a.nm_produto_completo
          ,ds_variacao
          ,a.ds_tipo_produto
          ,a.cd_produto_bling_pai
          ,a.fg_produto_composicao
          ,a.ds_tipo_estoque
          ,a.qt_estoque_minimo
          ,a.qt_estoque_atual
          ,a.vl_custo_compra
          ,a.vl_custo_total
          ,a.vl_preco_venda
          ,b.nm_categoria                           as ds_subcategoria
          ,coalesce(c.nm_categoria, b.nm_categoria) as ds_categoria
          ,a.ds_situacao
          ,a.dm_altura
          ,a.dm_largura
          ,a.dm_profundidade
          ,a.dm_peso_bruto
          ,a.dm_peso_liquido
          ,a.ds_descricao_produto
          ,a.dt_ultima_ingestao
      from cte_produto   as a
 left join cte_categoria    as b
        on a.cd_categoria = b.cd_categoria
 left join cte_categoria    as c
        on b.cd_categoria_pai = c.cd_categoria     
)

, cte_campos_customizados as (
    select cd_produto_bling
          ,cd_produto
          ,ds_classificacao_produto
          ,ds_origem_produto
          ,vl_alteracao_preco
      from `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_campo_customizado_produto`
)

, cte_imagem_produto as (
    select cd_produto_bling
          ,cd_produto
          ,lk_imagem_produto
      from `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_imagem_produto`
     where seq_imagem = 1
)

, cte_lead_time as (
    select distinct
           ds_origem_produto
          ,qt_lead_time
          ,qt_cobertura_desejada
      from `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_lead_time`
)

, cte_produto_fabricado as (
    select cd_produto
          ,nm_produto_completo
          ,cd_produto_composicao 
          ,nm_produto_completo_composicao 
          ,qt_produto_composicao 
     from `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_produto_fabricado`
)

, cte_link_compra_produto as (
    select cd_produto
          ,lk_produto_compra
     from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_link_compra_produto`
    where seq = 1
)

, cte_joins as (
    select distinct
           a.cd_produto_bling
          ,a.cd_produto
          ,a.cd_codigo_barras
          ,a.nm_produto
          ,a.nm_produto_completo
          ,a.ds_variacao
          ,a.ds_tipo_produto
          ,a.cd_produto_bling_pai
          ,a.fg_produto_composicao
          ,if(e.cd_produto is not null, true, false) as fg_produto_fabricado
          ,a.ds_tipo_estoque
          ,a.qt_estoque_minimo
          ,a.qt_estoque_atual
          ,a.vl_custo_compra
          ,a.vl_custo_total
          ,a.vl_preco_venda
          ,b.vl_alteracao_preco                      as vl_preco_venda_por
          ,a.ds_subcategoria
          ,a.ds_categoria
          ,b.ds_classificacao_produto
          ,b.ds_origem_produto
          ,a.dm_altura
          ,a.dm_largura
          ,a.dm_profundidade
          ,a.dm_peso_bruto
          ,a.dm_peso_liquido
          ,d.qt_lead_time
          ,d.qt_cobertura_desejada
          ,a.ds_descricao_produto
          ,f.lk_produto_compra                       as lk_ultima_compra
          ,c.lk_imagem_produto
          ,a.dt_ultima_ingestao
          ,datetime(current_timestamp(), "America/Sao_Paulo") as dt_ultima_atualizacao
      from cte_tratamentos            as a
 left join cte_campos_customizados    as b
        on a.cd_produto_bling = b.cd_produto_bling 
 left join cte_imagem_produto         as c
        on a.cd_produto_bling = c.cd_produto_bling     
 left join cte_lead_time              as d
        on b.ds_origem_produto = d.ds_origem_produto     
 left join cte_produto_fabricado      as e
        on a.cd_produto = e.cd_produto
 left join cte_link_compra_produto    as f
        on a.cd_produto = f.cd_produto
)

  select *
    from cte_joins
order by nm_produto_completo
    );
  