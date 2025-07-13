
  
    

    create or replace table `igneous-sandbox-381622`.`dbt_dw_dw`.`dm_produto`
      
    
    

    OPTIONS(
      description=""""""
    )
    as (
      

with cte_produto as (
   select *
     from `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_produto`as a
)

-- cte para tratamentos gerais nos campos de tipo de produto e tipo de estoque
, cte_tratamentos as (
    select cd_produto_bling
          ,cd_produto
          ,cd_codigo_barras
          ,nm_produto_completo
          ,case 
            when ds_formato = 'S' and cd_produto_bling_pai is null then 'SIMPLES'
            when ds_formato = 'V' and cd_produto_bling_pai is null then 'PAI'
            when cd_produto_bling_pai is not null then 'FILHO'
            else 'SIMPLES'                                   end ds_tipo_produto
          ,cd_produto_bling_pai
          ,case when ds_formato = 'E' then true else false   end fg_produto_composicao
          ,if(ds_tipo_estoque = 'V' or ds_formato = 'E', 'VIRTUAL', 'FISICO') as ds_tipo_estoque
          ,qt_estoque_minimo
          ,qt_estoque_atual
          ,vl_custo_compra
          ,vl_custo_total
          ,vl_preco_venda
          ,if(ds_situacao = 'A', 'ATIVO', 'INATIVO')      as ds_situacao
          ,dm_altura
          ,dm_largura
          ,dm_profundidade
          ,dm_peso_bruto
          ,dm_peso_liquido
          ,cd_categoria
          ,ds_descricao_produto
          ,dt_ultima_ingestao
      from cte_produto
)

--normalizando os produtos pai com o mesmo tipo de estoque dos filhos
, cte_base as (
    select distinct
           a.cd_produto_bling
          ,a.cd_produto
          ,a.cd_codigo_barras
          ,a.nm_produto_completo
          ,a.ds_tipo_produto
          ,a.cd_produto_bling_pai
          ,a.fg_produto_composicao
          ,coalesce(b.ds_tipo_estoque, a.ds_tipo_estoque) ds_tipo_estoque
          ,a.qt_estoque_minimo
          ,a.qt_estoque_atual
          ,a.vl_custo_compra
          ,a.vl_custo_total
          ,a.vl_preco_venda
          ,a.ds_situacao
          ,a.dm_altura
          ,a.dm_largura
          ,a.dm_profundidade
          ,a.dm_peso_bruto
          ,a.dm_peso_liquido
          ,a.cd_categoria
          ,a.ds_descricao_produto
          ,a.dt_ultima_ingestao
      from cte_tratamentos  as a
 left join cte_tratamentos  as b
        on a.cd_produto_bling = b.cd_produto_bling_pai
)

--pego o nome do produto pai pra criar uma coluna de nome de produto simples, sem as variações
, cte_nome_simples_base as (
   select distinct
           a.cd_produto_bling_pai
          ,b.nm_produto_completo as nm_produto
      from cte_base  as a
 left join cte_base   as b
        on a.cd_produto_bling_pai = b.cd_produto_bling
     where a.cd_produto_bling_pai is not null
)

--join pra trazer o nome do produto simplificado
, cte_nome_simples as (
    select distinct
           a.cd_produto_bling
          ,a.cd_produto
          ,a.cd_codigo_barras
          ,coalesce(b.nm_produto, a.nm_produto_completo) as nm_produto
          ,a.nm_produto_completo
          ,a.ds_tipo_produto
          ,a.cd_produto_bling_pai
          ,a.fg_produto_composicao
          ,a.ds_tipo_estoque
          ,a.qt_estoque_minimo
          ,a.qt_estoque_atual
          ,a.vl_custo_compra
          ,a.vl_custo_total
          ,a.vl_preco_venda
          ,a.ds_situacao
          ,a.dm_altura
          ,a.dm_largura
          ,a.dm_profundidade
          ,a.dm_peso_bruto
          ,a.dm_peso_liquido
          ,a.cd_categoria
          ,a.ds_descricao_produto
          ,a.dt_ultima_ingestao
      from cte_base                as a
 left join cte_nome_simples_base   as b
        on a.cd_produto_bling_pai = b.cd_produto_bling_pai
)

--retiro o nome do produto pra deixar so a variação e criar uma coluna com esse dado
, cte_base_final as (
    select distinct
           a.cd_produto_bling
          ,a.cd_produto
          ,a.cd_codigo_barras
          ,a.nm_produto
          ,a.nm_produto_completo
          ,coalesce(nullif(trim(replace(a.nm_produto_completo, a.nm_produto, '')), ''), 'Sem Variacao') ds_variacao
          ,a.ds_tipo_produto
          ,a.cd_produto_bling_pai
          ,a.fg_produto_composicao
          ,a.ds_tipo_estoque
          ,a.qt_estoque_minimo
          ,a.qt_estoque_atual
          ,a.vl_custo_compra
          ,a.vl_custo_total
          ,a.vl_preco_venda
          ,a.ds_situacao
          ,a.dm_altura
          ,a.dm_largura
          ,a.dm_profundidade
          ,a.dm_peso_bruto
          ,a.dm_peso_liquido
          ,a.cd_categoria
          ,a.ds_descricao_produto
          ,a.dt_ultima_ingestao
      from cte_nome_simples   as a
)

select *
    from cte_base_final
  order by nm_produto_completo
    );
  