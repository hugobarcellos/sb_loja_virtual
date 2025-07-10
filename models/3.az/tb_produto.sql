{{ config(
    tags = ['dw', 'produto'],
    enabled = true
)}}

with cte_produto as (
   select *
     from {{ ref('dm_produto') }}
)

, cte_categoria as (
   select cd_categoria
         ,nm_categoria
         ,cd_categoria_pai
         ,ds_tipo_categoria
     from {{ ref('stg_categoria_produto') }}
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
          ,fg_alteracao_preco
          ,ds_tipo_alteracao_preco
          ,dt_alteracao_preco
          ,vl_alteracao_preco
      from {{ ref('stg_campo_customizado_produto') }}
)

, cte_imagem_produto as (
    select cd_produto_bling
          ,cd_produto
          ,lk_imagem_produto
      from {{ ref('stg_imagem_produto') }}
     where seq_imagem = 1
)

, cte_join_campos_imagens as (
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
          ,a.ds_tipo_estoque
          ,a.qt_estoque_minimo
          ,a.qt_estoque_atual
          ,a.vl_custo_compra
          ,a.vl_custo_total
          ,a.vl_preco_venda
          ,a.ds_subcategoria
          ,a.ds_categoria
          ,b.ds_classificacao_produto
          ,b.ds_origem_produto
          ,b.fg_alteracao_preco
          ,b.ds_tipo_alteracao_preco
          ,b.dt_alteracao_preco
          ,b.vl_alteracao_preco
          ,a.ds_situacao
          ,a.dm_altura
          ,a.dm_largura
          ,a.dm_profundidade
          ,a.dm_peso_bruto
          ,a.dm_peso_liquido
          ,a.ds_descricao_produto
          ,c.lk_imagem_produto
      from cte_tratamentos            as a
 left join cte_campos_customizados    as b
        on a.cd_produto_bling = b.cd_produto_bling 
 left join cte_imagem_produto         as c
        on a.cd_produto_bling = c.cd_produto_bling     
)

  select *
    from cte_join_campos_imagens
order by nm_produto_completo