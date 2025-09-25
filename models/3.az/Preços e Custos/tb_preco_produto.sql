{{ config(
    tags = ['az', 'produto', 'snaps'],
    enabled = true
)}}

with cte_preco as (
    select distinct
           cd_produto_bling
          ,cd_produto
          ,cd_codigo_barras
          ,nm_produto
          ,ds_variacao
          ,vl_custo_cadastro
          ,vl_custo_ultima_compra
          ,vl_preco_venda
          ,vl_preco_venda_por
          ,ds_subcategoria
          ,ds_categoria
          ,ds_classificacao_produto
          ,ds_origem_produto
          ,ds_tipo_produto
          ,fg_produto_composicao
          ,fg_produto_base_composicao
          ,fg_produto_fabricado
          ,pr_desconto_padrao 
          ,pr_meta_margem 
          ,tx_aliquota_cartao
          ,tx_fixa_cartao
          ,tx_aliquota_pix
          ,vl_desconto_fixo_pix
          ,vl_materiais_envio
          ,dt_ultima_compra
          ,dt_ultima_ingestao
          ,dt_ultima_atualizacao
      from {{ ref('tb_preco_produto_base') }} 
)

, cte_hist as (
    select cd_produto_bling
          ,cd_produto
          ,cd_codigo_barras
          ,nm_produto
          ,ds_variacao
          ,vl_custo_final
          ,vl_preco_final
          ,fg_alteracao
          ,vl_custo_final_anterior
          ,vl_preco_final_anterior
          ,dt_ini_vigencia
          ,hr_ini_vigencia
          ,dt_fim_vigencia
          ,hr_fim_vigencia
          ,dt_ultima_atualizacao
          ,nr_linha
     from {{ ref('tb_hist_preco_custo_produto') }} 
    where nr_linha = 1
)

, cte_base as (
    select distinct
           a.cd_produto_bling
          ,a.cd_produto
          ,a.cd_codigo_barras
          ,a.nm_produto
          ,a.ds_variacao
          ,a.vl_custo_cadastro
          ,a.vl_custo_ultima_compra
          ,a.vl_preco_venda
          ,a.vl_preco_venda_por
          ,b.vl_custo_final_anterior
          ,b.vl_preco_final_anterior
          ,a.ds_subcategoria
          ,a.ds_categoria
          ,a.ds_classificacao_produto
          ,a.ds_origem_produto
          ,a.ds_tipo_produto
          ,a.fg_produto_composicao
          ,a.fg_produto_base_composicao
          ,a.fg_produto_fabricado
          ,b.fg_alteracao
          ,a.pr_desconto_padrao 
          ,a.pr_meta_margem 
          ,a.tx_aliquota_cartao
          ,a.tx_fixa_cartao
          ,a.tx_aliquota_pix
          ,a.vl_desconto_fixo_pix
          ,a.vl_materiais_envio
          ,b.dt_ini_vigencia
          ,a.dt_ultima_compra
          ,a.dt_ultima_ingestao
          ,a.dt_ultima_atualizacao
      from cte_preco                as a
 left join cte_hist                 as b 
        on a.cd_produto_bling = b.cd_produto_bling
     where a.ds_tipo_produto not in ('PAI')
)

  select *
    from cte_base
order by nm_produto
        ,ds_variacao