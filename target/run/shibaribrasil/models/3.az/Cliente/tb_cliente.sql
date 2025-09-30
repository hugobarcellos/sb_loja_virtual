
  
    

    create or replace table `igneous-sandbox-381622`.`dbt_dw_az`.`tb_cliente`
      
    
    

    OPTIONS(
      description=""""""
    )
    as (
      

with cte_contato as (
  select cd_contato
        ,nm_contato
        ,nr_documento
        ,ds_email
        ,nr_telefone
        ,ds_endereco_uf
        ,ds_endereco_municipio
    from `igneous-sandbox-381622`.`dbt_dw_stg`.`stg_contatos`
   where ds_tipo_contato = 'Cliente'
)

, cte_pedido as (
  select cd_codigo_interno
        ,cd_pedido
        ,dt_prim_dia_mes	
        ,dt_pedido
        ,cd_status_pedido
        ,ds_status_pedido
        ,cd_produto_bling
        ,cd_produto
        ,nm_produto
        ,nm_produto_completo
        ,ds_subcategoria
        ,ds_categoria	
        ,ds_classificacao_produto	
        ,ds_origem_produto	
        ,qt_item	
        ,vl_item	
        ,vl_total_item
        ,vl_desconto_rateio	
        ,vl_frete_rateio
        ,vl_total_pedido	
        ,vl_custo_item
        ,vl_custo_pedido
        ,ds_forma_pagamento
        ,cd_contato
        ,nm_contato
        ,nr_doc_contato
        ,ds_loja
    from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_pedido`
)

, cte_base as (
   select a.cd_contato
         ,a.nm_contato
         ,a.nr_documento
         ,a.ds_email
         ,a.nr_telefone
         ,a.ds_endereco_uf
         ,a.ds_endereco_municipio
         ,count(distinct cd_pedido)        qt_pedido
         ,count(distinct cd_produto_bling) qt_produtos
         ,sum(qt_item)                     qt_pecas
         ,sum(vl_total_pedido)             vl_total_pedido
         ,max(dt_pedido)                   dt_ult_pedido
         ,min(dt_pedido)                   dt_prim_pedido
     from cte_contato as a
left join cte_pedido  as b
       on a.cd_contato = b.cd_contato
      and b.ds_status_pedido not in ('CANCELADO')
 group by a.cd_contato
         ,a.nm_contato
         ,a.nr_documento
         ,a.ds_email
         ,a.nr_telefone
         ,a.ds_endereco_uf
         ,a.ds_endereco_municipio
)

select *
      ,if(qt_pedido = 0, true, false) fg_abandono_pedido
  from cte_base
    );
  