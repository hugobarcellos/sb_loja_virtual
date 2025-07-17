
      
  
    

    create or replace table `igneous-sandbox-381622`.`snapshots`.`snapshot_preco_custo_produto`
      
    
    

    OPTIONS()
    as (
      

    select *,
        to_hex(md5(concat(coalesce(cast(cd_produto_bling as string), ''), '|',coalesce(cast(
    current_timestamp()
 as string), '')))) as dbt_scd_id,
        
    current_timestamp()
 as dbt_updated_at,
        
    current_timestamp()
 as dbt_valid_from,
        nullif(
    current_timestamp()
, 
    current_timestamp()
) as dbt_valid_to
    from (
        



    select cd_produto_bling
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
          ,fg_produto_composicao
     from `igneous-sandbox-381622`.`dbt_dw_az`.`tb_preco_produto`

    ) sbq



    );
  
  