

with cte_contatos as (
   select trim(id)                             as cd_contato 
         ,trim(nome)                           as nm_contato 
         ,trim(numero_documento)               as nr_documento 
         ,trim(email)                          as ds_email 
         ,trim(telefone)                       as nr_telefone 
         ,trim(endereco__geral__uf)            as ds_endereco_uf 
         ,trim(endereco__geral__municipio)     as ds_endereco_municipio
         ,trim(endereco__geral__bairro)        as ds_endereco_bairro
         ,trim(endereco__geral__cep)           as ds_endereco_cep 
         ,trim(endereco__geral__endereco)      as ds_endereco_endereco
         ,trim(endereco__geral__numero)        as ds_endereco_numero 
         ,trim(endereco__geral__complemento)   as ds_endereco_complemento 
         ,trim(fantasia)                       as nm_fantasia
         ,json_value(json_item, '$.descricao') as ds_tipo_contato
     from `igneous-sandbox-381622`.`datalake_bling`.`contato_detalhes`,
     unnest(json_extract_array(tipos_contato)) as json_item
)

select *
  from cte_contatos