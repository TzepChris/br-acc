// Gather exposure metrics for a given entity
MATCH (e)
WHERE elementId(e) = $entity_id
WITH e, labels(e) AS lbls
OPTIONAL MATCH (e)-[r]-(connected)
WITH e, lbls,
     count(r) AS connection_count,
     collect(DISTINCT
       CASE
         WHEN connected:Contract THEN 'transparencia'
         WHEN connected:Sanction THEN 'ceis_cnep'
         WHEN connected:Election THEN 'tse'
         WHEN connected:Health THEN 'cnes'
         WHEN connected:Finance THEN 'pgfn'
         WHEN connected:Embargo THEN 'ibama'
         WHEN connected:Education THEN 'inep'
         WHEN connected:Convenio THEN 'transferegov'
         ELSE 'cnpj'
       END
     ) AS source_list
OPTIONAL MATCH (e)-[:VENCEU]->(c:Contract)
WITH e, lbls, connection_count, source_list,
     COALESCE(sum(c.value), 0) AS contract_volume
OPTIONAL MATCH (e)-[:DOOU]->(d)
WITH e, lbls, connection_count, source_list, contract_volume,
     COALESCE(sum(d.valor), 0) AS donation_volume
OPTIONAL MATCH (e)-[:RECEBEU_EMPRESTIMO|DEVE]->(f:Finance)
WITH e, lbls, connection_count, source_list, contract_volume, donation_volume,
     COALESCE(sum(f.value), 0) AS debt_loan_volume
RETURN
  elementId(e) AS entity_id,
  lbls AS entity_labels,
  connection_count,
  size(source_list) AS source_count,
  contract_volume + donation_volume + debt_loan_volume AS financial_volume,
  e.cnae_principal AS cnae_principal,
  e.role AS role
