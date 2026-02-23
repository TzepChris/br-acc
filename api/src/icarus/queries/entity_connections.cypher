MATCH (center) WHERE elementId(center) = $entity_id
CALL apoc.path.subgraphAll(center, {
  relationshipFilter: "SOCIO_DE|DOOU|CANDIDATO_EM|VENCEU|AUTOR_EMENDA|SANCIONADA|OPERA_UNIDADE|DEVE|RECEBEU_EMPRESTIMO|EMBARGADA|MANTEDORA_DE|BENEFICIOU|GEROU_CONVENIO",
  labelFilter: "-User|-Investigation|-Annotation|-Tag",
  maxLevel: $depth,
  limit: 200
})
YIELD nodes, relationships
WITH center, nodes, relationships
UNWIND relationships AS r
WITH center,
     startNode(r) AS src,
     endNode(r) AS tgt,
     r
RETURN center AS e,
       r,
       CASE WHEN elementId(src) = elementId(center) THEN tgt ELSE src END AS connected,
       labels(center) AS source_labels,
       CASE WHEN elementId(src) = elementId(center) THEN labels(tgt) ELSE labels(src) END AS target_labels,
       type(r) AS rel_type,
       elementId(startNode(r)) AS source_id,
       elementId(endNode(r)) AS target_id,
       elementId(r) AS rel_id