MATCH (n) WHERE elementId(n) = $entity_id
RETURN apoc.node.degree(n) AS degree