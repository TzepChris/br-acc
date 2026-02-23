CALL {
  MATCH (n) RETURN count(n) AS total_nodes
}
CALL {
  MATCH ()-[r]->() RETURN count(r) AS total_relationships
}
CALL {
  MATCH (p:Person) RETURN count(p) AS person_count
}
CALL {
  MATCH (c:Company) RETURN count(c) AS company_count
}
CALL {
  MATCH (h:Health) RETURN count(h) AS health_count
}
CALL {
  MATCH (f:Finance) RETURN count(f) AS finance_count
}
CALL {
  MATCH (c:Contract) RETURN count(c) AS contract_count
}
CALL {
  MATCH (s:Sanction) RETURN count(s) AS sanction_count
}
CALL {
  MATCH (e:Election) RETURN count(e) AS election_count
}
CALL {
  MATCH (a:Amendment) RETURN count(a) AS amendment_count
}
CALL {
  MATCH (e:Embargo) RETURN count(e) AS embargo_count
}
CALL {
  MATCH (e:Education) RETURN count(e) AS education_count
}
CALL {
  MATCH (c:Convenio) RETURN count(c) AS convenio_count
}
CALL {
  MATCH (l:LaborStats) RETURN count(l) AS laborstats_count
}
RETURN total_nodes, total_relationships,
       person_count, company_count, health_count,
       finance_count, contract_count, sanction_count,
       election_count, amendment_count, embargo_count,
       education_count, convenio_count, laborstats_count
