// ICARUS Neo4j Schema — Constraints and Indexes
// Applied on database initialization

// ── Uniqueness Constraints ──────────────────────────────
CREATE CONSTRAINT person_cpf_unique IF NOT EXISTS
  FOR (p:Person) REQUIRE p.cpf IS UNIQUE;

CREATE CONSTRAINT company_cnpj_unique IF NOT EXISTS
  FOR (c:Company) REQUIRE c.cnpj IS UNIQUE;

CREATE CONSTRAINT contract_contract_id_unique IF NOT EXISTS
  FOR (c:Contract) REQUIRE c.contract_id IS UNIQUE;

CREATE CONSTRAINT sanction_sanction_id_unique IF NOT EXISTS
  FOR (s:Sanction) REQUIRE s.sanction_id IS UNIQUE;

CREATE CONSTRAINT public_office_cpf_unique IF NOT EXISTS
  FOR (po:PublicOffice) REQUIRE po.cpf IS UNIQUE;

CREATE CONSTRAINT investigation_id_unique IF NOT EXISTS
  FOR (i:Investigation) REQUIRE i.id IS UNIQUE;

CREATE CONSTRAINT amendment_id_unique IF NOT EXISTS
  FOR (a:Amendment) REQUIRE a.amendment_id IS UNIQUE;

// ── Indexes ─────────────────────────────────────────────
CREATE INDEX person_name IF NOT EXISTS
  FOR (p:Person) ON (p.name);

CREATE INDEX person_author_key IF NOT EXISTS
  FOR (p:Person) ON (p.author_key);

CREATE INDEX person_sq_candidato IF NOT EXISTS
  FOR (p:Person) ON (p.sq_candidato);

CREATE INDEX company_razao_social IF NOT EXISTS
  FOR (c:Company) ON (c.razao_social);

CREATE INDEX contract_value IF NOT EXISTS
  FOR (c:Contract) ON (c.value);

CREATE INDEX contract_object IF NOT EXISTS
  FOR (c:Contract) ON (c.object);

CREATE INDEX sanction_type IF NOT EXISTS
  FOR (s:Sanction) ON (s.type);

CREATE INDEX election_year IF NOT EXISTS
  FOR (e:Election) ON (e.year);

CREATE INDEX election_composite IF NOT EXISTS
  FOR (e:Election) ON (e.year, e.cargo, e.uf, e.municipio);

CREATE INDEX amendment_object IF NOT EXISTS
  FOR (a:Amendment) ON (a.object);

CREATE INDEX company_cnae_principal IF NOT EXISTS
  FOR (c:Company) ON (c.cnae_principal);

CREATE INDEX contract_contracting_org IF NOT EXISTS
  FOR (c:Contract) ON (c.contracting_org);

CREATE INDEX contract_date IF NOT EXISTS
  FOR (c:Contract) ON (c.date);

// ── Finance Indexes ───────────────────────────────────
CREATE INDEX finance_id_idx IF NOT EXISTS
  FOR (f:Finance) ON (f.finance_id);

CREATE INDEX finance_type IF NOT EXISTS
  FOR (f:Finance) ON (f.type);

CREATE INDEX finance_value IF NOT EXISTS
  FOR (f:Finance) ON (f.value);

CREATE INDEX finance_date IF NOT EXISTS
  FOR (f:Finance) ON (f.date);

CREATE INDEX finance_source IF NOT EXISTS
  FOR (f:Finance) ON (f.source);

// ── Embargo Indexes ───────────────────────────────────
CREATE INDEX embargo_id IF NOT EXISTS
  FOR (e:Embargo) ON (e.embargo_id);

CREATE INDEX embargo_uf IF NOT EXISTS
  FOR (e:Embargo) ON (e.uf);

CREATE INDEX embargo_biome IF NOT EXISTS
  FOR (e:Embargo) ON (e.biome);

// ── Sanction Date Index ───────────────────────────────
CREATE INDEX sanction_date_start IF NOT EXISTS
  FOR (s:Sanction) ON (s.date_start);

CREATE INDEX amendment_date IF NOT EXISTS
  FOR (a:Amendment) ON (a.date);

// ── New Node Constraints ────────────────────────────────
CREATE CONSTRAINT finance_id_unique IF NOT EXISTS
  FOR (f:Finance) REQUIRE f.finance_id IS UNIQUE;

CREATE CONSTRAINT embargo_id_unique IF NOT EXISTS
  FOR (e:Embargo) REQUIRE e.embargo_id IS UNIQUE;

CREATE CONSTRAINT education_school_id_unique IF NOT EXISTS
  FOR (e:Education) REQUIRE e.school_id IS UNIQUE;

CREATE CONSTRAINT convenio_id_unique IF NOT EXISTS
  FOR (c:Convenio) REQUIRE c.convenio_id IS UNIQUE;

CREATE CONSTRAINT laborstats_id_unique IF NOT EXISTS
  FOR (l:LaborStats) REQUIRE l.stats_id IS UNIQUE;

// ── Education Indexes ───────────────────────────────────
CREATE INDEX education_name IF NOT EXISTS
  FOR (e:Education) ON (e.name);

// ── Convenio Indexes ────────────────────────────────────
CREATE INDEX convenio_date IF NOT EXISTS
  FOR (c:Convenio) ON (c.date);

// ── LaborStats Indexes ──────────────────────────────────
CREATE INDEX laborstats_uf IF NOT EXISTS
  FOR (l:LaborStats) ON (l.uf);

// ── Fulltext Search Index ───────────────────────────────
CREATE FULLTEXT INDEX entity_search IF NOT EXISTS
  FOR (n:Person|Company)
  ON EACH [n.name, n.razao_social, n.cpf, n.cnpj];

// ── User Constraints ────────────────────────────────────
CREATE CONSTRAINT user_email_unique IF NOT EXISTS
  FOR (u:User) REQUIRE u.email IS UNIQUE;
