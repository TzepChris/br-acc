from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd

from icarus_etl.base import Pipeline

if TYPE_CHECKING:
    from neo4j import Driver
from icarus_etl.loader import Neo4jBatchLoader
from icarus_etl.transforms import (
    deduplicate_rows,
    format_cnpj,
    strip_document,
)

logger = logging.getLogger(__name__)

# CAGED tipo_movimentacao: 1 = admission, 2 = dismissal
_MOVEMENT_TYPES: dict[str, str] = {
    "1": "admissao",
    "2": "desligamento",
    "3": "desligamento",  # some codes map to sub-types
}

# Chunk size for streaming CSV reads (100K rows per chunk)
_READ_CHUNK_SIZE = 100_000


def _generate_movement_id(cnpj_digits: str, cpf_digits: str, date: str, mtype: str) -> str:
    """Deterministic ID from CNPJ + CPF + date + movement type."""
    raw = f"{cnpj_digits}:{cpf_digits}:{date}:{mtype}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _build_movement_date(ano: str, mes: str) -> str:
    """Build YYYY-MM date string from year and month columns."""
    month = mes.zfill(2)
    return f"{ano}-{month}"


def _parse_salary(raw: str) -> float | None:
    """Parse salary value to float. Handles both dot-decimal and comma-decimal."""
    cleaned = raw.strip().replace("\u2212", "-")  # unicode minus
    if not cleaned or cleaned == "-":
        return None
    # Brazilian format: 1.500,50 -> dot as thousands, comma as decimal
    if "," in cleaned and "." in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    elif "," in cleaned:
        cleaned = cleaned.replace(",", ".")
    try:
        val = float(cleaned)
        return val if val >= 0 else None
    except ValueError:
        return None


class CagedPipeline(Pipeline):
    """ETL pipeline for CAGED labor movement data (admissions/dismissals).

    Uses stream-and-load pattern: reads CSVs in chunks, transforms and loads
    each chunk to Neo4j immediately, then discards. Never accumulates the
    full dataset in memory (~80M+ rows across all years).
    """

    name = "caged"
    source_id = "caged"

    def __init__(
        self,
        driver: Driver,
        data_dir: str = "./data",
        limit: int | None = None,
        chunk_size: int = 50_000,
    ) -> None:
        super().__init__(driver, data_dir, limit=limit, chunk_size=chunk_size)
        self._csv_files: list[Path] = []

    def extract(self) -> None:
        caged_dir = Path(self.data_dir) / "caged"
        self._csv_files = sorted(caged_dir.glob("caged_*.csv"))
        if not self._csv_files:
            logger.warning("No caged_*.csv files found in %s", caged_dir)

    def transform(self) -> None:
        pass  # Transform happens per chunk in load()

    def _transform_chunk(
        self, df: pd.DataFrame,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        """Transform a DataFrame chunk into (movements, company_rels, person_rels)."""
        movements: list[dict[str, Any]] = []
        company_rels: list[dict[str, Any]] = []
        person_rels: list[dict[str, Any]] = []

        for _idx, row in df.iterrows():
            cnpj_raw = str(row.get("cnpj_raiz", ""))
            cnpj_digits = strip_document(cnpj_raw)

            # cnpj_raiz is 8 digits (base CNPJ); pad to 14 for full CNPJ
            if len(cnpj_digits) == 8:
                cnpj_digits = cnpj_digits + "000100"  # main establishment
            if len(cnpj_digits) != 14:
                continue

            cnpj_formatted = format_cnpj(cnpj_digits)

            # CPF handling — CAGED microdados from BigQuery may not have CPF
            cpf_raw = str(row.get("cpf", ""))
            cpf_digits = strip_document(cpf_raw)

            ano = str(row.get("ano", "")).strip()
            mes = str(row.get("mes", "")).strip()
            if not ano or not mes:
                continue

            movement_date = _build_movement_date(ano, mes)

            tipo_raw = str(row.get("tipo_movimentacao", "")).strip()
            movement_type = _MOVEMENT_TYPES.get(tipo_raw, tipo_raw)

            movement_id = _generate_movement_id(
                cnpj_digits, cpf_digits, movement_date, movement_type,
            )

            salary = _parse_salary(str(row.get("salario_mensal", "")))

            movement: dict[str, Any] = {
                "movement_id": movement_id,
                "cnpj": cnpj_formatted,
                "movement_type": movement_type,
                "movement_date": movement_date,
                "cbo_code": str(row.get("cbo_2002", "")).strip(),
                "cnae_code": str(row.get("cnae_2_subclasse", "")).strip(),
                "municipality_code": str(row.get("id_municipio", "")).strip(),
                "uf": str(row.get("sigla_uf", "")).strip(),
                "source": "caged",
            }
            if salary is not None:
                movement["salary"] = salary

            movements.append(movement)

            company_rels.append({
                "source_key": cnpj_formatted,
                "target_key": movement_id,
            })

            if len(cpf_digits) == 11:
                person_rels.append({
                    "source_key": cpf_digits,
                    "target_key": movement_id,
                })

        movements = deduplicate_rows(movements, ["movement_id"])
        return movements, company_rels, person_rels

    def load(self) -> None:
        loader = Neo4jBatchLoader(self.driver)

        company_query = (
            "UNWIND $rows AS row "
            "MATCH (c:Company {cnpj: row.source_key}) "
            "MATCH (m:LaborMovement {movement_id: row.target_key}) "
            "MERGE (c)-[:MOVIMENTOU]->(m)"
        )
        person_query = (
            "UNWIND $rows AS row "
            "MATCH (p:Person {cpf: row.source_key}) "
            "MATCH (m:LaborMovement {movement_id: row.target_key}) "
            "MERGE (p)-[:EMPREGADO_EM]->(m)"
        )

        for csv_file in self._csv_files:
            logger.info("Processing %s ...", csv_file.name)
            reader = pd.read_csv(
                csv_file, dtype=str, keep_default_na=False,
                chunksize=_READ_CHUNK_SIZE, nrows=self.limit,
            )
            for chunk in reader:
                movements, company_rels, person_rels = self._transform_chunk(chunk)

                if movements:
                    loader.load_nodes("LaborMovement", movements, key_field="movement_id")

                    # Ensure Company nodes exist for CNPJ linking
                    companies = deduplicate_rows(
                        [{"cnpj": rel["source_key"]} for rel in company_rels],
                        ["cnpj"],
                    )
                    loader.load_nodes("Company", companies, key_field="cnpj")

                if company_rels:
                    loader.run_query_with_retry(company_query, company_rels)

                if person_rels:
                    loader.run_query_with_retry(person_query, person_rels)

            logger.info("Finished %s", csv_file.name)
