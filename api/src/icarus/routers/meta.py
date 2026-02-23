import time
from typing import Annotated, Any

from fastapi import APIRouter, Depends
from neo4j import AsyncSession

from icarus.dependencies import get_session
from icarus.services.neo4j_service import execute_query_single

router = APIRouter(prefix="/api/v1/meta", tags=["meta"])

_stats_cache: dict[str, Any] | None = None
_stats_cache_time: float = 0.0


@router.get("/health")
async def neo4j_health(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> dict[str, str]:
    record = await execute_query_single(session, "health_check", {})
    if record and record["ok"] == 1:
        return {"neo4j": "connected"}
    return {"neo4j": "error"}


@router.get("/stats")
async def database_stats(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> dict[str, Any]:
    global _stats_cache, _stats_cache_time  # noqa: PLW0603

    if _stats_cache is not None and (time.monotonic() - _stats_cache_time) < 300:
        return _stats_cache

    record = await execute_query_single(session, "meta_stats", {})
    result = {
        "total_nodes": record["total_nodes"] if record else 0,
        "total_relationships": record["total_relationships"] if record else 0,
        "person_count": record["person_count"] if record else 0,
        "company_count": record["company_count"] if record else 0,
        "health_count": record["health_count"] if record else 0,
        "finance_count": record["finance_count"] if record else 0,
        "contract_count": record["contract_count"] if record else 0,
        "sanction_count": record["sanction_count"] if record else 0,
        "election_count": record["election_count"] if record else 0,
        "amendment_count": record["amendment_count"] if record else 0,
        "embargo_count": record["embargo_count"] if record else 0,
        "education_count": record["education_count"] if record else 0,
        "convenio_count": record["convenio_count"] if record else 0,
        "laborstats_count": record["laborstats_count"] if record else 0,
        "data_sources": 14,
    }

    _stats_cache = result
    _stats_cache_time = time.monotonic()
    return result


@router.get("/sources")
async def list_sources() -> dict[str, list[dict[str, str]]]:
    return {
        "sources": [
            {"id": "cnpj", "name": "Receita Federal (CNPJ)", "frequency": "monthly"},
            {"id": "tse", "name": "Tribunal Superior Eleitoral", "frequency": "biennial"},
            {"id": "transparencia", "name": "Portal da Transparência", "frequency": "monthly"},
            {"id": "ceis", "name": "CEIS/CNEP/CEPIM/CEAF", "frequency": "monthly"},
            {"id": "cnes", "name": "CNES/DATASUS", "frequency": "monthly"},
            {"id": "bndes", "name": "BNDES (Empréstimos)", "frequency": "monthly"},
            {"id": "pgfn", "name": "PGFN (Dívida Ativa)", "frequency": "monthly"},
            {"id": "ibama", "name": "IBAMA (Embargos)", "frequency": "monthly"},
            {"id": "comprasnet", "name": "ComprasNet/PNCP", "frequency": "monthly"},
            {"id": "tcu", "name": "TCU (Sanções)", "frequency": "monthly"},
            {"id": "transferegov", "name": "TransfereGov (Convênios)", "frequency": "monthly"},
            {"id": "rais", "name": "RAIS (Estatísticas Trabalhistas)", "frequency": "annual"},
            {"id": "inep", "name": "INEP (Censo Educação)", "frequency": "annual"},
            {"id": "dou", "name": "Diário Oficial da União", "frequency": "daily"},
        ]
    }
