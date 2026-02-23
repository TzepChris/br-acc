from unittest.mock import AsyncMock, patch

import pytest
from httpx import AsyncClient


@pytest.mark.anyio
async def test_health_returns_ok(client: AsyncClient) -> None:
    response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.anyio
async def test_meta_sources(client: AsyncClient) -> None:
    response = await client.get("/api/v1/meta/sources")
    assert response.status_code == 200
    data = response.json()
    assert "sources" in data
    assert len(data["sources"]) == 14
    source_ids = [s["id"] for s in data["sources"]]
    assert "cnpj" in source_ids
    assert "tse" in source_ids
    assert "bndes" in source_ids
    assert "pgfn" in source_ids
    assert "ibama" in source_ids
    assert "comprasnet" in source_ids
    assert "tcu" in source_ids
    assert "transferegov" in source_ids
    assert "rais" in source_ids
    assert "inep" in source_ids
    assert "dou" in source_ids


@pytest.mark.anyio
async def test_meta_stats(client: AsyncClient) -> None:
    mock_record = {
        "total_nodes": 87_500_000,
        "total_relationships": 53_100_000,
        "person_count": 2_450_000,
        "company_count": 58_500_000,
        "health_count": 602_000,
        "finance_count": 24_000_000,
        "contract_count": 1_080_000,
        "sanction_count": 69_000,
        "election_count": 17_000,
        "amendment_count": 98_000,
        "embargo_count": 79_000,
        "education_count": 224_000,
        "convenio_count": 67_000,
        "laborstats_count": 29_500,
    }

    # Reset the stats cache between tests
    import icarus.routers.meta as meta_module
    meta_module._stats_cache = None
    meta_module._stats_cache_time = 0.0

    with patch(
        "icarus.routers.meta.execute_query_single",
        new_callable=AsyncMock,
        return_value=mock_record,
    ):
        response = await client.get("/api/v1/meta/stats")

    assert response.status_code == 200
    data = response.json()

    assert data["total_nodes"] == 87_500_000
    assert data["total_relationships"] == 53_100_000
    assert data["person_count"] == 2_450_000
    assert data["company_count"] == 58_500_000
    assert data["health_count"] == 602_000
    assert data["finance_count"] == 24_000_000
    assert data["contract_count"] == 1_080_000
    assert data["sanction_count"] == 69_000
    assert data["election_count"] == 17_000
    assert data["amendment_count"] == 98_000
    assert data["embargo_count"] == 79_000
    assert data["education_count"] == 224_000
    assert data["convenio_count"] == 67_000
    assert data["laborstats_count"] == 29_500
    assert data["data_sources"] == 14
