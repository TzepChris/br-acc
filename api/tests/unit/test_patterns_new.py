"""Tests for new Phase 14 pattern queries."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from icarus.services.pattern_service import PATTERN_QUERIES, run_pattern


def test_debtor_contracts_registered() -> None:
    assert "debtor_contracts" in PATTERN_QUERIES
    assert PATTERN_QUERIES["debtor_contracts"] == "pattern_debtor_contracts"


def test_embargoed_receiving_registered() -> None:
    assert "embargoed_receiving" in PATTERN_QUERIES
    assert PATTERN_QUERIES["embargoed_receiving"] == "pattern_embargoed_receiving"


def test_loan_debtor_registered() -> None:
    assert "loan_debtor" in PATTERN_QUERIES
    assert PATTERN_QUERIES["loan_debtor"] == "pattern_loan_debtor"


def test_pattern_count_is_eight() -> None:
    assert len(PATTERN_QUERIES) == 8


@pytest.mark.anyio
async def test_run_unknown_pattern_returns_empty() -> None:
    session = AsyncMock()
    result = await run_pattern(session, "nonexistent_pattern")
    assert result == []


@pytest.mark.anyio
async def test_debtor_contracts_returns_results() -> None:
    mock_record = MagicMock()
    mock_record.__iter__ = lambda self: iter([
        "company_name", "company_cnpj", "company_id",
        "total_debt", "total_contracts", "debt_count",
        "contract_count", "pattern_id",
    ])
    mock_record.__getitem__ = lambda self, key: {
        "company_name": "Test Corp",
        "company_cnpj": "12345678000100",
        "company_id": "4:abc:123",
        "total_debt": 100000.0,
        "total_contracts": 500000.0,
        "debt_count": 2,
        "contract_count": 3,
        "pattern_id": "debtor_contracts",
    }[key]

    session = AsyncMock()

    with patch("icarus.services.pattern_service.execute_query", new_callable=AsyncMock) as mock_eq:
        mock_eq.return_value = [mock_record]
        results = await run_pattern(session, "debtor_contracts")

    assert len(results) == 1
    assert results[0].pattern_id == "debtor_contracts"
    assert results[0].pattern_name == "Devedor com contratos públicos"


@pytest.mark.anyio
async def test_embargoed_receiving_returns_results() -> None:
    mock_record = MagicMock()
    mock_record.__iter__ = lambda self: iter([
        "company_name", "company_cnpj", "company_id",
        "embargo_description", "embargo_uf",
        "contract_count", "loan_count",
        "total_contract_value", "total_loan_value", "pattern_id",
    ])
    mock_record.__getitem__ = lambda self, key: {
        "company_name": "Embargo Corp",
        "company_cnpj": "98765432000100",
        "company_id": "4:abc:456",
        "embargo_description": "Desmatamento",
        "embargo_uf": "PA",
        "contract_count": 5,
        "loan_count": 1,
        "total_contract_value": 2000000.0,
        "total_loan_value": 500000.0,
        "pattern_id": "embargoed_receiving",
    }[key]

    session = AsyncMock()

    with patch("icarus.services.pattern_service.execute_query", new_callable=AsyncMock) as mock_eq:
        mock_eq.return_value = [mock_record]
        results = await run_pattern(session, "embargoed_receiving")

    assert len(results) == 1
    assert results[0].pattern_id == "embargoed_receiving"
    assert results[0].pattern_name == "Embargada recebendo recursos"


@pytest.mark.anyio
async def test_loan_debtor_returns_results() -> None:
    mock_record = MagicMock()
    mock_record.__iter__ = lambda self: iter([
        "company_name", "company_cnpj", "company_id",
        "total_loans", "total_debt", "loan_count",
        "debt_count", "pattern_id",
    ])
    mock_record.__getitem__ = lambda self, key: {
        "company_name": "Loan Debtor Corp",
        "company_cnpj": "11222333000144",
        "company_id": "4:abc:789",
        "total_loans": 3000000.0,
        "total_debt": 750000.0,
        "loan_count": 2,
        "debt_count": 4,
        "pattern_id": "loan_debtor",
    }[key]

    session = AsyncMock()

    with patch("icarus.services.pattern_service.execute_query", new_callable=AsyncMock) as mock_eq:
        mock_eq.return_value = [mock_record]
        results = await run_pattern(session, "loan_debtor")

    assert len(results) == 1
    assert results[0].pattern_id == "loan_debtor"
    assert results[0].pattern_name == "Tomador de empréstimo com dívida"
