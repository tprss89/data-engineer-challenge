from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from settlement_pipeline import lake
from settlement_pipeline.config import REPO_ROOT, load_settings
from settlement_pipeline.contracts import load_contracts

SAMPLE_DATA = REPO_ROOT / "docs" / "sample-data"
FIXTURE_REFERENCE_DATE = "2025-03-16"  # o CSV do fixture só tem liquidações de 16/03


@pytest.fixture
def data_path(tmp_path: Path) -> Path:
    """Cópia do fixture, para os testes poderem alterar arquivos à vontade."""
    target = tmp_path / "sources"
    shutil.copytree(SAMPLE_DATA, target)
    return target


@pytest.fixture
def settings(tmp_path: Path, data_path: Path):
    return load_settings(data_path=data_path, lake_path=tmp_path / "lake")


@pytest.fixture
def contracts():
    return load_contracts(REPO_ROOT / "contracts")


@pytest.fixture
def con(settings):
    connection = lake.connect(settings)
    yield connection
    connection.close()
