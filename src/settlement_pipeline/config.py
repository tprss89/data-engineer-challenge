"""Configuração lida do ambiente. Os defaults funcionam fora do container."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Settings:
    data_path: Path       # onde as fontes chegam (fixture ou saída do gerador)
    lake_path: Path       # catálogo e arquivos do DuckLake
    contracts_path: Path  # contratos YAML das fontes
    log_level: str

    @property
    def catalog_file(self) -> Path:
        return self.lake_path / "catalog.ducklake"

    @property
    def lake_data_path(self) -> Path:
        return self.lake_path / "files"


def load_settings(**overrides) -> Settings:
    values = {
        "data_path": Path(os.getenv("DATA_PATH", REPO_ROOT / "docs" / "sample-data")),
        "lake_path": Path(os.getenv("LAKE_PATH", REPO_ROOT / "data" / "lake")),
        "contracts_path": Path(os.getenv("CONTRACTS_PATH", REPO_ROOT / "contracts")),
        "log_level": os.getenv("LOG_LEVEL", "INFO"),
    }
    values.update({k: v for k, v in overrides.items() if v is not None})
    return Settings(**values)
