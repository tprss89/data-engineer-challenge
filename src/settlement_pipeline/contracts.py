"""Contratos das fontes: a interface que cada produtor se compromete a entregar.

O contrato descreve a fonte como ela chega (nomes e tipos físicos), não como queremos
consumir. Casting e regra de negócio ficam no silver.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import yaml

VALID_KINDS = {"cdc", "file"}
VALID_FORMATS = {"parquet", "csv"}


class ContractError(Exception):
    """Contrato mal definido. Erro de configuração, não de dado."""


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    required: bool = True


@dataclass(frozen=True)
class Contract:
    name: str
    kind: str
    format: str
    paths: list[str]
    columns: list[Column]
    description: str = ""
    owner: str = ""
    primary_key: list[str] = field(default_factory=list)
    reference_date_pattern: str | None = None

    @property
    def table(self) -> str:
        return f"bronze.{self.name}"

    @property
    def required_columns(self) -> list[str]:
        return [c.name for c in self.columns if c.required]

    def column(self, name: str) -> Column | None:
        return next((c for c in self.columns if c.name == name), None)

    def reference_date_from_filename(self, filename: str) -> str | None:
        if not self.reference_date_pattern:
            return None
        match = re.search(self.reference_date_pattern, filename)
        return match.group(1) if match else None


def load_contract(path: Path) -> Contract:
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    try:
        contract = Contract(
            name=raw["name"],
            kind=raw["kind"],
            format=raw["format"],
            paths=list(raw["paths"]),
            columns=[Column(**c) for c in raw["columns"]],
            description=(raw.get("description") or "").strip(),
            owner=raw.get("owner", ""),
            primary_key=list(raw.get("primary_key", [])),
            reference_date_pattern=raw.get("reference_date_pattern"),
        )
    except (KeyError, TypeError) as exc:
        raise ContractError(f"{path.name}: campo obrigatório ausente ou inválido ({exc})") from exc

    if contract.kind not in VALID_KINDS:
        raise ContractError(f"{path.name}: kind '{contract.kind}' inválido")
    if contract.format not in VALID_FORMATS:
        raise ContractError(f"{path.name}: format '{contract.format}' inválido")
    if contract.kind == "cdc" and not {"Op", "_timestamp"} <= {c.name for c in contract.columns}:
        raise ContractError(f"{path.name}: fonte CDC precisa declarar Op e _timestamp")
    return contract


def load_contracts(directory: Path) -> list[Contract]:
    contracts = [load_contract(p) for p in sorted(directory.glob("*.yml"))]
    names = [c.name for c in contracts]
    if len(names) != len(set(names)):
        raise ContractError(f"nomes de contrato duplicados em {directory}")
    return contracts
