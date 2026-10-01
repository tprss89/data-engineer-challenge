#!/usr/bin/env python3
"""Abre o lake na DuckDB UI (navegador), em modo leitura.

Roda na máquina host, não no container: a UI escuta só em localhost.

    pip install "duckdb>=1.5.6,<1.6"
    python scripts/explore.py

Enquanto a UI estiver aberta o pipeline não consegue escrever no lake (o catálogo é um
arquivo DuckDB com um escritor por vez). Aperte Enter para fechar e liberar.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--lake-path", type=Path, default=REPO_ROOT / "data" / "lake")
    args = parser.parse_args()

    lake = args.lake_path.resolve().as_posix()
    if not Path(lake, "catalog.ducklake").exists():
        raise SystemExit(f"lake não encontrado em {lake}; rode o pipeline primeiro")

    con = duckdb.connect()
    con.execute("install ducklake; load ducklake; install ui; load ui")
    # O catálogo guarda o caminho de dentro do container; aqui apontamos para o do host.
    con.execute(
        f"attach 'ducklake:{lake}/catalog.ducklake' as lake "
        f"(data_path '{lake}/files/', read_only, override_data_path true)"
    )
    con.execute("use lake")
    url = con.execute("call start_ui()").fetchone()[0]
    print(f"{url}\nlake: {lake} (somente leitura)")
    try:
        input("Enter para fechar e liberar o lake... ")
    except (KeyboardInterrupt, EOFError):
        pass
    finally:
        con.execute("call stop_ui_server()")
        con.close()


if __name__ == "__main__":
    main()
