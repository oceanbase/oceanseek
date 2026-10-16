#!/usr/bin/env python3
"""Generate deterministic TPC samples, not a reportable TPC benchmark.

Requires duckdb==1.4.4 with the official tpch and tpcds extensions.
The CSV exports measure byte-codec performance, not SQL/DTL row-encoding performance.
"""

import argparse
import hashlib
import json
from pathlib import Path

import duckdb


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--scale", type=float, default=0.1)
    parser.add_argument("--rows", type=int, default=65536)
    parser.add_argument("--extensions", type=Path, help="directory of signed, offline DuckDB extensions")
    args = parser.parse_args()
    if args.scale <= 0 or args.rows <= 0:
        parser.error("scale and rows must be positive")
    args.output.mkdir(parents=True, exist_ok=True)
    # Refuse to overwrite a previous corpus or its provenance.
    if any(args.output.glob("*.csv")) or (args.output / "manifest.json").exists():
        parser.error("output already contains a corpus")
    manifest = {"generator": "DuckDB official tpch/tpcds extensions",
                "duckdb_version": duckdb.__version__, "scale": args.scale,
                "max_rows_per_table": args.rows, "format": "pipe-delimited CSV, no header",
                "files": []}
    suites = (("tpch", "dbgen", ("lineitem", "orders", "customer")),
              ("tpcds", "dsdgen", ("store_sales", "item", "customer")))
    for suite, generator, tables in suites:
        with duckdb.connect() as conn:
            conn.execute("SET threads=1")
            conn.execute("SET memory_limit='2GB'")
            if args.extensions:
                extension = str((args.extensions / f"{suite}.duckdb_extension").resolve())
                conn.execute("LOAD '" + extension.replace("'", "''") + "'")
            else:
                conn.execute(f"INSTALL {suite}")
                conn.execute(f"LOAD {suite}")
            conn.execute(f"CALL {generator}(sf={args.scale})")
            for table in tables:
                filename = f"{suite}_{table}.csv"
                output = args.output / filename
                # Identifiers are fixed above. Parameterize the user-supplied output path.
                conn.execute(f"COPY (SELECT * FROM {table} ORDER BY ALL LIMIT {args.rows}) "
                             "TO ? (FORMAT CSV, DELIMITER '|', HEADER false)", [str(output)])
                content = output.read_bytes()
                count = min(args.rows, conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
                if not content or count == 0:
                    raise RuntimeError(f"empty sample: {filename}")
                manifest["files"].append({"file": filename, "rows": count, "bytes": len(content),
                                          "sha256": hashlib.sha256(content).hexdigest()})
                print(filename, count, len(content), flush=True)
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    main()
