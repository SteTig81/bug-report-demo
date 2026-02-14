
# Versioned Containment DAG System

## Modules

- db.py — schema + connection
- sample_data.py — demo dataset
- reports.py — hierarchical reporting engine
- cli.py — command line interface

## CLI Usage

Initialize DB:

    python cli.py init

Insert sample data:

    python cli.py sample

Generate report:

    python cli.py report --root APP_v2 --format both

## Logging

All debug and operational logs are written to:

    system.log

No debug output is printed to stdout.

## Architecture

Two orthogonal DAGs:

1. History DAG (version evolution)
2. Containment DAG (dependencies)

Tickets overlay element versions.
Bug activity computed via history reachability.
Reports compute bottom-up statistics.
