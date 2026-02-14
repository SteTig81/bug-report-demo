# Versioned Containment DAG System

See DESIGN.md for more details.

# Setup

    py -3.14t -m venv venv
    python cli.py init
    python cli.py sample

# Usage

    python cli.py report -c APP_v2 -p APP_v1 --format both

Find the reports in a sub-folder "reports".
