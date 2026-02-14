
import argparse
import logging
from db import initialize_database
from sample_data import create_sample_data
from reports import build_tree, export_json, export_html

logging.basicConfig(
    filename="system.log",
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def main():
    parser = argparse.ArgumentParser(description="Versioned DAG Ticket System")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("init")
    sub.add_parser("sample")

    report = sub.add_parser("report")
    report.add_argument("--root", required=True)
    report.add_argument("--format", choices=["json", "html", "both"], default="both")

    args = parser.parse_args()

    if args.command == "init":
        initialize_database()
        logging.info("Database initialized")

    elif args.command == "sample":
        create_sample_data()
        logging.info("Sample data inserted")

    elif args.command == "report":
        tree = build_tree(args.root)
        if args.format in ("json", "both"):
            export_json(tree, "report.json")
        if args.format in ("html", "both"):
            export_html(tree, "report.html", "Annotated DAG")
        logging.info("Report generated for root %s", args.root)

    else:
        parser.print_help()

if __name__ == "__main__":
    main()
