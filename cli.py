
import argparse
import logging
from pathlib import Path
from datetime import datetime
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
    report.add_argument("--current-root", "-c", dest="current_root", required=True,
                        help="Root version id for the current containment DAG")
    report.add_argument("--format", choices=["json", "html", "both"], default="both")
    report.add_argument("--predecessor-root", "-p", dest="predecessor_root",
                        help="Root version id for the predecessor containment DAG")

    args = parser.parse_args()

    if args.command == "init":
        initialize_database()
        logging.info("Database initialized")

    elif args.command == "sample":
        create_sample_data()
        logging.info("Sample data inserted")

    elif args.command == "report":
        logging.info("Generating report for current_root=%s predecessor_root=%s format=%s",
                     args.current_root, args.predecessor_root, args.format)
        tree = build_tree(args.current_root, predecessor_root=args.predecessor_root)

        reports_dir = Path("reports")
        reports_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y-%m-%d-%H%M")
        base = f"bug-report_{args.current_root}"
        if args.predecessor_root:
            base += f"_{args.predecessor_root}"
        base = base + f"_{timestamp}"

        if args.format in ("json", "both"):
            json_path = reports_dir / f"{base}.json"
            export_json(tree, str(json_path))
            logging.info("Wrote JSON report: %s", json_path)
        if args.format in ("html", "both"):
            html_path = reports_dir / f"{base}.html"
            export_html(tree, str(html_path), "Bug-Report")
            logging.info("Wrote HTML report: %s", html_path)

        logging.info("Report generated for current_root %s", args.current_root)

    else:
        parser.print_help()

if __name__ == "__main__":
    main()
