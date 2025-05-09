"""Script to create zip files auitable to upload to automation pipeline."""

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "charset-normalizer~=3.4.2",
#     "openpyxl~=3.1.5",
#     "pandas~=2.2.3",
#     "python-magic~=0.4.27",
#     "pyyaml~=6.0.2",
#     "tika~=3.1.0",
# ]
# ///
import argparse
import csv
import logging
import re
import shutil
import subprocess
from collections.abc import Iterator
from pathlib import Path

import magic
import pandas as pd
from charset_normalizer import from_bytes
from tika import parser

from manigen import manicov

parser.from_buffer("")
min_emails = 100
workdir = "z6yLr36C"
ignore_files = [".yaml", ".PNG", ".manifest"]
min_email_pattern = r"@"
email_pattern = r"[\w.$_%+-]+@[\w.-]+\.[\w]{2,6}"
delimiter_types = {
    "csv": ",",
    "semicolon": ";",
    "colon": ":",
    "pipe": "|",
    "tsv": "\t",
    "dash": "-",
}
sql_pattern = [
    "MySQL",
    "SQL dump",
    "CREATE TABLE",
    "INSERT INTO",
    "Host: localhost",
    "MariaDB",
]
json_pattern = r"{[\s\w\"\']+:"


json_pattern = re.compile(json_pattern, re.IGNORECASE)
email_pattern = re.compile(email_pattern, re.IGNORECASE)
min_email_pattern = re.compile(min_email_pattern, re.IGNORECASE)
sql_pattern = re.compile("|".join(sql_pattern), re.IGNORECASE)

logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(message)s",
    datefmt="%Y%m%dT%H%M%S",
    encoding="utf-8",
    level=logging.INFO,
)


def cli() -> dict:
    """CLI parser for glide."""
    parser = argparse.ArgumentParser(
        description="Script to automate the automation pipeline",
    )
    parser.add_argument(
        "search_dir",
        type=Path,
        help="Directory to Search",
    )
    parser.add_argument(
        "-p",
        "--parsable_dir",
        type=Path,
        help="Directory to move parsable directories to.",
        default=None,
    )
    parser.add_argument(
        "-j",
        "--rejected_dir",
        type=Path,
        help="Directory to move rejected directories to.",
        default=None,
    )
    parser.add_argument(
        "-s",
        "--parse_sql",
        action="store_true",
        help="Option to parse sql.",
    )
    args = parser.parse_args()
    return vars(args)


def get_encoding(file: Path) -> str:
    """Get encoding of the file."""
    with Path.open(file, "rb") as f:
        results = from_bytes(f.read(1024 * 10))
        return results.best().encoding


def cleanup(search_dir: Path) -> None:
    """Cleanup upload_dir before processing."""
    for dirpath, dirnames, _ in search_dir.walk():
        for dirn in dirnames:
            if str(dirn) == workdir:
                shutil.rmtree(dirpath / dirn)
                logging.info("Deleted %r", str(dirpath / dirn))


def get_filtered_files(search_dir: Path) -> Iterator[Path]:
    """Get filtered files."""
    logging.info("Fetching files from %r", str(search_dir))
    for dirpath, _, filenames in search_dir.walk():
        for file in filenames:
            if Path(file).suffix not in ignore_files:
                yield dirpath / file


def classify_file(file: Path) -> str:
    """Classify file based of thier magic number."""
    with Path.open(file, "rb") as f:
        file_magic = magic.from_buffer(f.read(1024 * 5), mime=True)
    match file_magic:
        case "text/plain" | "text/html":
            with Path.open(
                file,
                "r",
                encoding="ISO-8859-1",
            ) as f_a:
                f_b = f_a.read(1024 * 10)
            if sql_pattern.search(f_b):
                return "sql"
            if json_pattern.search(f_b):
                return "json"
            return "csv"
        case _:
            return file_magic


def find_email(file: Path, mode: str | None = None) -> bool:
    """Check if Email is greater or lesser than given."""
    logging.info("Processing : Fetching emails count for %r", str(file))
    email_count = 0
    match mode:
        case "tika":
            content = parser.from_file(str(file))
            if min_email_pattern.search(str(content)):
                for _ in email_pattern.finditer(str(content)):
                    email_count += 1
                    if email_count >= min_emails:
                        break
        case _:
            with Path.open(
                file,
                "r",
                encoding="ISO-8859-1",
            ) as file_open:
                for fline in file_open:
                    if min_email_pattern.search(fline):
                        email_count += len(email_pattern.findall(fline))
                        if email_count >= min_emails:
                            break
    return email_count


def get_delimiter(file: Path) -> str | None:
    """Get delimiter for the file."""
    sniffer = csv.Sniffer()
    with Path.open(file, "r", encoding="ISO-8859-1") as f:
        sample = ""
        for sample_len in [1024 * x for x in range(1, 11)[::-1]]:
            f.seek(0, 0)
            sample = f.read(sample_len)
            try:
                dialect = sniffer.sniff(
                    sample,
                    delimiters="".join(
                        delimiter_types.values(),
                    ),
                )
            except csv.Error:
                continue
            else:
                return dialect.delimiter
    return None


def upload_link(file: Path, file_index: int, search_dir: Path, delimiter: str) -> None:
    """Create s symlink to upload_dir."""
    delimiter_dir = Path(search_dir, workdir, "upload", delimiter)
    delimiter_dir.mkdir(parents=True, exist_ok=True)
    file_link = delimiter_dir / (str(file_index) + " - " + file.name)
    rel_sym = file.resolve().relative_to(file_link.parent.resolve(), walk_up=True)
    file_link.symlink_to(rel_sym)
    logging.info(
        "Linked: %r to %r",
        str(file_link),
        str(rel_sym),
    )


def process_csv(
    file: Path,
    file_index: int,
    search_dir: Path,
    custom_delimiter: str | None = None,
) -> None:
    """Process CSV."""
    email_count = find_email(file)
    if email_count < min_emails:
        logging.info("Ignoring : File %r has %d emails", str(file), email_count)
        return True
    found_delimiter_char = custom_delimiter if custom_delimiter else get_delimiter(file)
    logging.info("File delimiter is %r for %r", found_delimiter_char, str(file))
    for delimiter_name, delimiter_char in delimiter_types.items():
        if found_delimiter_char == delimiter_char:
            upload_link(file, file_index, search_dir, delimiter_name)
            return True
    return None


def gen_manifest_zip(search_dir: Path) -> None:
    """Generate Manifest and Zip."""
    if not manicov(
        next(iter(search_dir.glob("./*.manifest"))),
        Path(search_dir, workdir, "upload", "manifest.yaml"),
        Path(search_dir, workdir, "upload"),
    ):
        return False
    udir = Path(search_dir, workdir, "upload").absolute()
    zip_file = repr(search_dir.name + ".zip")
    cmd = f"cd {str(udir)!r} && rm {zip_file} ; zip -r {zip_file} ./*"
    result = subprocess.run(cmd, shell=True, check=True, text=True, capture_output=True)
    if result.returncode != 0:
        logging.error("zipping %s", result.stderr)
        return False
    logging.info("Created zip file %r", search_dir.name + ".zip")
    return True


def process_xlsx(file: Path, file_index: int, search_dir: Path) -> bool:
    """Process xlsx."""
    outdir = Path(search_dir, workdir, "xlsx2csv")
    outdir.mkdir(parents=True, exist_ok=True)
    sheets = pd.read_excel(file, sheet_name=None).keys()
    for sheet_index, sheet_name in enumerate(sheets):
        sheet = pd.read_excel(file, sheet_name=sheet_name)
        outcsv = outdir / f"{file_index!s} - {sheet_index!s} - {sheet_name!s}.csv"
        sheet.to_csv(outcsv, index=False, encoding="utf-8")
        logging.info("Converting : xslx %r to csv %r", str(file), str(outcsv))
        if not process_csv(outcsv, file_index, search_dir, custom_delimiter=","):
            logging.error("Aborting : Unable to process csv file %r", str(file))
            return False
    return True


def preprocess_sql(search_dir: Path) -> bool:
    """Convert sql to csv's."""
    for file_index, file in enumerate(get_filtered_files(search_dir)):
        outdir = Path(search_dir, ".glide", "sql2csv", str(file_index))
        outdir.mkdir(parents=True, exist_ok=True)
        if classify_file(file) == "sql":
            cmd = f"go_sql2csv -f {str(file)!r} -o {str(outdir)!r}"
            logging.info("Converting : sql %r to csv", str(file))
            result = subprocess.run(
                cmd,
                shell=True,
                check=True,
                text=True,
                capture_output=False,
            )
            if result.returncode != 0:
                logging.error(
                    "Aborting : Convertion error of sql %r with error: %s",
                    str(file),
                    result.stderr,
                )
                return False
            logging.info("Deleting : File %r", str(file))
            file.unlink()
    return True


def glide(search_dir: Path, parsable_dir: Path) -> None:
    """Automate the automation pipeline."""
    for file_index, file in enumerate(get_filtered_files(search_dir)):
        file_type = classify_file(file)
        match file_type:
            case "csv":
                logging.info("Processing : File as 'csv' %r", str(file))
                if not process_csv(file, file_index, search_dir):
                    logging.error("Aborting : Unable to process file %r", str(file))
                    return
            case "sql":
                logging.info("Processing : File as 'sql' %r", str(file))
                email_count = find_email(file)
                if email_count >= min_emails:
                    logging.error("Aborting : File type is 'sql' for %r", str(file))
                    return
            case "json":
                logging.info("Processing : File as 'json' %r", str(file))
                email_count = find_email(file)
                if email_count >= min_emails:
                    logging.error("Aborting : File type is 'json' for %r", str(file))
                    return
            case "application/vnd.ms-excel":
                logging.info("Processing : File as 'xlsx' %r", str(file))
                if not process_xlsx(file, file_index, search_dir):
                    logging.error(
                        "Aborting : Unable to process xlsx file %r",
                        str(file),
                    )
                    return
            case "application/x-7z-compressed":
                logging.error(
                    "Aborting : Unable to process 7z file %r",
                    str(file),
                )
                return
            case "application/zip":
                logging.error(
                    "Aborting : Unable to process zip file %r",
                    str(file),
                )
                return
            case _:
                email_count = find_email(file, mode="tika")
                if email_count >= min_emails:
                    logging.error(
                        "Aborting : File %r classfied as %r has %d emails",
                        str(file),
                        file_type,
                        email_count,
                    )
                    return

    if (
        Path(search_dir, workdir, "upload").exists()
        and gen_manifest_zip(search_dir)
        and parsable_dir
    ):
        logging.info("Parsed sucessfully %r", str(search_dir))
        shutil.move(search_dir, parsable_dir)
        logging.info("Moving %r to %r", str(search_dir), str(parsable_dir))
    else:
        if Path(search_dir, workdir, "upload").exists():
            logging.warning("Nothing to Parse in %r", str(search_dir))
        if args["rejected_dir"]:
            shutil.move(search_dir, args["rejected_dir"])
            logging.info("Moving %r to %r", str(search_dir), str(args["rejected_dir"]))


if __name__ == "__main__":
    args = cli()
    cleanup(args["search_dir"])
    if args["parse_sql"]:
        preprocess_sql(args["search_dir"])
    glide(args["search_dir"], args["parsable_dir"])
