#!/usr/bin/env -S uv run --script

"""Glide: Automated Data Pipeline Zip Creator."""

# /// script
# requires-python = "~=3.12.11"
# dependencies = [
#     "numpy~=2.2.6",
#     "openpyxl~=3.1.5",
#     "pandas~=2.2.3",
#     "python-magic~=0.4.27",
#     "pyxlsb~=1.0.10",
#     "pyyaml~=6.0.2",
#     "sqlglot[rs]~=26.33.0",
#     "tika~=3.1.0",
#     "tqdm~=4.67.1",
# ]
# ///

import argparse
import csv
import logging
import re
import shutil
import subprocess
from collections.abc import Generator, Iterator
from pathlib import Path

import magic
import pandas as pd
import sqlglot
import yaml
from tika import parser
from tqdm import tqdm

# --- Configuration Section ---
# This part of the code handles configurations.

min_emails = 10
workdir = "z6yLr36C"
ignore_files = [".yaml", ".PNG", ".manifest"]
source_names = ["XSS", "LeakBase", "BreachForums", "DarkForums", "Cracked"]
delimiter_types = {
    "csv": ",",
    "semicolon": ";",
    "colon": ":",
    "pipe": "|",
    "tsv": "\t",
    "dash": "-",
}

# --- End Configuration Section ---

min_email_pattern = r"@"
email_pattern = r"[\w.$_%+-]+@[\w.-]+\.[\w]{2,6}"
sql_pattern = [
    "MySQL",
    "SQL dump",
    "CREATE TABLE",
    "INSERT INTO",
    "Host: localhost",
    "MariaDB",
]
json_pattern = r"{[\s\w\"\']+:"

parser.from_buffer("")
json_pattern = re.compile(json_pattern, re.IGNORECASE)
email_pattern = re.compile(email_pattern, re.IGNORECASE)
min_email_pattern = re.compile(min_email_pattern, re.IGNORECASE)
sql_pattern = re.compile("|".join(sql_pattern), re.IGNORECASE)

logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(message)s",
    datefmt="%Y%m%dT%H%M%S",
    encoding="utf-8",
    level=logging.DEBUG,
)


def cli() -> dict:
    """CLI parser for glide."""
    logger.debug("Parsing cli arguments.")
    parser = argparse.ArgumentParser(
        description="Glide: Automated Data Pipeline Zip Creator.",
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
        help='Directory to move parsable directories to. default: "parsable_dir"',
        default=Path("parsable_dir"),
    )
    parser.add_argument(
        "-j",
        "--rejected_dir",
        type=Path,
        help='Directory to move rejected directories to. default: "rejected_dir"',
        default=Path("rejected_dir"),
    )
    parser.add_argument(
        "-s",
        "--parse_sql",
        action="store_true",
        help="Option to parse sql.",
    )
    parser.add_argument(
        "-i",
        "--ignore",
        action="store_true",
        help="Ignore files with emails if its not parasable.",
    )
    args = vars(parser.parse_args())
    logger.debug("Parsed %r arguments.", args)

    logger.debug(
        "Checking and creating %r and %r directories",
        str(args["parsable_dir"]),
        str(args["rejected_dir"]),
    )
    args["parsable_dir"].mkdir(parents=True, exist_ok=True)
    args["rejected_dir"].mkdir(parents=True, exist_ok=True)
    return args


def cleanup(search_dir: Path) -> None:
    """Cleanup upload_dir before processing."""
    for dirpath, dirnames, _ in search_dir.walk():
        for dirn in dirnames:
            if str(dirn) in (workdir, ".glide"):
                shutil.rmtree(dirpath / dirn)
                logger.debug("Deleted %r", str(dirpath / dirn))


def get_filtered_files(search_dir: Path) -> Iterator[Path]:
    """Get filtered files."""
    logger.debug("Fetching files from %r", str(search_dir))
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
                logger.debug("File classified as SQL, file: %r", str(file))
                return "sql"
            if json_pattern.search(f_b):
                logger.debug("File classified as JSON, file: %r", str(file))
                return "json"
            logger.debug("File classified as CSV, file: %r", str(file))
            return "csv"
        case _:
            logger.debug("File classified as %r, file: %r", file_magic, str(file))
            return file_magic


def find_email(file: Path, mode: str | None = None) -> bool:
    """Check if Email is greater or lesser than given."""
    logger.debug("Fetch email count from file: %r", str(file))
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
    logger.debug(
        "Found %d emails in file: %r",
        email_count,
        str(file),
    )
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
                logger.debug(
                    "Identified %r delimiter for CSV file: %r",
                    dialect.delimiter,
                    str(file),
                )
                return dialect.delimiter
    logger.warning(
        "Unable to identify delimiter for CSV file: %r",
        str(file),
    )
    return None


def symlinker(file: Path, file_index: int, search_dir: Path, delimiter: str) -> None:
    """Create symlink to upload_dir."""
    delimiter_dir = Path(search_dir, workdir, "upload", delimiter)
    delimiter_dir.mkdir(parents=True, exist_ok=True)
    file_link = delimiter_dir / (
        str(file_index) + " - " + file.name + ("" if file.suffix == ".csv" else ".csv")
    )
    rel_sym = file.resolve().relative_to(file_link.parent.resolve(), walk_up=True)
    file_link.symlink_to(rel_sym)
    logger.debug(
        "Created soft link from %r to %r",
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
        logger.info(
            "Insufficient emails of %d, Ignoring file: %r",
            email_count,
            str(file),
        )
        return True
    found_delimiter_char = custom_delimiter if custom_delimiter else get_delimiter(file)
    logger.info(
        "Processing CSV with delimiter %r for file: %r",
        found_delimiter_char,
        str(file),
    )
    for delimiter_name, delimiter_char in delimiter_types.items():
        if found_delimiter_char == delimiter_char:
            symlinker(file, file_index, search_dir, delimiter_name)
            return True

    logger.critical(
        "Unable to process CSV with delimiter %r for file: %r",
        found_delimiter_char,
        str(file),
    )
    return False


def manifest_gen(input_manifest: Path, output_manifest: Path, search_dir: Path) -> None:
    """Generate automation manifest using given manifest."""
    logger.debug("Reading manifest file: %r", str(input_manifest))
    pattern = {x: re.compile(x, re.IGNORECASE) for x in source_names}
    dir_pattern = {x: re.compile(x, re.IGNORECASE) for x in delimiter_types}

    with Path.open(input_manifest, "r") as file:
        try:
            data = yaml.safe_load(file)
            logger.debug("Sucessfully read manifest file: %r", str(input_manifest))
        except yaml.scanner.ScannerError:
            logger.critical("Unable to read manifest file: %r", str(input_manifest))
            return False

    new_data = {}
    new_data["files"] = []
    for item in search_dir.iterdir():
        for key, pat in dir_pattern.items():
            hit = pat.search(str(item))
            if hit:
                new_data["files"].append(
                    {"path": str(item.name), "delimiter": delimiter_types[key]},
                )
                logger.debug(
                    "Found %r directory, adding it to manifest file: %r",
                    str(item.name),
                    str(input_manifest),
                )
                break

    for key, pat in pattern.items():
        hit = pat.search(data["Source"])
        if hit:
            new_data["source_names"] = [key]
            logger.debug(
                "Found %r source, adding it to manifest file: %r",
                new_data["source_names"],
                str(input_manifest),
            )
            break

    new_data["breach_victim"] = data["Title"]
    new_data["advertise_url"] = data["Source"]
    new_data["download_url"] = data["Download Link"]
    new_data["published_date"] = data["Source Date"]
    new_data["actors"] = [str(data["Actor"])]

    with Path.open(output_manifest, "w") as file:
        logger.info("Writing manifest file: %r", str(output_manifest))
        yaml.dump(new_data, file)

    return True


def zip_gen(search_dir: Path) -> None:
    """Generate Zip."""
    udir = Path(search_dir, workdir, "upload").absolute()
    zip_file = repr(search_dir.name + ".zip")
    cmd = f"cd {str(udir)!r} && rm {zip_file} ; zip -r {zip_file} ./*"
    result = subprocess.run(cmd, shell=True, check=True, text=True, capture_output=True)
    if result.returncode != 0:
        logger.critical("Error zipping: %s", result.stderr)
        return False
    logger.info("Created zip file: %r", search_dir.name + ".zip")
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
        logger.info("Converted : xslx %r to csv %r", str(file), str(outcsv))
        if not process_csv(outcsv, file_index, search_dir, custom_delimiter=","):
            logger.critical("Aborting : Unable to process csv file %r", str(file))
            return False
    return True


def sql_chunk(file: Path) -> Generator[str]:
    """Give chunks of sql file."""
    sql_command = ""

    with Path.open(file) as f:
        num_lines = sum(1 for _ in f)

    with Path.open(file) as f:
        for line in tqdm(f, total=num_lines, leave=False, mininterval=1):
            sql_command += line
            if line.strip().endswith(";"):
                yield sql_command
                sql_command = ""
        yield sql_command


def preprocess_sql(search_dir: Path) -> bool:
    """Convert sql to csv's.

    Only processes mysql and if a line starts with INSERT statement
    """
    for file_index, file in enumerate(get_filtered_files(search_dir)):
        if classify_file(file) == "sql":
            outdir = Path(search_dir.absolute(), workdir, "s2c", str(file_index))
            outdir.mkdir(parents=True, exist_ok=True)
            logger.info("Converting : sql %r to csv", str(file))
            for seg in sql_chunk(file):
                if "@" not in seg:
                    continue
                if "INSERT" not in seg:
                    continue
                try:
                    lp = sqlglot.parse(seg, read="mysql")
                    tbl_name = lp[0].this.this.name
                    col_name = [i.name for i in lp[0].this.expressions]
                    table_data = []
                    for a in lp:
                        if a.expression:
                            for b in a.expression.expressions:
                                table_data.append(
                                    [c.name for c in b.expressions],
                                )

                    if not col_name:
                        col_name = [
                            "unnamed_" + str(i) for i in range(len(table_data[0]))
                        ]

                    pd.DataFrame(table_data, columns=col_name).to_csv(
                        outdir / tbl_name,
                        index=False,
                        na_rep="",
                        mode="a",
                        header=not Path(f"out/{tbl_name}").exists(),
                    )
                except Exception as err:
                    logger.critical(
                        "Aborting : Convertion error of sql %r with error: %s",
                        str(file),
                        err,
                    )
                    return False
    return True


def glide(search_dir: Path, parsable_dir: Path, parse_sql: bool) -> None:
    """Automate the automation pipeline."""
    for file_index, file in enumerate(get_filtered_files(search_dir)):
        try:
            file_type = classify_file(file)
            match file_type:
                case "csv":
                    logger.info("Processing file as CSV, file: %r", str(file))
                    if not process_csv(file, file_index, search_dir):
                        if args["ignore"]:
                            logger.info("Ignoring file: %r", str(file))
                            continue
                        return
                case "application/vnd.ms-excel":
                    logger.info("Processing file as XLSX, file:  %r", str(file))
                    if not process_xlsx(file, file_index, search_dir):
                        logger.error(
                            "Aborting : Unable to process XLSX file %r",
                            str(file),
                        )
                        if args["ignore"]:
                            logger.info("Ignoring file: %r", str(file))
                            continue
                        return
                case "application/x-7z-compressed" | "application/zip":
                    logger.error(
                        "Aborting : Unable to process %r, file: %r",
                        file_type,
                        str(file),
                    )
                    if args["ignore"]:
                        logger.info("Ignoring file: %r", str(file))
                        continue
                    return
                case _:
                    if parse_sql and file_type == "sql":
                        logger.info(
                            "Ignoring SQL file, Since it has been converted to csv's",
                        )
                        continue
                    logger.info("Processing file as %r, file: %r", file_type, str(file))
                    email_count = find_email(
                        file,
                        mode=None if file_type in ["sql", "json"] else "tika",
                    )
                    if email_count >= min_emails:
                        logger.critical(
                            "Aborting : %s file has %d emails, file: %r",
                            file_type,
                            email_count,
                            str(file),
                        )
                        if args["ignore"]:
                            logger.info("Ignoring file: %r", str(file))
                            continue
                        return
                    logger.debug(
                        "Insufficient emails of %d, Ignoring %r file: %r",
                        email_count,
                        file_type,
                        str(file),
                    )
        except Exception:
            logger.exception("Error: ")

    if Path(search_dir, workdir, "upload").exists():
        if not manifest_gen(
            next(iter(search_dir.glob("./*.manifest"))),
            Path(search_dir, workdir, "upload", "manifest.yaml"),
            Path(search_dir, workdir, "upload"),
        ):
            return
        if not zip_gen(search_dir):
            return
        logger.info("Parsed sucessfully %r", str(search_dir))
        shutil.move(search_dir, parsable_dir)
        logger.info("Moved %r to %r", str(search_dir), str(parsable_dir))
    else:
        logger.warning("Nothing to Parse in %r", str(search_dir))
        shutil.move(search_dir, args["rejected_dir"])
        logger.info("Moved %r to %r", str(search_dir), str(args["rejected_dir"]))


if __name__ == "__main__":
    args = cli()
    cleanup(args["search_dir"])
    if args["parse_sql"]:
        preprocess_sql(args["search_dir"])
    glide(args["search_dir"], args["parsable_dir"], args["parse_sql"])
