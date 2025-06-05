"""Python script to generate manifest."""

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "pyyaml~=6.0.2",
# ]
# ///

import argparse
import logging
import re
from pathlib import Path

import yaml

logging.basicConfig(
    format="%(asctime)s : %(levelname)s : %(message)s",
    datefmt="%Y%m%dT%H%M%S",
    encoding="utf-8",
    level=logging.INFO,
)


def cli() -> dict:
    """CLI parser for manigen."""
    parser = argparse.ArgumentParser(
        description="Python script to generate manifest.",
    )
    parser.add_argument(
        "output_manifest",
        type=Path,
        help="Path to generate Manifest",
        default="manifest.yaml",
    )
    parser.add_argument(
        "-i",
        "--input_manifest",
        type=Path,
        help="Path to input Manifest",
    )
    parser.add_argument(
        "-s",
        "--search_dir",
        type=Path,
        help="Path to search dirextory",
        default="./",
    )
    args = parser.parse_args()
    return vars(args)


def manicov(input_manifest: Path, output_manifest: Path, search_dir: Path) -> None:
    """Generate manifest using given manifest."""
    logging.info("Reading %s", input_manifest)

    pattern = {
        x: re.compile(x, re.IGNORECASE)
        for x in ["XSS", "LeakBase", "BreachForums", "DarkForums", "Cracked"]
    }

    dir_delimiter = {
        "csv": ",",
        "semicolon": ";",
        "colon": ":",
        "pipe": "|",
        "tsv": "\t",
        "dash": "-",
    }
    dir_pattern = {x: re.compile(x, re.IGNORECASE) for x in dir_delimiter}

    with Path.open(input_manifest, "r") as file:
        try:
            data = yaml.safe_load(file)
            logging.info("Sucessfully read manifest %r", str(input_manifest))
        except yaml.scanner.ScannerError:
            logging.info("Unable to read manifest %r", str(input_manifest))
            return False

    new_data = {}

    new_data["files"] = []
    for item in search_dir.iterdir():
        logging.info("Processing : %r", str(item))
        if item.is_dir():
            for key, pat in dir_pattern.items():
                hit = pat.search(str(item))
                if hit:
                    new_data["files"].append(
                        {"path": str(item.name), "delimiter": dir_delimiter[key]},
                    )
                    break

    for key, pat in pattern.items():
        hit = pat.search(data["Source"])
        if hit:
            new_data["source_names"] = [key]
            break

    new_data["breach_victim"] = data["Title"]
    new_data["advertise_url"] = data["Source"]
    new_data["download_url"] = data["Download Link"]
    new_data["published_date"] = data["Source Date"]
    new_data["actors"] = [str(data["Actor"])]

    with Path.open(output_manifest, "w") as file:
        logging.info("Writing to %r", str(output_manifest))
        yaml.dump(new_data, file)

    return True


if __name__ == "__main__":
    args = cli()
    manicov(args["input_manifest"], args["output_manifest"], args["search_dir"])
