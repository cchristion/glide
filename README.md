# Glide: Automated Data Pipeline Zip Creator

This Python script automates the process of preparing and packaging data directories into ZIP archives for ingestion into data pipelines. It intelligently processes various file types, extracts metadata, converts formats, and organizes the output, ensuring compliance and readiness for pipeline consumption.

Table of Contents
- Features
- Installation
- Usage
- File Processing Logic
- Output Structure
- Contributing
- License

## Features

- Automated Zip Creation: Streamlines the creation of .zip archives containing processed data.
- Pipeline Readiness: Generates output suitable for pushing to data pipelines.
- uv Integration: Utilizes uv for efficient virtual environment management and dependency handling (implicitly assumed for a Python project).
- Email Detection & Filtering:
    - Bare Email Pattern Check: Quickly identifies files containing potential email addresses using a bare email pattern for initial speed.
    - Full Email Validation: Follows up with a full email pattern check for accuracy.
    - Threshold-based Filtering: Processes files only if they contain at least 10 "bare emails" (or full emails after validation) to ensure relevant data.
    - Rejected Directories: Automatically moves directories without sufficient emails to a specified rejected directory (-j/--rejected_dir).
- Intelligent File Exclusion:
    - Ignores .yaml, .PNG, and .manifest files present in the root directory of the input.
- Source Name Extraction: Extracts the source name from the provided .manifest file for metadata.
- Dynamic Directory Creation:
    - Creates dedicated subdirectories within workdir/upload/ based on detected CSV delimiters: csv, semicolon, colon, pipe, tsv, and dash.
    - Files are symlinked to their respective delimiter directories.
- Comprehensive File Scanning: Recursively finds all files within the given input directory.
- Advanced File Classification:
    - Magic Number Classification: Identifies file types based on their magic numbers (e.g., text, zip/7z, other).
    - Text File Sub-Classification: For text files, further classifies them as csv, sql, or json.
    - CSV Delimiter Detection: Automatically detects the delimiter used in CSV files.
- Format Conversion:
    - XLSX to CSV: Converts .xlsx files to .csv.
    - SQL to CSV: Converts .sql files to .csv if the --sql argument is provided, linking them to workdir/upload/csv. This conversion utilizes a go tool.
- JSON File Identification: Informs if a file is identified as JSON.
- Metadata Generation: Creates metadata based on the provided YAML manifest.
- Output Management:
    - Clean Restart: Deletes the previously created workdir directory on every script restart to ensure a clean slate.
    - Parsable Directory: Moves the entire processed directory (including the final ZIP) to a user-specified parsable directory (-p/--parsable_dir) if all checks pass.
    - Unprocessable File Handling: The --ignore argument allows the script to proceed even if files are unprocessable, preventing immediate termination.

## Installation

- Clone the repository:
    ```bash
    git clone https://github.com/cchristion/glide
    cd glide
    ```

Note:
- uv will manage dependencies and virtual enviroment.
- Ensure uv and the gotool for SQL conversion are installed and accessible in your system's PATH.

## Usage

Note:
- Before running glide.py run, this will prep tika.
```bash
echo 'from tika import parser; parser.from_buffer(""); print("tika is runing")' | uv run --with tika -
```

```bash
uv run glide.py --help 
usage: glide.py [-h] [-p PARSABLE_DIR] [-j REJECTED_DIR] [-s] [-i] search_dir

positional arguments:
  search_dir            Directory to Search

options:
  -h, --help            show this help message and exit
  -p, --parsable_dir PARSABLE_DIR
                        Directory to move parsable directories to. default: "parsable_dir"
  -j, --rejected_dir REJECTED_DIR
                        Directory to move rejected directories to. default: "rejected_dir"
  -s, --parse_sql       Option to parse sql.
  -i, --ignore          Ignore files with emails if its not parasable.
```
- Arguments:
    - \<input_directory>: The path to the directory containing the files to be processed.
- Options:
    - -p, --parsable_dir \<directory>: Specifies the destination directory where the processed, zipped data directory will be moved if all checks pass.
    - -j, --rejected_dir \<directory>: Specifies the destination directory for input directories that do not contain enough email addresses or fail initial checks.
    - -i, --ignore: If present, the script will ignore unprocessable files and continue execution instead of terminating.
    - -s, --parse_sql: If present, enables the conversion of .sql files to .csv during processing using the gotool.

## File Processing Logic
- The script follows a robust processing flow:
    - Initialization: On startup, it removes the workdir directory from previous runs for a clean state.
    - Directory Traversal: Recursively scans the provided input directory to discover all files.
    - Initial Filtering: Ignores .yaml, .PNG, and .manifest files at the root level.
    - Email Check:
        - Performs a fast "bare email" pattern check on text-based files.
        - If at least 10 bare emails are found, a more thorough "full email" validation is performed.
        - If the email threshold is not met, the entire input directory is moved to the --rejected_dir.
    - File Classification: Each file is classified by its magic number:
        - text: Further classified as csv, sql, or json.
        - zip/7z
        - other
    - Format Conversion & Delimiter Detection:
        - xlsx files are converted to csv.
        - sql files are converted to csv (if --sql is enabled) using the gotool.
        - csv files are analyzed for their delimiters.
    - Output Structuring:
        - A workdir/upload directory is created.
        - Subdirectories (csv, semicolon, colon, pipe, tsv, dash) are created within workdir/upload.
        - Processed CSV files are symlinked into the appropriate delimiter-based subdirectory.
    - Metadata Generation: A metadata file is generated based on the source name extracted from the manifest YAML.
    - Zipping: All required files within the workdir are compressed into a single ZIP archive.
    - Final Move: The entire workdir (containing the final ZIP) is moved to the --parsable_dir.
    
## Output Structure
Upon successful processing, an input directory named input_dir (for example) would result in a structure similar to this within the --parsable_dir:
```
.
├── IMAGE.PNG
├── MANIFEST.manifest
├── DATA1.txt
├── DATA2.txt
├── metadata.yaml
└── z6yLr36C
    └── upload
        ├── colon
        │   └── 0 - DATA1.txt -> ../../../DATA1.txt
        ├── csv
        │   └── 1 - DATA2.txt -> ../../../DATA2.txt
        ├── ... (other delimiter dirs, if any)
        ├── DATA.zip (Zip file that can be pushed to data pipeline)
        └── manifest.yaml
```
## Contributing
Contributions are welcome! Please feel free to open issues or submit pull requests.

## License
This project is licensed under the MIT License - see the LICENSE file for details.