# Converted from Google Apps Script to Python

import re
from typing import Tuple

import kaiano_common_utils.config as config
from kaiano_common_utils import logger as log

log = log.get_logger()


def extract_date_and_title(file_name: str) -> Tuple[str, str]:
    match = re.match(r"^(\d{4}-\d{2}-\d{2})(.*)", file_name)
    if not match:
        return ("", file_name)
    date = match[1]
    title = match[2].lstrip("-_ ")
    return (date, title)


def extract_year_from_filename(filename):
    log.debug(f"extract_year_from_filename called with filename: {filename}")
    match = re.match(r"(\d{4})[-_]", filename)
    year = match.group(1) if match else None
    log.debug(f"Extracted year: {year} from filename: {filename}")
    return year


def normalize_csv(file_path):
    log.debug(f"normalize_csv called with file_path: {file_path} - reading file")
    with open(file_path, "r") as f:
        lines = f.readlines()
    cleaned_lines = [
        re.sub(r"\s+", " ", line).strip() for line in lines if line.strip()
    ]
    log.debug(f"Lines after cleaning: {len(cleaned_lines)}")
    with open(file_path, "w") as f:
        f.write("\n".join(cleaned_lines))
    log.info(f"✅ Normalized: {file_path}")


def normalize_prefixes_in_source(drive):
    """Remove leading status prefixes from files in the CSV source folder.
    If a file name starts with 'FAILED_' or 'possible_duplicate_' (case-insensitive),
    this function will attempt to rename it to the original base name (i.e. strip the prefix).
    Uses supportsAllDrives=True to operate on shared drives.
    """
    FAILED_PREFIX = "FAILED_"
    POSSIBLE_DUPLICATE_PREFIX = "possible_duplicate_"
    COPY_OF_PREFIX = "Copy of "
    try:
        log.debug("normalize_prefixes_in_source: listing source folder files")
        resp = (
            drive.files()
            .list(
                q=f"'{config.CSV_SOURCE_FOLDER_ID}' in parents and trashed = false",
                spaces="drive",
                fields="nextPageToken, files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        files = resp.get("files", [])
        log.info(f"normalize_prefixes_in_source: found {len(files)} files to inspect")

        for f in files:
            original_name = f.get("name", "")
            lower = original_name.lower()
            prefix = None
            if lower.startswith(FAILED_PREFIX.lower()):
                prefix = original_name[: len(FAILED_PREFIX)]
            elif lower.startswith(POSSIBLE_DUPLICATE_PREFIX.lower()):
                prefix = original_name[: len(POSSIBLE_DUPLICATE_PREFIX)]
            elif lower.startswith(COPY_OF_PREFIX.lower()):
                prefix = original_name[: len(COPY_OF_PREFIX)]

            if prefix:
                new_name = original_name[len(prefix) :]
                # If new_name is empty or already exists, skip
                if not new_name:
                    log.warning(
                        f"normalize_prefixes_in_source: derived empty new name for {original_name}, skipping"
                    )
                    continue

                # Check if a file with target name already exists in the same folder
                try:
                    query = f"name = '{new_name}' and '{config.CSV_SOURCE_FOLDER_ID}' in parents and trashed = false"
                    exists_resp = (
                        drive.files()
                        .list(
                            q=query,
                            fields="files(id, name)",
                            supportsAllDrives=True,
                            includeItemsFromAllDrives=True,
                        )
                        .execute()
                    )
                    if exists_resp.get("files"):
                        log.info(
                            f"normalize_prefixes_in_source: target name '{new_name}' already exists in source folder — leaving '{original_name}' as-is"
                        )
                        continue
                except Exception as e:
                    log.debug(
                        f"normalize_prefixes_in_source: error checking existing file for {new_name}: {e}"
                    )

                try:
                    log.info(
                        f"normalize_prefixes_in_source: renaming '{original_name}' -> '{new_name}'"
                    )
                    drive.files().update(
                        fileId=f["id"], body={"name": new_name}, supportsAllDrives=True
                    ).execute()
                except Exception as e:
                    log.error(
                        f"normalize_prefixes_in_source: failed to rename {original_name}: {e}"
                    )
    except Exception as e:
        log.error(f"normalize_prefixes_in_source: unexpected error: {e}")
