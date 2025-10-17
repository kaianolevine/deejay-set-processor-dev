# Converted from Google Apps Script to Python

import re
import time
from difflib import SequenceMatcher
from typing import Tuple

import kaiano_common_utils._google_credentials as google_api
import kaiano_common_utils.config as config
import kaiano_common_utils.google_drive as drive
from googleapiclient.errors import HttpError
from kaiano_common_utils import logger as log

log = log.get_logger()

# Simulated in-memory locking mechanism (should be replaced with persistent store in prod)
_folder_locks = {}


def get_shared_filled_fields(data1, data2, indices):
    count = 0
    for idx in indices:
        v1 = data1[idx["index"]]
        v2 = data2[idx["index"]]
        if v1 and v2:
            count += 1
    return count


def get_dedup_match_score(data1, data2, indices):
    total_score = 0
    count = 0
    for idx in indices:
        v1 = str(data1[idx["index"]] or "")
        v2 = str(data2[idx["index"]] or "")
        if v1 and v2:
            total_score += string_similarity(v1, v2)
            count += 1
    if count == 0:
        return 0
    return total_score / count


def string_similarity(a, b):
    """
    Returns a similarity score between 0 and 1 for two strings.
    """
    return SequenceMatcher(None, a, b).ratio()


def clean_title(title):
    """
    Clean title string for comparison: lowercase and strip.
    """
    return title.lower().strip()


def hex_to_rgb(hex_color):
    hex_color = hex_color.lstrip("#")
    if len(hex_color) == 6 and all(c in "0123456789abcdefABCDEF" for c in hex_color):
        r, g, b = tuple(int(hex_color[i : i + 2], 16) for i in (0, 2, 4))
    elif len(hex_color) == 3 and all(c in "0123456789abcdefABCDEF" for c in hex_color):
        r, g, b = tuple(int(hex_color[i] * 2, 16) for i in range(3))
    else:
        r, g, b = (255, 255, 255)
    return {"red": r / 255, "green": g / 255, "blue": b / 255}


def try_lock_folder(folder_name):
    """
    Emulate folder locking by creating a lock file inside the folder.
    Returns True if lock acquired, False if already locked.
    """
    drive_service = google_api.get_drive_client()
    folder_id = config.DJ_SETS_FOLDER_ID
    summary_folder_id = drive.get_or_create_subfolder(
        drive_service, folder_id, folder_name
    )
    query = f"'{summary_folder_id}' in parents and name='{config.LOCK_FILE_NAME}' and trashed=false"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get("files", [])
    if files:
        log.info(f"🔒 {folder_name} folder is locked — skipping.")
        return False
    # Create lock file
    file_metadata = {
        "name": config.LOCK_FILE_NAME,
        "parents": [summary_folder_id],
        "mimeType": "application/octet-stream",
    }
    drive_service.files().create(body=file_metadata).execute()
    return True


def release_folder_lock(folder_name):
    """
    Remove the lock file to release the lock.
    """
    drive_service = google_api.get_drive_client()
    folder_id = config.DJ_SETS_FOLDER_ID
    summary_folder_id = drive.get_or_create_subfolder(
        drive_service, folder_id, folder_name
    )
    query = f"'{summary_folder_id}' in parents and name='{config.LOCK_FILE_NAME}' and trashed=false"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get("files", [])
    for f in files:
        try:
            drive_service.files().delete(fileId=f["id"]).execute()
        except HttpError as e:
            log.error(f"Error releasing lock: {e}")


def _try_lock_folder(folder_name):
    """Try to acquire a lock for a specific folder name."""
    now = time.time() * 1000
    expires_in = 7 * 60 * 1000  # 7 minutes
    key = f"LOCK_{folder_name}"
    existing = _folder_locks.get(key)
    if existing and now - existing < expires_in:
        return False
    _folder_locks[key] = now
    return True


def _release_folder_lock(folder_name):
    """Release the lock for a specific folder name."""
    key = f"LOCK_{folder_name}"
    _folder_locks.pop(key, None)


def _clean_title(value):
    """Remove parenthetical phrases from a title string (e.g., '(Remix)')."""
    return re.sub(r"\s*\([^)]*\)", "", str(value or "")).strip()


def levenshtein_distance(a, b):
    """Compute Levenshtein edit distance between two strings."""
    m, n = len(a), len(b)
    dp = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(m + 1):
        dp[i][0] = i
    for j in range(n + 1):
        dp[0][j] = j
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            cost = 0 if a[i - 1] == b[j - 1] else 1
            dp[i][j] = min(dp[i - 1][j] + 1, dp[i][j - 1] + 1, dp[i - 1][j - 1] + cost)
    return dp[m][n]


def _string_similarity(a, b):
    """Calculate normalized string similarity between two strings."""
    if not a or not b:
        return 0
    d = levenshtein_distance(a, b)
    return 1 - d / max(len(a), len(b))


def _get_shared_filled_fields(row_a, row_b, dedup_indices):
    """Count how many deduplication fields are filled in both rows."""
    return sum(
        1
        for dedup in dedup_indices
        if str(
            row_a[dedup["index"]] if row_a[dedup["index"]] is not None else ""
        ).strip()
        and str(
            row_b[dedup["index"]] if row_b[dedup["index"]] is not None else ""
        ).strip()
    )


def _get_dedup_match_score(row_a, row_b, dedup_indices):
    """Evaluate similarity score across deduplication fields."""
    total = 0
    matches = 0
    for dedup in dedup_indices:
        field = dedup["field"]
        index = dedup["index"]
        a = str(row_a[index] if row_a[index] is not None else "").strip().lower()
        b = str(row_b[index] if row_b[index] is not None else "").strip().lower()
        if not a or not b:
            matches += 1
            total += 1
        else:
            total += 1
            if string_similarity(a, b) >= 0.5:
                matches += 1
            elif field == "Title" and clean_title(a) == clean_title(b):
                matches += 1
    return matches / total if total > 0 else 0


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
