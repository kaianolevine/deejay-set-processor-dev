import os

import kaiano_common_utils.config as config
import kaiano_common_utils.helpers as helpers
from kaiano_common_utils import logger as log
from kaiano_common_utils.google import GoogleAPI
from kaiano_common_utils.google import sheets_formatting as formatting

log = log.get_logger()


def normalize_prefixes_in_source(drive) -> None:
    """Remove leading status prefixes from files in the CSV source folder.

    If a file name starts with 'FAILED_', 'possible_duplicate_', or 'Copy of ' (case-insensitive),
    this will attempt to rename it to the stripped base name, but only if that target name does
    not already exist in the source folder.

    This function expects the new Drive facade / DriveFacade interface.
    """

    FAILED_PREFIX = "FAILED_"
    POSSIBLE_DUPLICATE_PREFIX = "possible_duplicate_"
    COPY_OF_PREFIX = "Copy of "

    try:
        log.debug("normalize_prefixes_in_source: listing source folder files")
        files = drive.list_files(
            config.CSV_SOURCE_FOLDER_ID, include_folders=False, trashed=False
        )
        log.info(f"normalize_prefixes_in_source: found {len(files)} files to inspect")

        # Build a quick lookup of names already present in the folder
        existing_names = {f.name for f in files if f.name}

        for f in files:
            original_name = f.name or ""
            lower = original_name.lower()
            prefix = None

            if lower.startswith(FAILED_PREFIX.lower()):
                prefix = original_name[: len(FAILED_PREFIX)]
            elif lower.startswith(POSSIBLE_DUPLICATE_PREFIX.lower()):
                prefix = original_name[: len(POSSIBLE_DUPLICATE_PREFIX)]
            elif lower.startswith(COPY_OF_PREFIX.lower()):
                prefix = original_name[: len(COPY_OF_PREFIX)]

            if not prefix:
                continue

            new_name = original_name[len(prefix) :]
            if not new_name:
                log.warning(
                    f"normalize_prefixes_in_source: derived empty new name for {original_name}, skipping"
                )
                continue

            if new_name in existing_names:
                log.info(
                    f"normalize_prefixes_in_source: target name '{new_name}' already exists in source folder — leaving '{original_name}' as-is"
                )
                continue

            try:
                log.info(
                    f"normalize_prefixes_in_source: renaming '{original_name}' -> '{new_name}'"
                )
                drive.rename_file(f.id, new_name)
                # Keep our local set consistent for subsequent checks in this run
                existing_names.discard(original_name)
                existing_names.add(new_name)
            except Exception as e:
                log.error(
                    f"normalize_prefixes_in_source: failed to rename {original_name}: {e}"
                )

    except Exception as e:
        log.error(f"normalize_prefixes_in_source: unexpected error: {e}")


# --- Utility: remove summary file for a given year ---
def remove_summary_file_for_year(g: GoogleAPI, year: str) -> None:
    try:
        summary_folder_id = g.drive.ensure_folder(config.DJ_SETS_FOLDER_ID, "Summary")
        summary_name = f"{year} Summary"

        # List files in the Summary folder and delete any exact name matches
        files = g.drive.list_files(
            summary_folder_id, include_folders=False, trashed=False
        )
        for f in files:
            if (f.name or "") == summary_name:
                g.drive.delete_file(f.id)
                log.info(
                    f"🗑️ Deleted existing summary file '{summary_name}' for year {year}"
                )
    except Exception as e:
        log.error(f"Failed to remove summary file for year {year}: {e}")


# --- Utility: check for duplicate base filename in a folder ---
def file_exists_with_base_name(g: GoogleAPI, folder_id: str, base_name: str) -> bool:
    try:
        candidates = g.drive.list_files(folder_id, include_folders=False, trashed=False)
        for f in candidates:
            if os.path.splitext(f.name or "")[0] == base_name:
                return True
    except Exception as e:
        log.error(f"Error checking for duplicates in folder {folder_id}: {e}")
    return False


def rename_file_as_duplicate(g: GoogleAPI, file_id: str, filename: str) -> None:
    try:
        new_name = f"possible_duplicate_{filename}"
        g.drive.rename_file(file_id, new_name)
        log.info(f"✏️ Renamed original to '{new_name}'")
    except Exception as rename_exc:
        log.error(f"Failed to rename original to possible_duplicate_: {rename_exc}")


def process_non_csv_file(g: GoogleAPI, file_metadata: dict, year: str) -> None:
    global non_csv_count
    filename = file_metadata["name"]
    file_id = file_metadata["id"]
    log.info(f"\n📄 Moving non-CSV file that starts with year: {filename}")
    try:
        year_folder_id = g.drive.ensure_folder(config.DJ_SETS_FOLDER_ID, year)
        base_name = os.path.splitext(filename)[0]
        if file_exists_with_base_name(g, year_folder_id, base_name):
            rename_file_as_duplicate(g, file_id, filename)
            non_csv_count += 1
            return

        g.drive.move_file(
            file_id, new_parent_id=year_folder_id, remove_from_parents=True
        )
        log.info(f"📦 Moved original file to {year} subfolder: {filename}")
        remove_summary_file_for_year(g, year)
        non_csv_count += 1
    except Exception as e:
        log.error(f"Failed to move non-CSV file {filename}: {e}")


def process_csv_file(g: GoogleAPI, file_metadata: dict, year: str) -> None:
    filename = file_metadata["name"]
    file_id = file_metadata["id"]
    log.info(f"\n🚧 Processing: {filename}")
    temp_path = os.path.join("/tmp", filename)

    try:
        g.drive.download_file(file_id, temp_path)
        helpers.normalize_csv(temp_path)
        log.info(f"Downloaded and normalized file: {filename}")

        year_folder_id = g.drive.ensure_folder(config.DJ_SETS_FOLDER_ID, year)
        base_name = os.path.splitext(filename)[0]
        if file_exists_with_base_name(g, year_folder_id, base_name):
            log.warning(
                f"⚠️ Destination already contains file with base name '{base_name}' in year folder {year_folder_id}. Marking original as possible duplicate and skipping."
            )
            rename_file_as_duplicate(g, file_id, filename)
            return

        sheet_id = g.drive.upload_csv_as_google_sheet(
            temp_path, parent_id=year_folder_id
        )
        log.debug(f"Uploaded sheet ID: {sheet_id}")
        formatting.apply_formatting_to_sheet(sheet_id)
        remove_summary_file_for_year(g, year)

        try:
            archive_folder_id = g.drive.ensure_folder(year_folder_id, "Archive")
            g.drive.move_file(
                file_id, new_parent_id=archive_folder_id, remove_from_parents=True
            )
            log.info(f"📦 Moved original file to Archive subfolder: {filename}")
        except Exception as move_exc:
            log.error(f"Failed to move original file to Archive subfolder: {move_exc}")

    except Exception as e:
        log.error(f"❌ Failed to upload or format {filename}: {e}")
        try:
            failed_name = f"FAILED_{filename}"
            g.drive.rename_file(file_id, failed_name)
            log.info(f"✏️ Renamed original to '{failed_name}'")
        except Exception as rename_exc:
            log.error(f"Failed to rename original to FAILED_: {rename_exc}")
    finally:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception:
                pass


# === MAIN ===
def main():
    log.info("Starting main process")
    g = GoogleAPI.from_env()

    # Normalize any leftover status prefixes before processing
    normalize_prefixes_in_source(g.drive)

    files = g.drive.list_files(
        config.CSV_SOURCE_FOLDER_ID, include_folders=False, trashed=False
    )
    files = [{"id": f.id, "name": f.name} for f in files]
    log.info(f"Found {len(files)} files in source folder")

    global csv_count, non_csv_count, skipped_count
    csv_count = 0
    non_csv_count = 0
    skipped_count = 0

    for file_metadata in files:
        filename = file_metadata["name"]
        log.debug(f"Processing file: {filename}")

        year = helpers.extract_year_from_filename(filename)
        if not year:
            log.warning(f"⚠️ Skipping unrecognized filename format: {filename}")
            skipped_count += 1
            continue

        # If the file is not a CSV but starts with a year, move it straight to the year folder
        if not filename.lower().endswith(".csv"):
            process_non_csv_file(g, file_metadata, year)
            continue

        # At this point we only process CSVs
        csv_count += 1
        process_csv_file(g, file_metadata, year)

    log.info(
        f"✅ Done: {csv_count} CSVs, {non_csv_count} non-CSV files, {skipped_count} skipped."
    )


if __name__ == "__main__":
    main()
