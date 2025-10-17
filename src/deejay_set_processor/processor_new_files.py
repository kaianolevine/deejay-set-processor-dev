import os

import kaiano_common_utils.config as config
import kaiano_common_utils.google_drive as google_api
from kaiano_common_utils import logger as log

import deejay_set_processor.helpers as helpers

log = log.get_logger()


# --- Utility: remove summary file for a given year ---
def remove_summary_file_for_year(drive, year):
    try:
        summary_folder_id = google_api.get_or_create_folder(
            config.DJ_SETS_FOLDER_ID, "Summary", drive
        )
        summary_query = f"name = '{year} Summary' and '{summary_folder_id}' in parents and trashed = false"
        summary_resp = (
            drive.files()
            .list(
                q=summary_query,
                spaces="drive",
                fields="files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        summary_files = summary_resp.get("files", [])
        for summary_file in summary_files:
            drive.files().delete(
                fileId=summary_file["id"], supportsAllDrives=True
            ).execute()
            log.info(
                f"🗑️ Deleted existing summary file '{summary_file.get('name')}' for year {year}"
            )
    except Exception as e:
        log.error(f"Failed to remove summary file for year {year}: {e}")


# --- Utility: check for duplicate base filename in a folder ---
def file_exists_with_base_name(drive, folder_id, base_name):
    try:
        resp = (
            drive.files()
            .list(
                q=f"'{folder_id}' in parents and trashed = false",
                spaces="drive",
                fields="files(id, name)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        candidates = resp.get("files", [])
        for f in candidates:
            if os.path.splitext(f.get("name", ""))[0] == base_name:
                return True
    except Exception as e:
        log.error(f"Error checking for duplicates in folder {folder_id}: {e}")
    return False


def rename_file_as_duplicate(drive, file_id, filename):
    try:
        new_name = f"possible_duplicate_{filename}"
        drive.files().update(
            fileId=file_id, body={"name": new_name}, supportsAllDrives=True
        ).execute()
        log.info(f"✏️ Renamed original to '{new_name}'")
    except Exception as rename_exc:
        log.error(f"Failed to rename original to possible_duplicate_: {rename_exc}")


def process_non_csv_file(drive, file_metadata, year):
    filename = file_metadata["name"]
    file_id = file_metadata["id"]
    log.info(f"\n📄 Moving non-CSV file that starts with year: {filename}")
    try:
        year_folder_id = google_api.get_or_create_folder(
            config.DJ_SETS_FOLDER_ID, year, drive
        )
        base_name = os.path.splitext(filename)[0]
        if file_exists_with_base_name(drive, year_folder_id, base_name):
            rename_file_as_duplicate(drive, file_id, filename)
            global non_csv_count
            non_csv_count += 1
            return

        drive.files().update(
            fileId=file_id,
            addParents=year_folder_id,
            removeParents=config.CSV_SOURCE_FOLDER_ID,
            supportsAllDrives=True,
        ).execute()
        log.info(f"📦 Moved original file to {year} subfolder: {filename}")
        remove_summary_file_for_year(drive, year)
        non_csv_count += 1
    except Exception as e:
        log.error(f"Failed to move non-CSV file {filename}: {e}")


def process_csv_file(drive, file_metadata, year):
    filename = file_metadata["name"]
    file_id = file_metadata["id"]
    log.info(f"\n🚧 Processing: {filename}")
    temp_path = os.path.join("/tmp", filename)

    try:
        google_api.download_file(drive, file_id, temp_path)
        helpers.normalize_csv(temp_path)
        log.info(f"Downloaded and normalized file: {filename}")

        year_folder_id = google_api.get_or_create_folder(
            config.DJ_SETS_FOLDER_ID, year, drive
        )
        base_name = os.path.splitext(filename)[0]
        if file_exists_with_base_name(drive, year_folder_id, base_name):
            log.warning(
                f"⚠️ Destination already contains file with base name '{base_name}' in year folder {year_folder_id}. Marking original as possible duplicate and skipping."
            )
            try:
                new_name = f"possible_duplicate_{filename}"
                drive.files().update(
                    fileId=file_id, body={"name": new_name}, supportsAllDrives=True
                ).execute()
                log.info(f"✏️ Renamed original to '{new_name}'")
            except Exception as rename_exc:
                log.error(
                    f"Failed to rename original to possible_duplicate_: {rename_exc}"
                )
            return

        sheet_id = google_api.upload_to_drive(drive, temp_path, year_folder_id)
        log.debug(f"Uploaded sheet ID: {sheet_id}")
        google_api.apply_formatting_to_sheet(sheet_id)
        remove_summary_file_for_year(drive, year)

        try:
            archive_folder_id = google_api.get_or_create_folder(
                year_folder_id, "Archive", drive
            )
            drive.files().update(
                fileId=file_id,
                addParents=archive_folder_id,
                removeParents=config.CSV_SOURCE_FOLDER_ID,
                supportsAllDrives=True,
            ).execute()
            log.info(f"📦 Moved original file to Archive subfolder: {filename}")
        except Exception as move_exc:
            log.error(f"Failed to move original file to Archive subfolder: {move_exc}")

    except Exception as e:
        log.error(f"❌ Failed to upload or format {filename}: {e}")
        try:
            failed_name = f"FAILED_{filename}"
            drive.files().update(
                fileId=file_id, body={"name": failed_name}, supportsAllDrives=True
            ).execute()
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
    drive = google_api.get_drive_service()

    # Normalize any leftover status prefixes before processing
    helpers.normalize_prefixes_in_source(drive)

    files = google_api.list_files_in_folder(drive, config.CSV_SOURCE_FOLDER_ID)
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
            process_non_csv_file(drive, file_metadata, year)
            continue

        # At this point we only process CSVs
        csv_count += 1
        process_csv_file(drive, file_metadata, year)

    log.info(
        f"✅ Done: {csv_count} CSVs, {non_csv_count} non-CSV files, {skipped_count} skipped."
    )


if __name__ == "__main__":
    main()
