import random
import time

import kaiano_common_utils.config as config
import kaiano_common_utils.google_drive as google_drive
import kaiano_common_utils.google_sheets as google_sheets
import kaiano_common_utils.logger as log
import kaiano_common_utils.sheets_formatting as format
from googleapiclient.errors import HttpError

import deejay_set_processor.deduplice_summary as deduplication

log = log.get_logger()


def _safe_get_spreadsheet(sheet_service, spreadsheet_id, fields=None, max_retries=6):
    """Get spreadsheet metadata with exponential backoff on rate limits (429).

    Returns the decoded response dict or raises the last exception.
    """
    delay = 1.0
    for attempt in range(max_retries):
        try:
            req = sheet_service.spreadsheets().get(spreadsheetId=spreadsheet_id)
            if fields:
                req = sheet_service.spreadsheets().get(
                    spreadsheetId=spreadsheet_id, fields=fields
                )
            return req.execute()
        except HttpError as e:
            status = getattr(e.resp, "status", None)
            # Retry on rate limit errors
            if status == 429 or (status == 403 and "quota" in str(e).lower()):
                wait = delay + random.uniform(0, 0.5)
                log.warning(
                    f"Rate limited when fetching spreadsheet {spreadsheet_id}; retrying in {wait:.1f}s (attempt {attempt+1}/{max_retries})"
                )
                time.sleep(wait)
                delay *= 2
                continue
            raise
        except Exception:
            # Non-HTTP errors: re-raise
            raise
    # If we exhausted retries, make one final attempt to raise the underlying error
    return sheet_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()


def retry_with_backoff(task_fn, max_retries=6, base_delay=1.0, task_description="task"):
    for attempt in range(max_retries):
        try:
            return task_fn()
        except HttpError as e:
            status = getattr(e.resp, "status", None)
            if status == 429 or (status == 403 and "quota" in str(e).lower()):
                wait = base_delay * (2**attempt) + random.uniform(0, 0.5)
                log.warning(
                    f"⚠️ Rate limited on {task_description}, retrying in {wait:.1f}s (attempt {attempt+1}/{max_retries})"
                )
                time.sleep(wait)
                continue
            raise
        except Exception as e:
            log.error(f"❌ Unexpected error on {task_description} – {e}")
            if attempt >= 2:
                raise
            wait = base_delay * (2**attempt) + random.uniform(0, 0.5)
            time.sleep(wait)
    raise RuntimeError(f"Failed all retries for {task_description}")


def generate_next_missing_summary():
    """
    Generate the next missing summary for a year, if not locked.
    """
    log.info("🚀 Starting generate_next_missing_summary()")
    drive_service = google_drive.get_drive_service()
    sheet_service = google_sheets.get_sheets_service()

    summary_folder = google_drive.get_or_create_folder(
        config.DJ_SETS_FOLDER_ID, config.SUMMARY_FOLDER_NAME, drive_service
    )
    log.debug(f"Summary folder: {summary_folder}")

    year_folders = google_drive.get_files_in_folder(
        drive_service,
        config.DJ_SETS_FOLDER_ID,
        mime_type="application/vnd.google-apps.folder",
    )
    log.debug(f"Year folders found: {[f['name'] for f in year_folders]}")
    processed_any = False
    for folder in year_folders:
        year = folder["name"]
        if year.lower() == "summary":
            continue

        summary_name = f"{year} Summary"
        existing_summaries = google_drive.get_files_in_folder(
            drive_service, summary_folder, name_contains=summary_name
        )
        log.debug(f"Found existing summaries for {year}: {existing_summaries}")
        if existing_summaries:
            log.info(f"✅ Summary already exists for {year}")
            if processed_any:
                break
            continue

        log.debug(f"Getting files for year {year}")
        files = google_drive.get_files_in_folder(
            drive_service,
            folder["id"],
            mime_type="application/vnd.google-apps.spreadsheet",
        )
        if any(
            f["name"].startswith("FAILED_") or "_Cleaned" in f["name"] for f in files
        ):
            log.info(f"⛔ Skipping year {year} — unready files found")
            continue

        log.debug(f"Files to process for {year}: {[f['name'] for f in files]}")
        log.info(f"🔧 Generating summary for {year}...")
        generate_summary_for_folder(
            drive_service, sheet_service, files, summary_folder, summary_name, year
        )
        processed_any = True
        break


def generate_summary_for_folder(
    drive_service, sheet_service, files, summary_folder_id, summary_name, year
):
    log.debug(
        f"Starting generate_summary_for_folder for year {year} with {len(files)} files"
    )
    all_headers = set()
    sheet_data = []

    for f in files:
        log.info(f"🔍 Reading {f['name']}")
        try:
            sheets_metadata = retry_with_backoff(
                lambda: _safe_get_spreadsheet(
                    sheet_service, f["id"], fields="sheets(properties(title))"
                ),
                task_description=f"fetching spreadsheet metadata for {f['name']}",
            )

            sheets = sheets_metadata.get("sheets", [])
            if not sheets:
                log.warning(
                    f"⚠️ No sheets found in spreadsheet {f['name']} ({f['id']}); skipping"
                )
                continue

            for sheet in sheets:
                sheet_title = sheet.get("properties", {}).get("title")
                if not sheet_title:
                    log.debug(
                        f"Skipping sheet with missing title in spreadsheet {f['name']}"
                    )
                    continue
                values = retry_with_backoff(
                    lambda: google_sheets.get_sheet_values(
                        sheet_service, f["id"], sheet_title
                    ),
                    base_delay=2.0,
                    task_description=f"reading sheet '{sheet_title}' in {f['name']}",
                )

                time.sleep(2)

                if not values or len(values) < 2:
                    log.warning(f"⚠️ No data in {f['name']} - sheet '{sheet_title}'")
                    continue

                header = values[0]
                rows = values[1:]
                lower_header = [h.strip().lower() for h in header]
                keep_indices = [
                    i for i, h in enumerate(lower_header) if h in config.ALLOWED_HEADERS
                ]
                if not keep_indices:
                    continue
                filtered_header = [header[i] for i in keep_indices]
                filtered_rows = []
                for row in rows:
                    if not any((cell or "").strip() for cell in row):
                        continue
                    padded = row + [""] * (max(keep_indices) + 1 - len(row))
                    filtered_rows.append([padded[i] for i in keep_indices])
                log.debug(
                    f"Filtered header for sheet '{sheet_title}': {filtered_header}, rows: {len(filtered_rows)}"
                )
                if filtered_rows:
                    all_headers.update(filtered_header)
                    sheet_data.append((filtered_header, filtered_rows))
        except Exception as e:
            log.error(f"❌ Fatal error accessing {f['name']} – {e}")
            raise

    if not sheet_data:
        log.info(f"📭 No valid data found in folder: {year}")
        return

    ordered_header = [col for col in config.desiredOrder if col in all_headers]
    unordered_header = [col for col in all_headers if col not in config.desiredOrder]
    final_header = ordered_header + unordered_header + ["Count"]
    final_rows = []
    for header, rows in sheet_data:
        idx_map = {h: i for i, h in enumerate(header)}
        for row in rows:
            aligned = [
                row[idx_map[h]] if h in idx_map else "" for h in final_header[:-1]
            ]
            final_rows.append(aligned + [1])

    log.debug(
        f"Final header for year {year}: {final_header}, total rows: {len(final_rows)}"
    )

    if "Title" in final_header:
        title_index = final_header.index("Title")
        final_rows.sort(key=lambda r: r[title_index])
    else:
        final_rows.sort()

    ss_id = google_drive.create_spreadsheet(
        drive_service, name=summary_name, parent_folder_id=summary_folder_id
    )
    log.debug(f"Created spreadsheet ID for {summary_name}: {ss_id}")

    # Ensure a sheet named "Summary" exists
    spreadsheet_info = _safe_get_spreadsheet(
        sheet_service, ss_id, fields="sheets(properties(sheetId,title))"
    )
    sheets = spreadsheet_info.get("sheets", [])
    found_summary = False
    for sheet in sheets:
        if sheet.get("properties", {}).get("title") == "Summary":
            found_summary = True
            break
    if not found_summary and sheets:
        # Rename the first sheet to "Summary"
        first_sheet_id = sheets[0]["properties"]["sheetId"]
        google_sheets.rename_sheet(sheet_service, ss_id, first_sheet_id, "Summary")

    # Delete all sheets except "Summary"
    log.info(f"Deleting all sheets except 'Summary' in spreadsheet {ss_id}")
    google_sheets.delete_all_sheets_except(sheet_service, ss_id, "Summary")
    log.debug("All sheets except 'Summary' deleted.")

    # Write summary data to "Summary" sheet
    log.info(f"Writing summary data to 'Summary' sheet with {len(final_rows)} rows")
    google_sheets.write_sheet_data(
        sheet_service, ss_id, "Summary", final_header, final_rows
    )
    log.debug("Summary data written to 'Summary' sheet.")

    # Format the "Summary" sheet
    log.info("Formatting 'Summary' sheet")
    # Get sheet ID for "Summary"
    spreadsheet = _safe_get_spreadsheet(
        sheet_service, ss_id, fields="sheets(properties(sheetId,title))"
    )
    summary_sheet_id = None
    for sheet in spreadsheet.get("sheets", []):
        if sheet["properties"]["title"] == "Summary":
            summary_sheet_id = sheet["properties"]["sheetId"]
            break
    if summary_sheet_id is None:
        log.error('Sheet "Summary" not found in spreadsheet.')
        return
    # Set all cells (including header) to plain text format
    format.set_number_format(
        sheet_service,
        ss_id,
        summary_sheet_id,
        1,
        len(final_rows) + 1,
        1,
        len(final_header),
        "TEXT",
    )
    # Set header row bold
    format.set_bold_font(
        sheet_service, ss_id, summary_sheet_id, 1, 1, 1, len(final_header)
    )
    # Freeze header row
    format.freeze_rows(sheet_service, ss_id, summary_sheet_id, 1)
    # Set horizontal alignment to left for all data
    format.set_horizontal_alignment(
        sheet_service,
        ss_id,
        summary_sheet_id,
        1,
        len(final_rows) + 1,
        1,
        len(final_header),
        "LEFT",
    )
    # Auto resize columns and adjust width with max 200 pixels
    format.auto_resize_columns(
        sheet_service, ss_id, summary_sheet_id, 1, len(final_header)
    )
    log.info("Formatting of 'Summary' sheet complete.")

    # Create a duplicate of the generated spreadsheet before deduplication
    duplicated_name = "dedup_" + summary_name
    duplicated_ss_id = copy_file(
        drive_service, ss_id, duplicated_name, parent_folder_id=summary_folder_id
    )
    log.info(f"Original spreadsheet ID: {ss_id}")
    log.info(
        f"Duplicated spreadsheet ID with name '{duplicated_name}': {duplicated_ss_id}"
    )

    # Run deduplication on the duplicate spreadsheet
    deduplication.deduplicate_summary(duplicated_ss_id)


def copy_file(
    drive_service, source_file_id: str, new_name: str, parent_folder_id: str = None
) -> str:
    """
    Create a copy of a file in Google Drive with retries to handle propagation delay.
    """
    body = {"name": new_name}
    if parent_folder_id:
        body["parents"] = [parent_folder_id]

    delay = 1.0
    for attempt in range(5):
        try:
            log.info(
                f"📄 Copying file {source_file_id} → '{new_name}' (attempt {attempt+1})"
            )
            copied_file = (
                drive_service.files()
                .copy(
                    fileId=source_file_id,
                    body=body,
                    supportsAllDrives=True,
                )
                .execute()
            )
            new_file_id = copied_file.get("id")
            log.info(f"✅ File copied successfully: {new_file_id}")
            return new_file_id

        except HttpError as e:
            # File not found can happen immediately after creation due to propagation delay
            status = getattr(e.resp, "status", None)
            if status == 404 and "File not found" in str(e):
                wait = delay + random.uniform(0, 0.5)
                log.warning(
                    f"⚠️ File {source_file_id} not yet visible, retrying in {wait:.1f}s (attempt {attempt+1}/5)"
                )
                time.sleep(wait)
                delay *= 2
                continue
            raise

    raise RuntimeError(f"Failed to copy file {source_file_id} after multiple retries")


if __name__ == "__main__":
    generate_next_missing_summary()
