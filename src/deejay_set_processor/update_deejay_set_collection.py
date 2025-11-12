import re
from typing import List

import kaiano_common_utils.config as config
import kaiano_common_utils.google_drive as drive
import kaiano_common_utils.google_sheets as sheets
import kaiano_common_utils.sheets_formatting as format
from kaiano_common_utils import logger as log

import deejay_set_processor.helpers as helpers

log = log.get_logger()


def generate_dj_set_collection():
    log.info("🚀 Starting generate_dj_set_collection")
    drive_service = drive.get_drive_service()
    sheets_service = sheets.get_sheets_service()

    # Locate DJ_SETS folder (we assume the constant ID points to the shared drive folder or folder in shared drive)
    parent_folder_id = config.DJ_SETS_FOLDER_ID
    log.info(f"📁 Using DJ_SETS folder: {parent_folder_id}")

    # Check for existing file or create new (create directly in the shared drive parent)
    spreadsheet_id = drive.find_or_create_file_by_name(
        drive_service,
        config.OUTPUT_NAME,
        parent_folder_id,
        mime_type="application/vnd.google-apps.spreadsheet",
    )
    log.info(f"📄 Spreadsheet ID: {spreadsheet_id}")

    # Ensure there's exactly one temp sheet to start from
    sheets.clear_all_except_one_sheet(
        sheets_service, spreadsheet_id, config.TEMP_TAB_NAME
    )

    # Enumerate subfolders in DJ_SETS
    subfolders = drive.get_all_subfolders(drive_service, parent_folder_id)
    log.debug(f"Retrieved {len(subfolders)} subfolders")
    subfolders.sort(key=lambda f: f["name"], reverse=True)

    tabs_to_add: List[str] = []

    for folder in subfolders:
        name = folder["name"]
        folder_id = folder["id"]
        log.info(f"📁 Processing folder: {name} (id: {folder_id})")

        files = drive.get_files_in_folder(drive_service, folder_id)
        log.debug(f"Found {len(files)} files in folder '{name}'")
        rows = []

        for f in files:
            file_name = f.get("name", "")
            mime_type = f.get("mimeType", "")
            file_url = f"https://docs.google.com/spreadsheets/d/{f.get('id', '')}"
            log.debug(
                f"Processing file: Name='{file_name}', MIME='{mime_type}', URL='{file_url}'"
            )

            if file_name.lower() == "archive":
                log.info(f"⏭️ Skipping folder: {name} (archive folder)")
                continue
            # if mime_type != "application/vnd.google-apps.spreadsheet":
            #    continue

            if name.lower() == "summary":
                year_match = re.search(r"\b(20\d{2})\b", file_name)
                year = year_match.group(1) if year_match else ""
                rows.append([f"'{year}", f'=HYPERLINK("{file_url}", "{file_name}")'])
            else:
                date, title = helpers.extract_date_and_title(file_name)
                rows.append([date, title, f'=HYPERLINK("{file_url}", "{file_name}")'])

        if name.lower() == "summary":
            if rows:
                complete = [r for r in rows if not r[0]]
                others = sorted(
                    [r for r in rows if r[0]], key=lambda r: r[0], reverse=True
                )
                all_rows = complete + others
                log.debug(f"Adding Summary sheet with {len(all_rows)} rows")
                # add Summary sheet
                log.info("➕ Adding Summary sheet")
                sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={
                        "requests": [
                            {
                                "addSheet": {
                                    "properties": {"title": config.SUMMARY_TAB_NAME}
                                }
                            }
                        ]
                    },
                ).execute()
                log.info("Inserting rows into Summary sheet")
                sheets.insert_rows(
                    sheets_service,
                    spreadsheet_id,
                    config.SUMMARY_TAB_NAME,
                    [["Year", "Link"]] + all_rows,
                )
                # Force plain text formatting for the first column to prevent Google Sheets from converting year numbers (e.g., 2025) into dates like 1905-07-17
                set_column_text_formatting(
                    sheets_service, spreadsheet_id, config.SUMMARY_TAB_NAME, [0]
                )
                log.info("Setting column formatting for Summary sheet")
                format.set_column_formatting(
                    sheets_service, spreadsheet_id, config.SUMMARY_TAB_NAME, 2
                )
                format.apply_sheet_formatting(spreadsheet_id)
        elif rows:
            rows.sort(key=lambda r: r[0], reverse=True)
            log.debug(f"Adding sheet for folder '{name}' with {len(rows)} rows")
            log.info(f"➕ Adding sheet for folder '{name}'")
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": [{"addSheet": {"properties": {"title": name}}}]},
            ).execute()
            log.info(f"Inserting rows into sheet '{name}'")
            sheets.insert_rows(
                sheets_service, spreadsheet_id, name, [["Date", "Name", "Link"]] + rows
            )
            log.info(f"Setting column formatting for sheet '{name}'")
            format.set_column_formatting(sheets_service, spreadsheet_id, name, 3)
            format.apply_sheet_formatting(spreadsheet_id)
            tabs_to_add.append(name)

    # Clean up temp sheets if any
    log.info(f"Deleting temp sheets: {config.TEMP_TAB_NAME} and 'Sheet1' if they exist")
    sheets.delete_sheet_by_name(sheets_service, spreadsheet_id, config.TEMP_TAB_NAME)
    sheets.delete_sheet_by_name(sheets_service, spreadsheet_id, "Sheet1")

    # Reorder sheets: tabs_to_add then Summary
    log.info(f"Reordering sheets with order: {tabs_to_add + [config.SUMMARY_TAB_NAME]}")
    metadata = sheets.get_spreadsheet_metadata(sheets_service, spreadsheet_id)
    format.reorder_sheets(
        sheets_service,
        spreadsheet_id,
        tabs_to_add + [config.SUMMARY_TAB_NAME],
        metadata,
    )
    log.info("Completed reordering sheets")

    log.info("✅ Finished generate_dj_set_collection")


def set_column_text_formatting(
    sheets_service, spreadsheet_id: str, sheet_name: str, column_indexes
):
    """
    Force plain text formatting for the given zero-based column indexes on a sheet.

    This prevents Google Sheets from auto-parsing numeric-looking values (e.g., 2025)
    into dates (e.g., 1905-07-17).

    Args:
        sheets_service: Authorized Google Sheets API service.
        spreadsheet_id: ID of the spreadsheet.
        sheet_name: Title of the target sheet.
        column_indexes: Iterable of zero-based column indexes to format.
    """
    # Resolve the sheetId from the sheet name
    meta = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet = next(
        (
            s
            for s in meta.get("sheets", [])
            if s.get("properties", {}).get("title") == sheet_name
        ),
        None,
    )
    if not sheet:
        raise ValueError(
            f"Sheet named '{sheet_name}' not found in spreadsheet {spreadsheet_id}"
        )

    sheet_id = sheet["properties"]["sheetId"]

    # Apply TEXT number format (pattern "@") to the entire column, skipping the header row
    requests = []
    for col in column_indexes:
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,  # leave header row unmodified
                        "startColumnIndex": int(col),
                        "endColumnIndex": int(col) + 1,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "numberFormat": {
                                "type": "TEXT",
                                "pattern": "@",
                            }
                        }
                    },
                    "fields": "userEnteredFormat.numberFormat",
                }
            }
        )

    if requests:
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": requests}
        ).execute()


if __name__ == "__main__":
    generate_dj_set_collection()
