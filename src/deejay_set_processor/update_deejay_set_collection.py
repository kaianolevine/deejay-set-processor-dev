from typing import List

import kaiano_common_utils.config as config
from kaiano_common_utils import logger as log
from kaiano_common_utils.google import GoogleAPI
from kaiano_common_utils.google import sheets_formatting as format

import deejay_set_processor.helpers as helpers

log = log.get_logger()


def generate_dj_set_collection():
    log.info("🚀 Starting generate_dj_set_collection")
    g = GoogleAPI.from_env()

    # Locate DJ_SETS folder (we assume the constant ID points to the shared drive folder or folder in shared drive)
    parent_folder_id = config.DJ_SETS_FOLDER_ID
    log.info(f"📁 Using DJ_SETS folder: {parent_folder_id}")

    # Check for existing file or create new (create directly in the shared drive parent)
    spreadsheet_id = g.drive.find_or_create_spreadsheet(
        parent_folder_id=parent_folder_id,
        name=config.OUTPUT_NAME,
    )
    log.info(f"📄 Spreadsheet ID: {spreadsheet_id}")

    # Ensure there's exactly one temp sheet to start from
    g.sheets.clear_all_except_one_sheet(spreadsheet_id, config.TEMP_TAB_NAME)

    # Enumerate subfolders in DJ_SETS
    subfolders = g.drive.get_all_subfolders(parent_folder_id)
    log.debug(f"Retrieved {len(subfolders)} subfolders")
    subfolders.sort(key=lambda f: f.name, reverse=True)

    tabs_to_add: List[str] = []

    for folder in subfolders:
        name = folder.name
        folder_id = folder.id
        log.info(f"📁 Processing folder: {name} (id: {folder_id})")

        if name.lower() == "archive":
            log.info(f"⏭️ Skipping folder: {name} (archive folder)")
            continue

        files = g.drive.get_files_in_folder(folder_id, include_folders=True)
        log.debug(f"Found {len(files)} files in folder '{name}'")
        rows = []

        for f in files:
            file_name = f.name or ""
            mime_type = f.mime_type or ""
            file_url = f"https://docs.google.com/spreadsheets/d/{f.id}"
            log.debug(
                f"Processing file: Name='{file_name}', MIME='{mime_type}', URL='{file_url}'"
            )

            if name.lower() == "summary":
                rows.append([f'=HYPERLINK("{file_url}", "{file_name}")'])
            else:
                date, title = helpers.extract_date_and_title(file_name)
                rows.append([date, title, f'=HYPERLINK("{file_url}", "{file_name}")'])

        if name.lower() == "summary":
            if rows:
                all_rows = sorted(rows, key=lambda r: r[0], reverse=True)
                log.debug(f"Adding Summary sheet with {len(all_rows)} rows")
                # add Summary sheet
                log.info("➕ Adding Summary sheet")
                log.info("Inserting rows into Summary sheet")
                g.sheets.insert_rows(
                    spreadsheet_id,
                    config.SUMMARY_TAB_NAME,
                    [["Link"]] + all_rows,
                )
                # Force plain text formatting for the first column to prevent Google Sheets from converting year numbers (e.g., 2025) into dates like 1905-07-17
                format.set_column_text_formatting(
                    g.sheets, spreadsheet_id, config.SUMMARY_TAB_NAME, [0]
                )
                log.info("Setting column formatting for Summary sheet")
        elif rows:
            rows.sort(key=lambda r: r[0], reverse=True)
            log.debug(f"Adding sheet for folder '{name}' with {len(rows)} rows")
            log.info(f"➕ Adding sheet for folder '{name}'")
            log.info(f"Inserting rows into sheet '{name}'")
            g.sheets.insert_rows(
                spreadsheet_id,
                name,
                [["Date", "Name", "Link"]] + rows,
            )
            tabs_to_add.append(name)

    # Clean up temp sheets if any
    log.info(f"Deleting temp sheets: {config.TEMP_TAB_NAME} and 'Sheet1' if they exist")
    g.sheets.delete_sheet_by_name(spreadsheet_id, config.TEMP_TAB_NAME)
    g.sheets.delete_sheet_by_name(spreadsheet_id, "Sheet1")

    log.info("Setting column formatting for spreadsheet")
    format.apply_formatting_to_sheet(spreadsheet_id)

    # Reorder sheets: tabs_to_add then Summary
    log.info(f"Reordering sheets with order: {tabs_to_add + [config.SUMMARY_TAB_NAME]}")
    metadata = g.sheets.get_metadata(spreadsheet_id)
    format.reorder_sheets(
        g.sheets,
        spreadsheet_id,
        tabs_to_add + [config.SUMMARY_TAB_NAME],
        metadata,
    )
    log.info("Completed reordering sheets")
    log.info("✅ Finished generate_dj_set_collection")


if __name__ == "__main__":
    generate_dj_set_collection()
