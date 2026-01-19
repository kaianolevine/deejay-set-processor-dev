import re
from typing import List, Tuple

import kaiano.config as config
from kaiano import logger as logger_mod
from kaiano.google import GoogleAPI

log = logger_mod.get_logger()


def generate_dj_set_collection():
    log.info("🚀 Starting generate_dj_set_collection")
    g = GoogleAPI.from_env()
    fmt = g.sheets.formatter

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

        files = g.drive.get_files_in_folder(folder_id, include_folders=False)
        log.debug(f"Found {len(files)} files in folder '{name}'")
        rows = []

        for f in files:
            file_name = f.name or ""
            mime_type = f.mime_type or ""
            file_url = f"https://docs.google.com/spreadsheets/d/{f.id}"
            log.debug(
                f"Processing file: Name='{file_name}', MIME='{mime_type}', URL='{file_url}'"
            )

            if mime_type != "application/vnd.google-apps.spreadsheet":
                continue

            if name.lower() == "summary":
                rows.append([f'=HYPERLINK("{file_url}", "{file_name}")', file_name])
            else:
                date, title = _extract_date_and_title(file_name)
                date_cell = f"'{date}" if date else ""
                title_cell = f"'{title}" if title else ""
                rows.append(
                    [date_cell, title_cell, f'=HYPERLINK("{file_url}", "{file_name}")']
                )

        if name.lower() == "summary":
            if rows:
                all_rows = sorted(rows, key=lambda r: r[1], reverse=True)
                log.debug(f"Adding Summary sheet with {len(all_rows)} rows")
                # add Summary sheet
                log.info("➕ Adding Summary sheet")
                log.info("Inserting rows into Summary sheet")
                g.sheets.insert_rows(
                    spreadsheet_id,
                    config.SUMMARY_TAB_NAME,
                    [["Link"]] + [[r[0]] for r in all_rows],
                    value_input_option="USER_ENTERED",
                )
        elif rows:
            rows.sort(key=lambda r: r[0], reverse=True)
            log.debug(f"Adding sheet for folder '{name}' with {len(rows)} rows")
            log.info(f"➕ Adding sheet for folder '{name}'")
            log.info(f"Inserting rows into sheet '{name}'")
            g.sheets.insert_rows(
                spreadsheet_id,
                name,
                [["Date", "Name", "Link"]] + rows,
                value_input_option="USER_ENTERED",
            )
            # Keep Date/Name as plain text; Link remains a formula.
            fmt.set_column_text_formatting(spreadsheet_id, name, [0, 1])
            tabs_to_add.append(name)

    # Clean up temp sheets if any
    log.info(f"Deleting temp sheets: {config.TEMP_TAB_NAME} and 'Sheet1' if they exist")
    g.sheets.delete_sheet_by_name(spreadsheet_id, config.TEMP_TAB_NAME)
    g.sheets.delete_sheet_by_name(spreadsheet_id, "Sheet1")

    log.info("Setting column formatting for spreadsheet")
    fmt.apply_formatting_to_sheet(spreadsheet_id)

    # Reorder sheets: tabs_to_add then Summary
    log.info(f"Reordering sheets with order: {tabs_to_add + [config.SUMMARY_TAB_NAME]}")
    metadata = g.sheets.get_metadata(spreadsheet_id)
    fmt.reorder_sheets(
        spreadsheet_id,
        tabs_to_add + [config.SUMMARY_TAB_NAME],
        metadata,
    )
    log.info("Completed reordering sheets")
    log.info("✅ Finished generate_dj_set_collection")


def _extract_date_and_title(file_name: str) -> Tuple[str, str]:
    match = re.match(r"^(\d{4}-\d{2}-\d{2})(.*)", file_name)
    if not match:
        return ("", file_name)
    date = match[1]
    title = match[2].lstrip("-_ ")
    return (date, title)


if __name__ == "__main__":
    generate_dj_set_collection()
