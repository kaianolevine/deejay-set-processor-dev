import kaiano_common_utils.google_sheets as google_sheets
import kaiano_common_utils.logger as log
import kaiano_common_utils.sheets_formatting as format


def deduplicate_summary(spreadsheet_id: str):
    log.info(f"🚀 Starting deduplicate_summary for spreadsheet: {spreadsheet_id}")
    sheets_service = google_sheets.get_sheets_service()
    spreadsheet = (
        sheets_service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))")
        .execute()
    )
    sheets = spreadsheet.get("sheets", [])

    for sheet in sheets:
        sheet_props = sheet["properties"]
        sheet_id = sheet_props["sheetId"]
        sheet_name = sheet_props["title"]
        log.debug(f"Processing sheet '{sheet_name}' (ID: {sheet_id})")

        data = google_sheets.get_sheet_values(
            sheets_service, spreadsheet_id, sheet_name
        )
        if not data or len(data) < 2:
            log.warning(f"⚠️ Skipping empty or header-only sheet: {sheet_name}")
            continue

        header = data[0]
        rows = data[1:]

        # Ensure 'Count' column exists
        if "Count" not in header:
            header.append("Count")
            rows = [row + ["1"] for row in rows]
        count_index = header.index("Count")

        # Normalize row lengths
        for i, row in enumerate(rows):
            if len(row) < len(header):
                rows[i] = row + [""] * (len(header) - len(row))
            elif len(row) > len(header):
                rows[i] = row[: len(header)]

        deduped_rows = []
        total_count_sum = 0

        i = 0
        while i < len(rows):
            current_row = rows[i]
            try:
                current_count = int(current_row[count_index])
            except Exception:
                current_count = 0

            j = i + 1
            while j < len(rows):
                next_row = rows[j]
                if rows_equal_except_count(current_row, next_row, count_index):
                    try:
                        current_count += int(next_row[count_index])
                    except Exception:
                        pass
                    j += 1
                else:
                    break

            combined_row = current_row.copy()
            combined_row[count_index] = str(current_count)
            deduped_rows.append(combined_row)
            total_count_sum += current_count
            i = j

        log.debug(
            f"Sheet '{sheet_name}': original rows={len(rows)}, deduplicated rows={len(deduped_rows)}, total count={total_count_sum}"
        )

        final_data = [header] + deduped_rows
        google_sheets.clear_sheet(sheets_service, spreadsheet_id, sheet_name)
        format.update_sheet_values(
            sheets_service, spreadsheet_id, sheet_name, final_data
        )

    log.info(f"✅ Finished deduplicate_summary for spreadsheet: {spreadsheet_id}")


def rows_equal_except_count(row1, row2, count_index):
    return all(
        (i == count_index or (i < len(row1) and i < len(row2) and row1[i] == row2[i]))
        for i in range(len(row1))
    )
