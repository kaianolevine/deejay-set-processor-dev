import argparse
import random
import sys
import time
from typing import Any

import kaiano_common_utils.google_sheets as google_sheets
import kaiano_common_utils.logger as log
import kaiano_common_utils.sheets_formatting as format
from googleapiclient.errors import HttpError


def _get_retry_after_seconds(http_error: HttpError) -> float | None:
    """Best-effort parsing of Retry-After from a googleapiclient HttpError."""
    try:
        resp = getattr(http_error, "resp", None)
        if resp is None:
            return None
        # googleapiclient uses httplib2.Response-like objects that support dict-style access
        ra = None
        try:
            ra = resp.get("retry-after")
        except Exception:
            ra = getattr(resp, "get", lambda *_: None)("retry-after")
        if not ra:
            return None
        return float(ra)
    except Exception:
        return None


def _is_retryable_http_error(e: HttpError) -> bool:
    """Return True if the HTTP error should be retried."""
    try:
        status = getattr(getattr(e, "resp", None), "status", None)
        if status is None:
            # Fallback: sometimes present as status_code
            status = getattr(getattr(e, "resp", None), "status_code", None)
        # Retry common transient/rate limit statuses
        return status in (408, 429, 500, 502, 503, 504)
    except Exception:
        return False


def _with_retry(
    fn,
    *,
    desc: str,
    max_attempts: int = 8,
    base_delay_s: float = 1.5,
    max_delay_s: float = 90.0,
):
    """Execute `fn()` with exponential backoff + jitter on retryable failures.

    This is primarily to handle Google Sheets API rate limiting (HTTP 429) and other transient errors.
    """
    last_err: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except HttpError as e:
            last_err = e
            if not _is_retryable_http_error(e) or attempt == max_attempts:
                raise

            status = getattr(getattr(e, "resp", None), "status", "?")
            retry_after = _get_retry_after_seconds(e)

            # Exponential backoff with full jitter.
            backoff = min(max_delay_s, base_delay_s * (2 ** (attempt - 1)))
            # If server tells us when to retry, respect it.
            if retry_after is not None:
                backoff = max(backoff, retry_after)

            sleep_s = backoff + random.uniform(0, min(1.0, backoff / 3))
            log.warning(
                f"⚠️ Retryable API error during {desc} (HTTP {status}). "
                f"Attempt {attempt}/{max_attempts}; sleeping {sleep_s:.1f}s then retrying..."
            )
            time.sleep(sleep_s)
        except Exception as e:
            # Network-ish / transient failures that sometimes surface outside HttpError
            last_err = e
            if attempt == max_attempts:
                raise
            sleep_s = min(
                max_delay_s, base_delay_s * (2 ** (attempt - 1))
            ) + random.uniform(0, 1.0)
            log.warning(
                f"⚠️ Unexpected error during {desc}: {e}. "
                f"Attempt {attempt}/{max_attempts}; sleeping {sleep_s:.1f}s then retrying..."
            )
            time.sleep(sleep_s)

    # Should not be reachable, but keep mypy happy.
    if last_err is not None:
        raise last_err


def deduplicate_summary(spreadsheet_id: str):
    log.info(f"🚀 Starting deduplicate_summary for spreadsheet: {spreadsheet_id}")
    sheets_service = google_sheets.get_sheets_service()
    spreadsheet = _with_retry(
        lambda: sheets_service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, fields="sheets(properties(sheetId,title))")
        .execute(),
        desc=f"spreadsheets.get({spreadsheet_id})",
    )
    sheets = spreadsheet.get("sheets", [])

    for sheet in sheets:
        sheet_props = sheet["properties"]
        sheet_id = sheet_props["sheetId"]
        sheet_name = sheet_props["title"]
        log.debug(f"Processing sheet '{sheet_name}' (ID: {sheet_id})")

        data = _with_retry(
            lambda: google_sheets.get_sheet_values(
                sheets_service, spreadsheet_id, sheet_name
            ),
            desc=f"get_sheet_values({sheet_name})",
        )
        if not data or len(data) < 2:
            log.warning(f"⚠️ Skipping empty or header-only sheet: {sheet_name}")
            continue

        header = data[0]
        rows = data[1:]

        # Ensure 'Count' column exists (case-insensitive)
        count_index = _find_column_index_ci(header, "Count")
        if count_index is None:
            header.append("Count")
            rows = [row + ["1"] for row in rows]
            count_index = len(header) - 1

        title_index = _find_column_index_ci(header, "Title")

        # Normalize row lengths
        for i, row in enumerate(rows):
            if len(row) < len(header):
                row = row + [""] * (len(header) - len(row))
            elif len(row) > len(header):
                row = row[: len(header)]

            # Strip leading/trailing whitespace on ALL cells (this affects what we write back).
            row = [_strip_cell_value(v) for v in row]
            rows[i] = row

        # Build a non-adjacent dedup map keyed by row values (excluding Count).
        length_index = _find_column_index_ci(header, "Length")
        # Placeholder for future BPM normalization if desired (not applied for now).
        bpm_index = _find_column_index_ci(header, "BPM")

        comment_index = _find_column_index_ci(header, "Comment")
        genre_index = _find_column_index_ci(header, "Genre")
        year_index = _find_column_index_ci(header, "Year")

        optional_indices = [
            i
            for i in [comment_index, genre_index, year_index, length_index, bpm_index]
            if i is not None
        ]

        # Group rows by an identity key that excludes Count and the optional match columns.
        # Within each identity key, merge rows when optional columns are compatible:
        # - empty vs non-empty is allowed (and we fill template with the non-empty value)
        # - non-empty vs different non-empty is NOT allowed
        identity_to_entries: dict[tuple[str, ...], list[dict[str, Any]]] = {}

        def _norm_optional(col_i: int, cell: Any) -> str:
            s = _normalize_key_cell(cell)
            if not s:
                return ""
            if length_index is not None and col_i == length_index:
                return _normalize_length(s)
            if bpm_index is not None and col_i == bpm_index:
                return _normalize_bpm(s)
            return s

        for row in rows:
            try:
                row_count = int(_strip_cell_value(row[count_index]))
            except Exception:
                row_count = 0

            identity_parts: list[str] = []
            for col_i, cell in enumerate(row):
                if col_i == count_index:
                    continue
                if col_i in optional_indices:
                    continue
                norm = _normalize_key_cell(cell)
                if title_index is not None and col_i == title_index:
                    # For title key comparison only: remove all non-alphanumeric characters, including whitespace
                    norm = "".join(ch for ch in norm if ch.isalnum())
                identity_parts.append(norm.lower())

            identity_key = tuple(identity_parts)

            # Compute normalized optional values for compatibility checks
            opt_norm: dict[int, str] = {
                i: _norm_optional(i, row[i]) for i in optional_indices
            }

            entries = identity_to_entries.setdefault(identity_key, [])

            matched_entry = None
            for entry in entries:
                entry_opt: dict[int, str] = entry["opt_norm"]
                compatible = True
                for i in optional_indices:
                    a = entry_opt.get(i, "")
                    b = opt_norm.get(i, "")
                    if a and b and a != b:
                        compatible = False
                        break
                if compatible:
                    matched_entry = entry
                    break

            if matched_entry is None:
                # New distinct entry under this identity
                template_row = row.copy()
                # Ensure Count cell is string
                template_row[count_index] = str(row_count)
                entries.append(
                    {
                        "row": template_row,
                        "count": row_count,
                        "opt_norm": opt_norm,
                    }
                )
            else:
                # Merge into the matched entry
                matched_entry["count"] += row_count
                matched_entry["row"][count_index] = str(matched_entry["count"])

                # Fill missing optional values from incoming row (preserve original text)
                for i in optional_indices:
                    existing_norm = matched_entry["opt_norm"].get(i, "")
                    incoming_norm = opt_norm.get(i, "")
                    if not existing_norm and incoming_norm:
                        matched_entry["opt_norm"][i] = incoming_norm
                        if i < len(matched_entry["row"]) and i < len(row):
                            matched_entry["row"][i] = row[i]

        deduped_rows: list[list[str]] = []
        total_count_sum = 0
        for _, entries in identity_to_entries.items():
            for entry in entries:
                deduped_rows.append(entry["row"])
                total_count_sum += entry["count"]

        log.debug(
            f"Sheet '{sheet_name}': original rows={len(rows)}, deduplicated rows={len(deduped_rows)}, total count={total_count_sum}"
        )

        final_data = [header] + deduped_rows
        _with_retry(
            lambda: google_sheets.clear_sheet(
                sheets_service, spreadsheet_id, sheet_name
            ),
            desc=f"clear_sheet({sheet_name})",
        )
        body = {"values": final_data}
        _with_retry(
            lambda: sheets_service.spreadsheets()
            .values()
            .update(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1",
                valueInputOption="RAW",
                body=body,
            )
            .execute(),
            desc=f"values.update({sheet_name})",
        )

    log.info(f"✅ Starting apply_sheet_formatting for spreadsheet: {spreadsheet_id}")
    _apply_sheet_formatting_safe(spreadsheet_id)
    log.info(f"✅ Finished deduplicate_summary for spreadsheet: {spreadsheet_id}")


# Helper for best-effort formatting via gspread
def _apply_sheet_formatting_safe(spreadsheet_id: str) -> None:
    """Best-effort formatting wrapper.

    We keep deduplication fully functional even if formatting fails.
    Prefer `kaiano_common_utils.sheets_formatting.apply_formatting_to_sheet(spreadsheet_id)`,
    which formats all worksheets in the spreadsheet.
    """
    try:
        # kaiano_common_utils already knows how to open the spreadsheet and format all worksheets.
        if hasattr(format, "apply_formatting_to_sheet"):
            _with_retry(
                lambda: format.apply_formatting_to_sheet(spreadsheet_id),
                desc=f"apply_formatting_to_sheet({spreadsheet_id})",
                max_attempts=5,
            )
            return

        # Back-compat: older utils may only expose apply_sheet_formatting(sheet_worksheet)
        log.warning(
            "⚠️ Skipping formatting: kaiano_common_utils.sheets_formatting.apply_formatting_to_sheet is not available."
        )
    except Exception as e:
        log.warning(f"⚠️ Formatting failed (continuing without formatting): {e}")


def _find_column_index_ci(header: list[str], target: str) -> int | None:
    for i, h in enumerate(header):
        if _normalize_key_cell(h).lower() == _normalize_key_cell(target).lower():
            return i
    return None


def _normalize_key_cell(value: Any) -> str:
    """Normalize cell text for deduplication key comparisons.

    Accented characters are folded to their base letters (e.g., 'Beyoncé' == 'Beyonce').

    This is intentionally *more aggressive* than what we write back to the sheet.
    It strips invisible unicode format characters (e.g. zero-width space, BOM),
    normalizes unicode width/compat forms, and collapses whitespace.

    NOTE: We only apply this to the *key*, not to the stored template row.
    """
    if value is None:
        s = ""
    else:
        s = str(value)

    # Normalize non-breaking spaces and common whitespace to plain spaces
    s = s.replace("\u00A0", " ")  # NBSP
    s = s.replace("\t", " ").replace("\r", " ").replace("\n", " ")

    try:
        import unicodedata

        # First decompose characters so accents become combining marks
        # e.g. "é" -> "e" + "́"
        s = unicodedata.normalize("NFKD", s)

        # Remove combining marks (accents/diacritics)
        s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")

        # Re-compose to a stable form
        s = unicodedata.normalize("NFKC", s)

        # Remove invisible/format characters (category Cf), e.g. \u200b, \ufeff
        s = "".join(ch for ch in s if unicodedata.category(ch) != "Cf")
    except Exception:
        pass

    # Collapse runs of whitespace and trim
    s = " ".join(s.split())
    return s


def _strip_cell_value(value: Any) -> str:
    """Strip leading/trailing whitespace from a cell value.

    This mutates what we write back to Sheets (unlike `_normalize_key_cell`, which is key-only).
    We keep internal whitespace intact; we only remove leading/trailing whitespace and convert
    NBSP to a normal space first.
    """
    if value is None:
        return ""
    s = str(value)
    s = s.replace("\u00A0", " ")  # NBSP → space
    return s.strip()


def _normalize_length(value: str) -> str:
    """Normalize length values so equivalent time formats match (MM:SS and H:MM:SS).

    Supports both MM:SS and H:MM:SS time formats and ignores leading zero hours for key comparisons.
    Examples: '00:2:54' == '0:02:54' == '2:54'.
    """
    if value is None:
        return ""

    s = _normalize_key_cell(value)
    if not s:
        return ""

    parts = [p.strip() for p in s.split(":") if p.strip() != ""]

    # Accept common formats:
    # - MM:SS
    # - H:MM:SS (or 0:MM:SS)
    # We normalize to:
    # - M:SS when hours == 0
    # - H:MM:SS when hours > 0
    if len(parts) == 2:
        mm_raw, ss_raw = parts
        try:
            h = 0
            m = int(mm_raw) if mm_raw else 0
            sec = int(ss_raw) if ss_raw else 0
        except Exception:
            return s
    elif len(parts) == 3:
        hh_raw, mm_raw, ss_raw = parts
        try:
            h = int(hh_raw) if hh_raw else 0
            m = int(mm_raw) if mm_raw else 0
            sec = int(ss_raw) if ss_raw else 0
        except Exception:
            return s
    else:
        # Not a recognized time format; leave as-is.
        return s

    # Basic sanity checks
    if h < 0 or m < 0 or sec < 0 or sec >= 60 or m >= 60:
        return s

    if h == 0:
        return f"{m}:{sec:02d}"

    return f"{h}:{m:02d}:{sec:02d}"


def _normalize_bpm(value: Any) -> str:
    """Normalize BPM values for deduplication key comparisons.

    Treats numeric equivalents as equal (e.g., '100' == '100.0').
    Leaves non-numeric BPM text unchanged.

    NOTE: This is key-only; we do not mutate what gets written back.
    """
    s = _normalize_key_cell(value)
    if not s:
        return ""

    # Common case: int-like or float-like strings
    try:
        f = float(s)
    except Exception:
        return s

    # If it's effectively an integer, drop the .0
    if abs(f - round(f)) < 1e-9:
        return str(int(round(f)))

    # Otherwise keep a stable string form (trim trailing zeros)
    # e.g. 100.50 -> '100.5'
    out = ("%f" % f).rstrip("0").rstrip(".")
    return out


def rows_equal_except_count(row1, row2, count_index):
    return all(
        (i == count_index or (i < len(row1) and i < len(row2) and row1[i] == row2[i]))
        for i in range(len(row1))
    )


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Deduplicate one or more Google Sheets spreadsheets in-place by combining duplicate rows "
            "(summing the Count column) across all tabs."
        )
    )
    parser.add_argument(
        "spreadsheet_ids",
        nargs="+",
        help=(
            "One or more Google Sheets spreadsheet IDs to deduplicate (in-place). "
            "Example: 174AK9BTKpRhf4_uUSR5GtWarTSxEiVUdhvrn6Jk-OMA"
        ),
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args(sys.argv[1:])
    exit_code = 0
    for ss_id in args.spreadsheet_ids:
        try:
            deduplicate_summary(ss_id)
        except Exception as e:
            log.error(f"❌ Dedup failed for spreadsheet {ss_id}: {e}")
            exit_code = 1
    raise SystemExit(exit_code)
