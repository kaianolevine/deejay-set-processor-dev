from __future__ import annotations

import csv
import io
import re
import tempfile
from pathlib import Path
from typing import Optional, Tuple

import kaiano_common_utils.google_drive as google_drive
import kaiano_common_utils.logger as log
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from mutagen import File as MutagenFile
from mutagen.easyid3 import EasyID3
from mutagen.id3 import ID3NoHeaderError
from mutagen.mp4 import MP4

AUDIO_EXTS = {".mp3", ".flac", ".m4a", ".mp4", ".ogg", ".opus", ".wav", ".aiff", ".aif"}

DRIVE_FOLDER_ID = "1hDFTDOavXDtJN-MR-ruqqapMaXGp4mB6"
SOURCE_MODE = "auto"  # title -> filename fallback
REPORT_FILENAME = "retag_report.csv"


def split_camel(s: str) -> str:
    """Turn CasesLikeThis / SamCooke / 2AM into spaced words."""
    s = (s or "").strip()
    if not s:
        return ""

    s = re.sub(r"([a-z])([A-Z])", r"\1 \2", s)
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", s)
    s = re.sub(r"([A-Za-z])(\d)", r"\1 \2", s)
    s = re.sub(r"(\d)([A-Za-z])", r"\1 \2", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def parse_compact_string(value: str) -> Optional[Tuple[str, str]]:
    """
    Parse your custom structure from a single string:
      <baseTitle> <extra...> <artist> <bpm>
    Rules:
      - last part is BPM (ignored)
      - part before BPM is Artist
      - first part is the base Title
      - all middle parts become (Extra) segments on the Title
    Requires at least 3 parts.
    """
    parts = [p for p in (value or "").split() if p.strip()]
    if len(parts) < 3:
        return None

    base_title = parts[0]
    artist = parts[-2]
    extras = parts[1:-2]

    new_title = split_camel(base_title)
    for x in extras:
        new_title += f" ({split_camel(x)})"

    new_artist = split_camel(artist)
    return new_title.strip(), new_artist.strip()


def read_source_string(path: Path, source: str) -> str:
    """
    source:
      - title: read from Title tag
      - filename: read from filename stem
      - auto: Title if present else filename
    """
    source = (source or "auto").strip().lower()
    if source not in {"title", "filename", "auto"}:
        raise ValueError(f"Invalid --source: {source}")

    if source == "filename":
        return path.stem

    title = ""

    # MP3: EasyID3 is the most consistent
    if path.suffix.lower() == ".mp3":
        try:
            tags = EasyID3(str(path))
            v = tags.get("title")
            if v:
                title = str(v[0])
        except Exception:
            title = ""

    if not title:
        audio = MutagenFile(str(path))
        if audio is not None and audio.tags is not None:
            tags = audio.tags

            # MP4/M4A title
            if isinstance(audio, MP4):
                v = tags.get("\xa9nam")
                if v and isinstance(v, list) and v:
                    title = str(v[0])

            # Vorbis/FLAC/Opus title
            if not title:
                for key in ("title", "TITLE"):
                    if key in tags:
                        v = tags.get(key)
                        if isinstance(v, list):
                            title = str(v[0]) if v else ""
                        else:
                            title = str(v)
                        break

    title = (title or "").strip()
    if source == "title":
        return title

    return title if title else path.stem


def write_tags(path: Path, new_title: str, new_artist: str) -> None:
    ext = path.suffix.lower()

    if ext == ".mp3":
        try:
            audio = EasyID3(str(path))
        except ID3NoHeaderError:
            audio = MutagenFile(str(path), easy=True)
            if audio is not None:
                audio.add_tags()
            audio = EasyID3(str(path))
        audio["title"] = [new_title]
        audio["artist"] = [new_artist]
        audio.save()
        return

    audio = MutagenFile(str(path))
    if audio is None:
        raise ValueError("Unrecognized/unsupported audio container")

    if isinstance(audio, MP4):
        audio.tags["\xa9nam"] = [new_title]
        audio.tags["\xa9ART"] = [new_artist]
        audio.save()
        return

    if audio.tags is None:
        audio.add_tags()

    audio.tags["TITLE"] = new_title
    audio.tags["ARTIST"] = new_artist
    audio.save()


def _download_drive_file_to_path(drive_service, file_id: str, dest_path: Path) -> None:
    request = drive_service.files().get_media(fileId=file_id, supportsAllDrives=True)
    fh = io.FileIO(str(dest_path), mode="wb")
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.close()


def _upload_path_to_drive_file(drive_service, file_id: str, src_path: Path) -> None:
    media = MediaFileUpload(str(src_path), resumable=True)
    # Preserve metadata; update only content
    drive_service.files().update(
        fileId=file_id, media_body=media, supportsAllDrives=True
    ).execute()


def main() -> int:
    folder_id = (DRIVE_FOLDER_ID or "").strip()
    if not folder_id or folder_id == "REPLACE_ME_WITH_FOLDER_ID":
        raise SystemExit("Set DRIVE_FOLDER_ID in retag_from_title.py before running.")

    log.info(f"🎵 Retag start: folder_id={folder_id}")
    log.info(f"Mode: SOURCE_MODE={SOURCE_MODE}, report={REPORT_FILENAME}")

    drive_service = google_drive.get_drive_service()
    files = google_drive.list_files_in_folder(drive_service, folder_id)
    log.info(f"Found {len(files)} total file(s) in folder.")

    # Filter to likely-audio files by extension (Drive mime types vary)
    audio_files = []
    for f in files:
        name = (f.get("name") or "").strip()
        file_id = f.get("id")
        if not name or not file_id:
            continue
        if Path(name).suffix.lower() in AUDIO_EXTS:
            audio_files.append(f)
    log.info(f"Found {len(audio_files)} audio candidate file(s) (by extension).")

    rows: list[list[str]] = []
    changed = 0
    skipped = 0

    for f in sorted(audio_files, key=lambda x: (x.get("name") or "").lower()):
        file_id = f.get("id")
        name = f.get("name") or "(unknown)"

        log.info(f"➡️ Processing: {name} ({file_id})")

        with tempfile.TemporaryDirectory() as tmpdir:
            local_path = Path(tmpdir) / name

            try:
                log.debug(f"⬇️ Downloading Drive file to temp path: {local_path}")
                _download_drive_file_to_path(drive_service, file_id, local_path)
                log.debug("✅ Download complete")

                source_str = read_source_string(local_path, SOURCE_MODE)
                log.debug(f"🔎 Source string ({SOURCE_MODE}): {source_str}")
                parsed = parse_compact_string(source_str)

                if not parsed:
                    skipped += 1
                    log.info(
                        "⏭️ Skip: string did not match expected pattern: <title> <extra...> <artist> <bpm>"
                    )
                    rows.append(
                        [
                            name,
                            file_id,
                            source_str,
                            "",
                            "",
                            "SKIP (doesn't match pattern)",
                        ]
                    )
                    continue

                new_title, new_artist = parsed
                log.info(f"✍️ Retag: Title='{new_title}' | Artist='{new_artist}'")

                # Always apply: write tags and upload back to Drive
                write_tags(local_path, new_title, new_artist)
                log.debug("⬆️ Uploading updated file back to Drive")
                log.debug("(supportsAllDrives=True)")
                _upload_path_to_drive_file(drive_service, file_id, local_path)
                log.debug("✅ Upload complete")

                rows.append([name, file_id, source_str, new_title, new_artist, "OK"])
                changed += 1

            except Exception as e:
                skipped += 1
                log.error(f"❌ ERROR processing {name} ({file_id}): {e}")
                rows.append([name, file_id, "", "", "", f"ERROR: {e}"])

    report_path = Path(REPORT_FILENAME).expanduser().resolve()
    with report_path.open("w", newline="", encoding="utf-8") as fp:
        w = csv.writer(fp)
        w.writerow(
            [
                "drive_name",
                "drive_file_id",
                "source_string",
                "new_title",
                "new_artist",
                "status",
            ]
        )
        w.writerows(rows)

    log.info(f"✅ Retag complete: updated {changed} file(s); skipped {skipped}.")
    log.info(f"Report written: {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
