import logging
import shutil
import sys
import threading
import json
from pathlib import Path
from datetime import datetime
from typing import Dict, Iterable, Optional, Callable, List, Set

logger = logging.getLogger("file_organizer")
UNDO_LOG_FILE = Path("undo.log")
CATEGORIES_FILE = Path("categories.json")

DEFAULT_CATEGORIES: Dict[str, Set[str]] = {
    "Images": {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".webp", ".heic"},
    "Videos": {".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm", ".m4v"},
    "Audio": {".mp3", ".wav", ".aac", ".ogg", ".flac", ".m4a", ".wma"},
    "Documents": {".pdf", ".docx", ".doc", ".txt", ".pptx", ".ppt", ".xlsx", ".xls", ".odt", ".csv", ".rtf"},
    "Archives": {".zip", ".rar", ".7z", ".tar", ".gz", ".bz2"},
    "Executables": {".exe", ".msi", ".apk", ".appimage"},
    "Others": set(),
}

def load_categories() -> Dict[str, Set[str]]:
    """Loads categories from categories.json, creates it if it doesn't exist."""
    if not CATEGORIES_FILE.exists():
        try:
            with open(CATEGORIES_FILE, "w", encoding="utf-8") as f:
                json.dump({k: list(v) for k, v in DEFAULT_CATEGORIES.items()}, f, indent=2)
            return DEFAULT_CATEGORIES
        except IOError as e:
            logger.error(f"Could not create default categories file: {e}")
            return DEFAULT_CATEGORIES
    try:
        with open(CATEGORIES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return {k: set(v) for k, v in data.items()}
    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"Failed to load categories file, falling back to defaults: {e}")
        return DEFAULT_CATEGORIES

def build_ext_index(categories: Dict[str, Set[str]]) -> Dict[str, str]:
    """Builds a mapping from file extension to category name."""
    idx: Dict[str, str] = {}
    for cat, exts in categories.items():
        for e in exts:
            if e:
                e_lower = e.lower()
                if e_lower not in idx:
                    idx[e_lower] = cat
    return idx

def unique_path(path: Path) -> Path:
    i = 1
    stem, suffix = path.stem, path.suffix
    parent = path.parent
    candidate = path
    while candidate.exists():
        candidate = parent / f"{stem} ({i}){suffix}"
        i += 1
    return candidate

def resolve_conflict(destination: Path, conflict_policy: str) -> Optional[Path]:
    if not destination.exists():
        return destination
    if conflict_policy == "skip":
        return None
    elif conflict_policy == "overwrite":
        try:
            if destination.is_file() or destination.is_symlink():
                destination.unlink(missing_ok=True)
        except OSError as e:
            logger.warning("Could not remove existing file for overwrite: %s (%s)", destination, e)
        return destination
    elif conflict_policy == "rename":
        return unique_path(destination)
    else:
        raise ValueError(f"Unknown conflict policy: {conflict_policy}")

def log_undo_operation(action: str, src: Path, dst: Path):
    """Logs a successful file transfer for potential rollback."""
    try:
        with open(UNDO_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"{action.upper()}|{src.resolve()}|{dst.resolve()}\n")
    except IOError as e:
        logger.error(f"Could not write to undo log: {e}")

def do_transfer(src: Path, dst: Path, action: str, dry_run: bool) -> bool:
    """
    Performs the file transfer. In dry-run, it creates the directory structure
    but does not move/copy the actual file.
    """
    try:
        dst.parent.mkdir(parents=True, exist_ok=True)
        if dry_run:
            logger.info("[DRY-RUN] Created structure for: %s -> %s", src, dst)
            return True
        if action == "move":
            shutil.move(str(src), str(dst))
        else:
            shutil.copy2(str(src), str(dst))
        logger.info("%s %s -> %s", action.upper(), src, dst)
        log_undo_operation(action, src, dst)
        return True
    except Exception as e:
        logger.error("Failed to %s %s: %s", action, src, e)
        return False

def organize_by_type(file: Path, dest_root: Path, **kwargs) -> Optional[bool]:
    ext_index = kwargs.get("ext_index", {})
    cat = ext_index.get(file.suffix.lower(), "Others")
    dest_file = dest_root / cat / file.name
    final_dest = resolve_conflict(dest_file, kwargs['conflict_policy'])
    if final_dest is None:
        return None
    return do_transfer(file, final_dest, kwargs['action'], kwargs['dry_run'])

def organize_by_name(file: Path, dest_root: Path, **kwargs) -> Optional[bool]:
    dest_file = dest_root / file.stem / file.name
    final_dest = resolve_conflict(dest_file, kwargs['conflict_policy'])
    if final_dest is None:
        return None
    return do_transfer(file, final_dest, kwargs['action'], kwargs['dry_run'])

def organize_by_date(file: Path, dest_root: Path, **kwargs) -> Optional[bool]:
    try:
        m_time = file.stat().st_mtime
        date = datetime.fromtimestamp(m_time)
        dest_dir = dest_root / str(date.year) / f"{date.month:02d}-{date.strftime('%B')}"
        dest_file = dest_dir / file.name
        final_dest = resolve_conflict(dest_file, kwargs['conflict_policy'])
        if final_dest is None:
            return None
        return do_transfer(file, final_dest, kwargs['action'], kwargs['dry_run'])
    except Exception as e:
        logger.error(f"Could not get date for {file.name}: {e}")
        return False

def organize_by_day(file: Path, dest_root: Path, **kwargs) -> Optional[bool]:
    """Organizes files into YYYY/MM/DD structure."""
    try:
        m_time = file.stat().st_mtime
        date = datetime.fromtimestamp(m_time)
        dest_dir = dest_root / str(date.year) / f"{date.month:02d}" / f"{date.day:02d}"
        dest_file = dest_dir / file.name
        final_dest = resolve_conflict(dest_file, kwargs['conflict_policy'])
        if final_dest is None:
            return None
        return do_transfer(file, final_dest, kwargs['action'], kwargs['dry_run'])
    except Exception as e:
        logger.error(f"Could not get date for {file.name}: {e}")
        return False

def organize_by_size(file: Path, dest_root: Path, **kwargs) -> Optional[bool]:
    try:
        size_mb = file.stat().st_size / (1024 * 1024)
        if size_mb < 1:
            cat = "Small (Under 1MB)"
        elif size_mb < 100:
            cat = "Medium (1-100MB)"
        else:
            cat = "Large (Over 100MB)"
        dest_file = dest_root / cat / file.name
        final_dest = resolve_conflict(dest_file, kwargs['conflict_policy'])
        if final_dest is None:
            return None
        return do_transfer(file, final_dest, kwargs['action'], kwargs['dry_run'])
    except Exception as e:
        logger.error(f"Could not get size for {file.name}: {e}")
        return False

def organize_by_first_letter(file: Path, dest_root: Path, **kwargs) -> Optional[bool]:
    first_letter = file.stem[0].upper()
    cat = first_letter if first_letter.isalpha() else "#"
    dest_file = dest_root / cat / file.name
    final_dest = resolve_conflict(dest_file, kwargs['conflict_policy'])
    if final_dest is None:
        return None
    return do_transfer(file, final_dest, kwargs['action'], kwargs['dry_run'])

ORGANIZERS = {
    "type": organize_by_type,
    "name": organize_by_name,
    "date": organize_by_date,
    "day": organize_by_day,
    "size": organize_by_size,
    "first_letter": organize_by_first_letter,
}

def list_files(source: Path, recursive: bool, exclude_dir: Optional[Path] = None) -> List[Path]:
    pattern = source.rglob("*") if recursive else source.glob("*")
    files: List[Path] = []
    ex = exclude_dir.resolve() if exclude_dir else None
    for p in pattern:
        try:
            if p.is_file():
                if ex and ex in p.resolve().parents:
                    continue
                files.append(p)
        except (OSError, PermissionError) as e:
            logger.warning("Could not access path %s: %s", p, e)
    return files

def clear_undo_log():
    if UNDO_LOG_FILE.exists():
        try:
            UNDO_LOG_FILE.unlink()
        except OSError as e:
            logger.error(f"Could not clear undo log: {e}")

def perform_undo(on_progress: Optional[Callable[[int, int], None]] = None):
    """
    Reads the undo log and reverts the operations.
    Supports an optional progress callback.
    """
    if not UNDO_LOG_FILE.exists():
        logger.info("No undo log found. Nothing to revert.")
        if on_progress:
            on_progress(0, 0)
        return {"total": 0, "succeeded": 0, "failed": 0}
    
    with open(UNDO_LOG_FILE, "r", encoding="utf-8") as f:
        lines = f.readlines()

    total = len(lines)
    succeeded = failed = 0
    
    for idx, line in enumerate(reversed(lines), start=1):
        try:
            action, original_src, final_dst = line.strip().split('|')
            original_src = Path(original_src)
            final_dst = Path(final_dst)

            if final_dst.exists():
                logger.info(f"UNDO: Moving {final_dst} back to {original_src.parent}")
                shutil.move(str(final_dst), str(original_src))
                succeeded += 1
            else:
                logger.warning(f"UNDO SKIP: Source file {final_dst} not found.")
        except Exception as e:
            logger.error(f"UNDO FAILED for line: {line.strip()} - {e}")
            failed += 1
        
        if on_progress:
            on_progress(idx, total)
            
    clear_undo_log()
    return {"total": total, "succeeded": succeeded, "failed": failed}

def process_directory(**kwargs) -> Dict[str, int]:
    source: Path = kwargs['source']
    dest: Path = kwargs['dest']
    mode: str = kwargs['mode']
    
    organizer_func = ORGANIZERS.get(mode)
    if not organizer_func:
        raise ValueError(f"Unknown organization mode: {mode}")
    
    files: List[Path] = kwargs.get('files', list_files(source, kwargs['recursive'], exclude_dir=dest))
    total = len(files)
    processed = succeeded = failed = skipped = 0
    
    categories = kwargs.get('categories', {})
    ext_index = build_ext_index(categories) if mode == "type" else {}

    for idx, item in enumerate(files, start=1):
        if kwargs.get('cancel_event') and kwargs['cancel_event'].is_set():
            logger.info("Cancellation requested. Stopping...")
            break

        result = organizer_func(item, dest, ext_index=ext_index, **kwargs)
        
        processed += 1
        if result is None:
            skipped += 1
        elif result:
            succeeded += 1
        else:
            failed += 1

        if 'on_progress' in kwargs:
            kwargs['on_progress'](idx, total, item, result)

    return {
        "total": total,
        "processed": processed,
        "succeeded": succeeded,
        "failed": failed,
        "skipped": skipped
    }
