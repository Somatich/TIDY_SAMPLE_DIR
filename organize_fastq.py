#!/usr/bin/env python3
"""Organize FASTQ files into per-sample folders.

Usage:
  OUTDIR=/path/to/outdir ./organize_fastq.py
  ./organize_fastq.py --outdir /path/to/outdir
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import DefaultDict, Iterable, List, Optional, Tuple


FASTQ_RE = re.compile(
    r"^(?P<prefix>.+)_(?P<sample>[^_]+)_L(?P<lane>\d+)_R(?P<read>[12])"
    r"(?P<ext>\.(?:f(?:ast)?q)(?:\.gz)?)$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class FastqEntry:
    path: Path
    lane: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect FASTQ files into per-sample folders and concatenate lanes.",
    )
    parser.add_argument(
        "--outdir",
        default=os.environ.get("OUTDIR"),
        help="Output directory (defaults to $OUTDIR).",
    )
    return parser.parse_args()


def find_fastqs(root: Path, *, exclude: Optional[Path]) -> Iterable[Tuple[str, str, str, FastqEntry]]:
    exclude_resolved = exclude.resolve() if exclude else None
    for dirpath, _, filenames in os.walk(root):
        current_dir = Path(dirpath).resolve()
        if exclude_resolved and exclude_resolved in current_dir.parents:
            continue
        for filename in filenames:
            match = FASTQ_RE.match(filename)
            if not match:
                continue
            sample = match.group("sample")
            read = match.group("read")
            ext = match.group("ext")
            lane = int(match.group("lane"))
            yield sample, read, ext, FastqEntry(Path(dirpath) / filename, lane)


def concatenate(files: List[FastqEntry], destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    files_sorted = sorted(files, key=lambda entry: entry.lane)
    with destination.open("wb") as dest_handle:
        for entry in files_sorted:
            with entry.path.open("rb") as source_handle:
                shutil.copyfileobj(source_handle, dest_handle)


def relocate(entries: List[FastqEntry], destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if len(entries) == 1:
        shutil.move(str(entries[0].path), destination)
        return
    concatenate(entries, destination)
    for entry in entries:
        entry.path.unlink()


def main() -> int:
    args = parse_args()
    if not args.outdir:
        print("Error: OUTDIR is not set and --outdir was not provided.", file=sys.stderr)
        return 1

    outdir = Path(args.outdir).expanduser().resolve()
    outdir.mkdir(parents=True, exist_ok=True)

    groups: DefaultDict[Tuple[str, str, str], List[FastqEntry]] = defaultdict(list)
    for sample, read, ext, entry in find_fastqs(Path("."), exclude=outdir):
        groups[(sample, read, ext)].append(entry)

    if not groups:
        print("No FASTQ files matched the expected pattern.", file=sys.stderr)
        return 1

    for (sample, read, ext), entries in groups.items():
        dest = outdir / sample / f"{sample}_R{read}{ext}"
        relocate(entries, dest)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
