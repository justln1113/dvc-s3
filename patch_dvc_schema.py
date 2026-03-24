#!/usr/bin/env python3
"""Patch DVC's config schema to accept s3transfer tuning parameters.

DVC validates remote config against a strict schema that does not include
transfer tuning keys (max_concurrent_requests, multipart_threshold, etc.).
This script adds them so that `dvc remote modify` accepts these parameters.

Usage:
    python patch_dvc_schema.py          # auto-detect and patch
    python patch_dvc_schema.py --check  # check if patch is needed
    python patch_dvc_schema.py --revert # remove the patch
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PARAMS = [
    '"max_concurrent_requests": str,',
    '"multipart_threshold": str,',
    '"multipart_chunksize": str,',
    '"max_queue_size": str,',
]

ANCHOR = '"read_timeout": Coerce(int),'
MARKER = PARAMS[0]


def find_schema_file() -> Path:
    import dvc.config_schema

    return Path(dvc.config_schema.__file__)


def is_patched(text: str) -> bool:
    return MARKER in text


def patch(text: str) -> str:
    if is_patched(text):
        return text

    lines = text.split("\n")
    for i, line in enumerate(lines):
        if ANCHOR in line:
            indent = " " * (len(line) - len(line.lstrip()))
            insert = "\n".join(f"{indent}{p}" for p in PARAMS)
            lines.insert(i + 1, insert)
            return "\n".join(lines)

    print(f"ERROR: anchor line not found: {ANCHOR}", file=sys.stderr)
    sys.exit(1)


def revert(text: str) -> str:
    lines = text.split("\n")
    return "\n".join(line for line in lines if not any(p in line for p in PARAMS))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--check", action="store_true", help="Check if patch is needed")
    group.add_argument("--revert", action="store_true", help="Remove the patch")
    args = parser.parse_args()

    path = find_schema_file()
    text = path.read_text()

    if args.check:
        if is_patched(text):
            print(f"Already patched: {path}")
        else:
            print(f"Patch needed: {path}")
            sys.exit(1)
        return

    if args.revert:
        if not is_patched(text):
            print(f"Not patched, nothing to revert: {path}")
            return
        path.write_text(revert(text))
        print(f"Reverted: {path}")
        return

    if is_patched(text):
        print(f"Already patched: {path}")
        return

    path.write_text(patch(text))
    print(f"Patched: {path}")


if __name__ == "__main__":
    main()
