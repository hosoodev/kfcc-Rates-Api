#!/usr/bin/env python3
"""Render exactly one OG image for fast layout review.

This command never creates a manifest and never deletes images.  It is only a
preview helper; use generate_og_images.py for the production incremental job.
"""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any

from generate_og_images import ASSET_DIRECTORY, image_payload, render_image


REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_BANKS_FILE = REPOSITORY_ROOT / "data" / "banks.json"
DEFAULT_OUTPUT_DIRECTORY = REPOSITORY_ROOT / "temp" / "og-image-preview"


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Render one KFCC OG image preview.")
    parser.add_argument("--banks-file", type=Path, help="Local V2 banks.json path")
    parser.add_argument("--gmgo-cd", help="Preview this group code instead of the first one")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIRECTORY)
    return parser.parse_args()


def load_banks(path: Path | None) -> dict[str, Any]:
    if path:
        return json.loads(path.read_text(encoding="utf-8"))
    if DEFAULT_BANKS_FILE.exists():
        return json.loads(DEFAULT_BANKS_FILE.read_text(encoding="utf-8"))

    # Fall back to the tracked api-data branch without creating a checkout.
    result = subprocess.run(
        ["git", "show", "origin/api-data:v2/meta/banks.json"],
        cwd=REPOSITORY_ROOT,
        text=True,
        encoding="utf-8",
        capture_output=True,
        check=False,
    )
    if result.returncode:
        raise FileNotFoundError(
            "banks.json was not found locally and origin/api-data could not be read. "
            "Pass --banks-file explicitly."
        )
    return json.loads(result.stdout)


def main() -> int:
    args = parse_arguments()
    data = load_banks(args.banks_file)
    groups = sorted(data.get("banks", []), key=lambda group: str(group.get("gmgoCd", "")))
    if not groups:
        raise ValueError("No bank groups were found in banks.json")

    group = next((item for item in groups if str(item.get("gmgoCd")) == args.gmgo_cd), None)
    if args.gmgo_cd and group is None:
        raise ValueError(f"gmgoCd '{args.gmgo_cd}' was not found")
    group = group or groups[0]
    payload = image_payload(group)
    if not payload["gmgoCd"]:
        raise ValueError("The selected bank group has no gmgoCd")

    destination = args.output_dir.resolve() / f"{payload['gmgoCd']}.png"
    render_image(payload, destination, ASSET_DIRECTORY)
    print(f"Generated one preview: {destination}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
