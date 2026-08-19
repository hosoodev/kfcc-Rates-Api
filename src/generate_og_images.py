#!/usr/bin/env python3
"""Generate incremental Open Graph images from v2/meta/banks.json.

The destination directory is intended to be the root of the ``og-images``
worktree/branch.  Only files listed in its manifest are ever removed.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from functools import lru_cache

from PIL import Image, ImageDraw, ImageFont


CANVAS_SIZE = (1200, 630)
RENDER_VERSION = "2"
MANIFEST_NAME = "manifest.json"
IMAGE_DIRECTORY = "images"
ASSET_DIRECTORY = Path(__file__).resolve().parent / "og-images"


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        data = json.load(file)
    if not isinstance(data, dict) or not isinstance(data.get("banks"), list):
        raise ValueError(f"'{path}' is not a v2 banks metadata file")
    return data


def image_payload(group: dict[str, Any]) -> dict[str, str]:
    """Return exactly the data which affects a rendered image."""
    head_office = group.get("head_office") or {}
    return {
        "gmgoCd": str(group.get("gmgoCd") or "").strip(),
        "group_name": str(group.get("group_name") or "").strip(),
        "province": str(head_office.get("province") or "").strip(),
        "district": str(head_office.get("district") or "").strip(),
    }


def payload_hash(payload: dict[str, str]) -> str:
    serialized = json.dumps(
        {"renderer": RENDER_VERSION, "bank": payload},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def load_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": 1, "renderer_version": RENDER_VERSION, "images": {}}
    with path.open("r", encoding="utf-8") as file:
        manifest = json.load(file)
    if not isinstance(manifest, dict) or not isinstance(manifest.get("images"), dict):
        raise ValueError(f"'{path}' has an invalid manifest format")
    return manifest


@lru_cache(maxsize=None)
def font(path: Path, size: int) -> ImageFont.FreeTypeFont:
    if not path.exists():
        raise FileNotFoundError(f"Required font was not found: {path}")
    return ImageFont.truetype(str(path), size=size)


@lru_cache(maxsize=None)
def background_template(path: Path) -> Image.Image:
    """Load and scale the shared background once, before worker threads start."""
    if not path.exists():
        raise FileNotFoundError(f"Required background was not found: {path}")
    with Image.open(path) as source:
        return fit_background(source.convert("RGB")).convert("RGBA")


def warm_assets(assets: Path) -> None:
    """Preload immutable render assets so every worker can reuse them."""
    background_template(assets / "og-bg.png")
    font(assets / "PAPERLOGY-9BLACK.TTF", 108)
    font(assets / "PAPERLOGY-7BOLD.TTF", 39)
    font(assets / "PAPERLOGY-8EXTRABOLD.TTF", 28)


def fit_background(background: Image.Image) -> Image.Image:
    canvas_width, canvas_height = CANVAS_SIZE
    scale = max(canvas_width / background.width, canvas_height / background.height)
    resized = background.resize(
        (round(background.width * scale), round(background.height * scale)), Image.Resampling.LANCZOS
    )
    left = (resized.width - canvas_width) // 2
    top = (resized.height - canvas_height) // 2
    return resized.crop((left, top, left + canvas_width, top + canvas_height))


def draw_wrapped_text(
    draw: ImageDraw.ImageDraw, text: str, text_font: ImageFont.FreeTypeFont,
    x: int, y: int, max_width: int, fill: str, spacing: int = 12, centered: bool = False,
    stroke_width: int = 0, stroke_fill: str | None = None,
) -> int:
    """Draw text split by words/characters and return its final bottom y."""
    lines: list[str] = []
    line = ""
    for character in text:
        candidate = line + character
        if line and draw.textbbox((0, 0), candidate, font=text_font)[2] > max_width:
            lines.append(line)
            line = character
        else:
            line = candidate
    if line:
        lines.append(line)

    bbox = draw.textbbox((0, 0), "가", font=text_font)
    line_height = bbox[3] - bbox[1]
    for index, line in enumerate(lines[:2]):
        line_x = x
        if centered:
            line_width = draw.textbbox((0, 0), line, font=text_font)[2]
            line_x += (max_width - line_width) // 2
        draw.text(
            (line_x, y + index * (line_height + spacing)), line, font=text_font, fill=fill,
            stroke_width=stroke_width, stroke_fill=stroke_fill,
        )
    return y + min(len(lines), 2) * (line_height + spacing)


def draw_justified_characters(
    draw: ImageDraw.ImageDraw, text: str, text_font: ImageFont.FreeTypeFont,
    x: int, y: int, target_width: int, fill: str, stroke_width: int, stroke_fill: str,
) -> None:
    """Draw Korean characters with equal gaps so their outer edges match a title."""
    characters = list(text)
    widths = [draw.textbbox((0, 0), character, font=text_font)[2] for character in characters]
    gap = (target_width - sum(widths)) / max(len(characters) - 1, 1)
    cursor = float(x)
    for character, width in zip(characters, widths):
        draw.text(
            (round(cursor), y), character, font=text_font, fill=fill,
            stroke_width=stroke_width, stroke_fill=stroke_fill,
        )
        cursor += width + gap


def render_image(payload: dict[str, str], destination: Path, assets: Path) -> None:
    background_path = assets / "og-bg.png"
    image = background_template(background_path).copy()
    draw = ImageDraw.Draw(image)

    # Sized for Naver's small square thumbnail crop: the three key title lines
    # remain large and centered within the 630px-wide middle area.
    title_font = font(assets / "PAPERLOGY-9BLACK.TTF", 108)
    location_font = font(assets / "PAPERLOGY-7BOLD.TTF", 39)
    badge_font = font(assets / "PAPERLOGY-8EXTRABOLD.TTF", 28)

    # Visual treatment intentionally lives in this single function for quick design iteration.
    center_x = CANVAS_SIZE[0] // 2
    badge_text = "새마을금고 금리비교 · 경영평가등급"
    badge_width = draw.textbbox((0, 0), badge_text, font=badge_font)[2]
    draw.text((center_x - badge_width // 2, 48), badge_text, font=badge_font, fill="white")
    location = " ".join(value for value in (payload["province"], payload["district"]) if value)
    if location:
        location_bbox = draw.textbbox((0, 0), location, font=location_font)
        location_width = location_bbox[2] - location_bbox[0]
        location_left = center_x - (location_width + 64) // 2
        label_top, label_bottom = 112, 172
        draw.rounded_rectangle(
            (location_left, label_top, location_left + location_width + 64, label_bottom),
            radius=30, fill=(29, 178, 139, 255),
        )
        location_x = center_x - location_width // 2 - location_bbox[0]
        location_y = (label_top + label_bottom - (location_bbox[3] - location_bbox[1])) // 2 - location_bbox[1]
        draw.text((location_x, location_y), location, font=location_font, fill="white")
    name = payload["group_name"] or "새마을금고"
    name_width = draw.textbbox((0, 0), name, font=title_font)[2]
    if name_width <= 960:
        draw.text((center_x - name_width // 2, 195), name, font=title_font, fill="white")
    else:
        draw_wrapped_text(draw, name, title_font, center_x - 480, 195, 960, "white", centered=True)
    subtitle_text = "새마을금고"
    subtitle_width = draw.textbbox((0, 0), subtitle_text, font=title_font)[2]
    subtitle_x = center_x - subtitle_width // 2
    draw.text((subtitle_x, 315), subtitle_text, font=title_font, fill="white")
    draw_justified_characters(
        draw, "금리비교", title_font, subtitle_x, 435, subtitle_width, "white", 0, "white",
    )

    destination.parent.mkdir(parents=True, exist_ok=True)
    # Pillow's optimize=True performs an expensive exhaustive PNG pass.  This
    # branch contains thousands of generated files, so prefer fast standard
    # compression; it does not change the rendered pixels.
    image.convert("RGB").save(destination, format="PNG", compress_level=6)


def safely_remove(root: Path, relative_path: str) -> None:
    target = (root / relative_path).resolve()
    images_root = (root / IMAGE_DIRECTORY).resolve()
    if images_root not in target.parents or target.suffix.lower() != ".png":
        raise ValueError(f"Refusing to delete unsafe manifest path: {relative_path}")
    if target.exists():
        target.unlink()


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Incrementally generate KFCC Open Graph images.")
    parser.add_argument("--banks-file", type=Path, required=True, help="Path to v2/meta/banks.json")
    parser.add_argument("--output-dir", type=Path, required=True, help="Root of the og-images worktree")
    parser.add_argument("--assets-dir", type=Path, default=ASSET_DIRECTORY, help="Directory containing background and fonts")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without writing files")
    parser.add_argument("--force", action="store_true", help="Regenerate every selected image")
    parser.add_argument("--gmgo-cd", action="append", default=[], help="Generate only this code (repeatable)")
    parser.add_argument("--limit", type=int, help="Generate at most this many pending images")
    parser.add_argument("--workers", type=int, default=8, help="Parallel image render workers (default: 8)")
    return parser.parse_args()


def main() -> int:
    args = parse_arguments()
    if args.workers < 1:
        raise ValueError("--workers must be at least 1")
    data = load_json(args.banks_file)
    output_dir = args.output_dir.resolve()
    manifest_path = output_dir / MANIFEST_NAME
    previous = load_manifest(manifest_path)
    previous_images: dict[str, dict[str, Any]] = previous["images"]

    selected_codes = set(args.gmgo_cd)
    groups: dict[str, dict[str, str]] = {}
    for group in data["banks"]:
        payload = image_payload(group)
        code = payload["gmgoCd"]
        if not code:
            raise ValueError("A bank group has no gmgoCd")
        if code in groups:
            raise ValueError(f"Duplicate gmgoCd in banks metadata: {code}")
        if not selected_codes or code in selected_codes:
            groups[code] = payload

    pending = [
        code for code, payload in sorted(groups.items())
        if args.force or previous_images.get(code, {}).get("hash") != payload_hash(payload)
        or not (output_dir / IMAGE_DIRECTORY / f"{code}.png").exists()
    ]
    if args.limit is not None:
        pending = pending[:args.limit]

    deleted = [] if selected_codes else sorted(set(previous_images) - set(groups))
    print(f"create/update: {len(pending)}, delete: {len(deleted)}, unchanged: {len(groups) - len(pending)}")
    for code in deleted:
        print(f"  delete {code}")
    if args.dry_run:
        return 0

    next_images = dict(previous_images)
    if pending:
        warm_assets(args.assets_dir)
        (output_dir / IMAGE_DIRECTORY).mkdir(parents=True, exist_ok=True)
        print(f"rendering {len(pending)} images with {args.workers} workers")
        with ThreadPoolExecutor(max_workers=args.workers, thread_name_prefix="og-render") as executor:
            futures = {
                executor.submit(
                    render_image, groups[code], output_dir / IMAGE_DIRECTORY / f"{code}.png", args.assets_dir
                ): code
                for code in pending
            }
            for completed_count, future in enumerate(as_completed(futures), start=1):
                code = futures[future]
                future.result()
                relative_path = f"{IMAGE_DIRECTORY}/{code}.png"
                next_images[code] = {"hash": payload_hash(groups[code]), "path": relative_path}
                print(f"  rendered {completed_count}/{len(pending)}: {code}")
    for code in deleted:
        safely_remove(output_dir, str(previous_images[code].get("path", "")))
        next_images.pop(code, None)

    # A targeted run must not make unrelated entries look absent from the source.
    manifest = {
        "version": 1,
        "renderer_version": RENDER_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {"banks_file": str(args.banks_file)},
        "images": dict(sorted(next_images.items())),
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
