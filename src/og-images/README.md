# OG image generator assets

`../generate_og_images.py` uses the fonts and `og-bg.png` in this directory.
The generated images do **not** belong here: write them to the root of an
`og-images` branch worktree instead.

Example (from the repository root):

```bash
python src/generate_og_images.py \
  --banks-file data/banks.json \
  --output-dir ../kfcc-og-images \
  --dry-run
```

Remove `--dry-run` to write `images/<gmgoCd>.png` and `manifest.json`.
`manifest.json` remembers the rendered data hash, so subsequent runs only
render added or changed groups and remove images for groups no longer present
in `banks.json`.

Useful design-iteration options:

```bash
# Regenerate one group after changing the layout.
python src/generate_og_images.py --banks-file data/banks.json --output-dir ../kfcc-og-images --gmgo-cd 0101 --force

# Review a small pending batch without creating files.
python src/generate_og_images.py --banks-file data/banks.json --output-dir ../kfcc-og-images --limit 10 --dry-run
```

Increase `RENDER_VERSION` in `generate_og_images.py` when a visual change
should intentionally regenerate every image.

For a quick visual review, generate only the first group without making an
`og-images` worktree or manifest:

```bash
python src/generate_og_image_preview.py
```

The preview is written to `temp/og-image-preview` in this repository. Use
`--gmgo-cd 0101` to inspect a specific group.

In GitHub Actions, pass `api-data/v2/meta/banks.json` instead: that is the
same V2 structure after the crawler writes it to the `api-data` worktree.
