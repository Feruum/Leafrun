# Leafrun logo

The primary asset is `leafrun-logo.png`, a transparent PNG used in the README. The mark combines a leaf and a folded document page. It was generated and edited with the built-in imagegen tool.

The current version removes the white background and leaf cutout and reduces the surrounding padding. The generated alpha channel is preserved; the PNG is copied into the repository without further image editing.

The original white-background image is retained in [Git history](https://github.com/Feruum/Leafrun/blob/73cf8d9ed4f2f3ebf2f76061fb35065e149db2ff/docs/assets/leafrun-logo.png). Both prompts are recorded below.

## Original generation prompt

```text
Use case: logo-brand.
Create the final primary logo icon for Leafrun, a small Go library that renders local Typst document projects into PDFs.
Design one original, simple, bold geometric symbol: a rounded document page with a clear folded top-right corner; a single broad leaf silhouette is cut cleanly into the page using negative space. The leaf should be recognized immediately, with no veins or intricate details. Page and leaf read as one integrated mark.
This must be a clean flat vector-style brand logo, like a finished professional SVG rendered to PNG. Use exactly two uniform solid colors: deep emerald green #227A49 for the mark, pure white #FFFFFF for the background and negative-space cuts. Absolutely no gradients, grain, texture, lighting, shadows, outlines, tiny detached fragments, speckles, roughness or 3D.
One centered icon only on a square white canvas. Generous white padding around all sides. Strong smooth precise contours and bold simple geometry that remain legible at 32 pixels. No text, letters, wordmark, slogan, watermark, mockup, border or decorative elements. Do not imitate any existing Overleaf or Typst logo.
Deliver one finished polished logo icon. Opaque white background.
```

## Transparent version prompt

```text
Use case: background-extraction.
Asset type: transparent logo PNG for the Leafrun GitHub README.
Input image: the existing Leafrun logo is the edit target.
Primary request: remove all white from the existing image so the same green Leafrun symbol can sit directly on light or dark backgrounds. Make the canvas genuinely transparent, including the white leaf cutout inside the page and the white gap around the folded corner.
Preserve the existing green document-and-leaf symbol: same silhouette, shape, orientation, proportions, emerald green color and folded-page detail. Do not add details, change the design, or add text.
Reduce the excessive empty margin: center the preserved mark within a square canvas with about 12% transparent padding around it. The mark should occupy roughly 76% of the square height.
Output must have real alpha transparency. Preserve clean smooth contours without a white halo, gray halo, stray pixels, speckles or background residue. The entire area outside the green shape and the entire leaf-shaped cutout must be fully transparent. No checkerboard, no colored rectangle, no shadow, no glow, no texture, no wordmark, no mockup. Deliver only the finished logo PNG.
```
