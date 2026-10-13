# Blockzilla documentation website

A static documentation site for Blockzilla users and format authors.
The content stays in repository Markdown. The site has sixteen pages, a user
guide, an advanced section, code-copy buttons, mobile navigation, and light
and dark themes. It makes no external font or script requests.

## Preview

Use Node.js 22 or later. From this directory:

```bash
npm ci
npm run dev
```

Open `http://127.0.0.1:4173`. The server listens on loopback only.
Use `PORT=4174 npm run dev` when that port is already in use.
After a content edit, restart the preview or run `npm run build` to rebuild.

## Build and check

```bash
npm run check
```

This builds `dist/`, checks relative source links, checks generated page links
and heading anchors, and checks that the browser scripts parse. Any static
host can serve the files in `dist/`. This change does not publish the site.

## Edit content

- `docs/start/` contains the user guide.
- `docs/advanced/` contains the format overview, V3 guide, and coverage guide.
- `docs/reference/archive-v2-hot-block-format.md` remains the V2 byte reference.
- `pages.json` defines page order, navigation groups, and descriptions.

Links between included Markdown pages become local HTML links. Other
repository links point to their GitHub source. The site renders only
author-controlled repository Markdown.

The quick start installs the CLI and reads a historical epoch through Cloudflare.
The live stream guide covers binary HTTP, SSE notices, browser/Node decoding,
local filters, and resume limits. The examples cover token JSON exports, a live
token SQLite index with backfill, and historical program SQLite dumps.
The token export example is in `blockzilla/cli/src/dump_tokens.rs` and has a
ClickHouse schema under `examples/token-dumper/`. Check the CLI commands after
a reader change. `docs/advanced/publishing.md` records package release steps.
Keep current behavior separate from proposed APIs, live services, and format
migration plans. The quick start is a sample read, not proof of epoch coverage.
