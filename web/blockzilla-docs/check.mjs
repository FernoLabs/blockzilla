import assert from 'node:assert/strict';
import { readFile, access } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';

const root = path.dirname(fileURLToPath(import.meta.url));
const out = path.join(root, 'dist');
const pages = JSON.parse(await readFile(path.join(root, 'pages.json'), 'utf8'));
const files = new Map(await Promise.all(pages.map(async page => [`${page.slug}.html`, await readFile(path.join(out, `${page.slug}.html`), 'utf8')])));
let links = 0;
for (const [name, html] of files) {
  assert.equal((html.match(/<h1\b/g) || []).length, 1, `${name}: one page title`);
  assert.equal((html.match(/aria-current="page"/g) || []).length, 1, `${name}: one current navigation page`);
  assert.ok(html.includes('name="viewport"'), `${name}: mobile viewport`);
  assert.ok(html.includes('class="skip-link"'), `${name}: keyboard skip link`);
  const ids = [...html.matchAll(/\bid="([^"]+)"/g)].map(match => match[1]);
  assert.equal(new Set(ids).size, ids.length, `${name}: unique IDs`);
  for (const [, href] of html.matchAll(/(?:href|src)="([^"]+)"/g)) {
    if (/^[a-z]+:|^\/\//i.test(href)) continue;
    const [target, fragment] = href.split('#');
    const targetName = target || name;
    await access(path.join(out, targetName));
    if (fragment) assert.ok(files.get(targetName)?.includes(`id="${fragment}"`), `${name}: missing anchor ${href}`);
    links += 1;
  }
}
execFileSync(process.execPath, ['--check', path.join(root, 'app.js')]);
execFileSync(process.execPath, ['--check', path.join(root, 'serve.mjs')]);
console.log(`Checked ${files.size} pages and ${links} local links. Browser scripts parse.`);
