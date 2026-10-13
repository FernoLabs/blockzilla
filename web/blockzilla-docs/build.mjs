import { readFile, writeFile, mkdir, copyFile, rm, access } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Marked, marked } from 'marked';

const project = path.dirname(fileURLToPath(import.meta.url));
const repo = path.resolve(project, '../..');
const out = path.join(project, 'dist');
const pages = JSON.parse(await readFile(path.join(project, 'pages.json'), 'utf8'));
const github = 'https://github.com/FernoLabs/blockzilla';
const bySource = new Map(pages.map(page => [page.source, page]));
const escape = value => String(value).replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[char]);
const slugify = value => value.toLowerCase().replace(/<[^>]*>/g, '').replace(/[^\p{L}\p{N}\s-]/gu, '').trim().replace(/\s+/g, '-');
const filename = page => `${page.slug}.html`;
const groups = [...new Set(pages.map(page => page.group))];
const icon = '<svg viewBox="0 0 28 28" aria-hidden="true"><path d="M14 2 25 8.5v11L14 26 3 19.5v-11L14 2Z M3 8.5 14 15l11-6.5 M14 15v11 M8.5 5.2l11 6.5" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linejoin="round"/></svg>';
await rm(out, { recursive: true, force: true });
await mkdir(out, { recursive: true });
for (const name of ['styles.css', 'app.js']) await copyFile(path.join(project, name), path.join(out, name));

for (const [index, page] of pages.entries()) {
  const headings = [];
  const usedIds = new Map();
  const markdown = await readFile(path.join(repo, page.source), 'utf8');
  const marked = new Marked({ gfm: true, renderer: {
    heading({ tokens, depth }) {
      const text = this.parser.parseInline(tokens);
      const base = slugify(text);
      const count = usedIds.get(base) || 0;
      usedIds.set(base, count + 1);
      const id = count ? `${base}-${count}` : base;
      if (depth === 2) headings.push({ id, text });
      return `<h${depth} id="${escape(id)}">${text}${depth > 1 ? `<a class="heading-link" href="#${escape(id)}" aria-label="Link to ${escape(text.replace(/<[^>]*>/g, ''))}">#</a>` : ''}</h${depth}>`;
    },
    code({ text, lang }) {
      const language = (lang || 'text').split(/[\s,]/)[0];
      return `<div class="code-block"><div class="code-tools"><span>${escape(language === 'bash' || language === 'console' ? 'Terminal' : language === 'rust' ? 'Rust' : 'Text')}</span><button type="button" class="copy-code" aria-label="Copy code">Copy</button></div><pre tabindex="0"><code class="language-${escape(language)}">${escape(text)}</code></pre></div>`;
    },
    link({ href, tokens, title }) {
      let target = href;
      if (href && !/^(?:[a-z]+:|#|\/\/)/i.test(href)) {
        const [relative, fragment] = href.split('#');
        const source = path.posix.normalize(path.posix.join(path.posix.dirname(page.source), relative));
        if (source.startsWith('../')) throw new Error(`Link outside repository: ${href}`);
        const known = bySource.get(source);
        target = known ? `${filename(known)}${fragment ? `#${fragment}` : ''}` : `${github}/blob/main/${source}${fragment ? `#${fragment}` : ''}`;
      }
      return `<a href="${escape(target || '')}"${title ? ` title="${escape(title)}"` : ''}>${this.parser.parseInline(tokens)}</a>`;
    },
    table(token) {
      const row = (cells, tag) => `<tr>${cells.map(cell => `<${tag}>${this.parser.parseInline(cell.tokens)}</${tag}>`).join('')}</tr>`;
      return `<div class="table-scroll" tabindex="0"><table><thead>${row(token.header, 'th')}</thead><tbody>${token.rows.map(cells => row(cells, 'td')).join('')}</tbody></table></div>`;
    }
  }});
  // Only checked-in, author-controlled Markdown is rendered; no user HTML input.
  const body = marked.parse(markdown);
  const nav = groups.map(group => `<section class="nav-group"><h2>${escape(group)}</h2>${pages.filter(item => item.group === group).map(item => `<a href="${filename(item)}"${item.slug === page.slug ? ' aria-current="page"' : ''}>${escape(item.title)}</a>`).join('')}</section>`).join('');
  const adjacent = (item, label) => item ? `<a href="${filename(item)}"><span>${label}</span><strong>${escape(item.title)} <span aria-hidden="true">${label === 'Next' ? '→' : '←'}</span></strong></a>` : '<span></span>';
  const html = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="description" content="${escape(page.description)}">
  <meta name="color-scheme" content="light dark">
  <title>${escape(page.title)} · Blockzilla</title>
  <script>try{const t=localStorage.getItem('blockzilla-docs-theme');if(t==='dark'||t==='light')document.documentElement.dataset.theme=t}catch{}</script>
  <link rel="stylesheet" href="styles.css">
  <script src="app.js" defer></script>
</head>
<body>
  <a class="skip-link" href="#content">Skip to content</a>
  <header class="site-header">
    <a class="brand" href="index.html" aria-label="Blockzilla documentation home">${icon}<span>Blockzilla</span></a>
    <span class="header-label">Documentation</span>
    <div class="header-actions"><a href="${github}">GitHub <span aria-hidden="true">↗</span></a><button type="button" id="theme-toggle" aria-label="Change color theme">Theme</button><button type="button" id="menu-toggle" aria-expanded="false" aria-controls="sidebar">Menu</button></div>
  </header>
  <div class="layout">
    <aside id="sidebar"><nav aria-label="Documentation">${nav}</nav><a class="sidebar-start" href="quick-start.html">Install and stream <span aria-hidden="true">→</span></a></aside>
    <main id="content" tabindex="-1"><article><div class="breadcrumb">${escape(page.group)}</div>${body}</article><nav class="page-nav" aria-label="Adjacent pages">${adjacent(pages[index - 1], 'Previous')}${adjacent(pages[index + 1], 'Next')}</nav><footer><a href="${github}/blob/main/${page.source}">View this page on GitHub <span aria-hidden="true">↗</span></a></footer></main>
    <aside class="contents"><nav aria-label="On this page"><h2>On this page</h2>${headings.map(heading => `<a href="#${escape(heading.id)}">${heading.text}</a>`).join('')}</nav></aside>
  </div>
  <div id="announcement" role="status" class="visually-hidden"></div>
</body>
</html>`;
  await writeFile(path.join(out, filename(page)), html);
}

// Catch broken source links before producing the site.
for (const page of pages) {
  const markdown = await readFile(path.join(repo, page.source), 'utf8');
  const tokens = marked.lexer(markdown);
  marked.walkTokens(tokens, token => {
    if (token.type !== 'link' || !token.href || /^(?:[a-z]+:|#|\/\/)/i.test(token.href)) return;
    const target = token.href.split('#')[0];
    token.repoTarget = path.resolve(repo, path.dirname(page.source), target);
  });
  const targets = [];
  marked.walkTokens(tokens, token => { if (token.repoTarget) targets.push(token.repoTarget); });
  await Promise.all(targets.map(target => access(target)));
}
console.log(`Built ${pages.length} documentation pages in ${out}`);
